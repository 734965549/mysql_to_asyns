package fullload

import (
	"fmt"
	"sync"
	"time"
)

// TableLoadPhase 表级加载阶段（P2 表级状态机）
type TableLoadPhase string

const (
	PhasePending         TableLoadPhase = "PENDING"          // 等待开始
	PhaseSnapshotOpening TableLoadPhase = "SNAPSHOT_OPENING" // 正在打开快照
	PhaseCopying         TableLoadPhase = "COPYING"          // 数据复制中
	PhaseDataReady       TableLoadPhase = "DATA_READY"       // 数据已提交，staging 表就绪
	PhasePublished       TableLoadPhase = "PUBLISHED"        // 已发布到最终表
	PhaseFailed          TableLoadPhase = "FAILED"           // 表级失败（不可重试或重试耗尽）
)

// TableLoadState 跟踪单张表的加载状态、重试次数、inflight 批次计数。
// 由 tableStateTracker 持有并通过互斥锁保护。
type TableLoadState struct {
	Schema string
	Table  string

	Phase     TableLoadPhase
	AttemptID int // 当前尝试序号（从 1 开始）

	Inflight      int       // 当前 attempt 的未提交批次数（含仍在队列中的）
	CommittedRows int64     // 当前 attempt 已提交行数
	ReadDone      bool      // 当前 attempt 读取是否完成
	LastError     error     // 上一次失败的错误
	LastAttempt   time.Time // 上一次尝试开始时间
	StagingTable  string    // 当前 attempt 的 staging 表名
}

// tableStateTracker 管理所有表的状态，提供原子状态转换和 inflight barrier。
type tableStateTracker struct {
	mu       sync.Mutex
	states   map[string]*TableLoadState // key = schema.table
	onChange TableStateCallback         // P3: 状态变更回调(可为 nil)
}

func newTableStateTracker(specs []*TableSpec) *tableStateTracker {
	t := &tableStateTracker{
		states: make(map[string]*TableLoadState, len(specs)),
	}
	for _, spec := range specs {
		if spec == nil {
			continue
		}
		key := tableKey(spec.SourceSchema, spec.SourceTable)
		t.states[key] = &TableLoadState{
			Schema:    spec.SourceSchema,
			Table:     spec.SourceTable,
			Phase:     PhasePending,
			AttemptID: 0,
		}
	}
	return t
}

// notifyChange 在锁外触发状态变更回调(P3 持久化)。
// 必须在释放 mu 后调用,避免回调中的 storage.Save 与锁形成死锁。
// 持久化失败返回错误，由调用方 fail-closed。
func (t *tableStateTracker) notifyChange(st *TableLoadState, errMsg string) error {
	if t == nil || t.onChange == nil || st == nil {
		return nil
	}
	return t.onChange(st.Schema, st.Table, string(st.Phase), st.AttemptID, st.StagingTable, errMsg, st.CommittedRows)
}

// startAttempt 开始新的尝试，返回新的 attemptID。
// maxRetries 是额外重试次数：总 attempt 上限 = maxRetries+1（含首次）。
// maxRetries=0 时仍允许首次 attempt（配合 staging 单次发布）。
func (t *tableStateTracker) startAttempt(schema, table string, maxRetries int) (int, error) {
	if t == nil {
		return 0, fmt.Errorf("nil tracker")
	}
	if maxRetries < 0 {
		maxRetries = 0
	}
	maxAttempts := maxRetries + 1

	t.mu.Lock()
	st := t.states[tableKey(schema, table)]
	if st == nil {
		t.mu.Unlock()
		return 0, fmt.Errorf("table %s.%s not found in tracker", schema, table)
	}
	if st.AttemptID >= maxAttempts {
		t.mu.Unlock()
		return 0, fmt.Errorf("table %s.%s exceeded max retries (%d retries => %d attempts)", schema, table, maxRetries, maxAttempts)
	}
	st.AttemptID++
	st.Inflight = 0
	st.CommittedRows = 0
	st.ReadDone = false
	st.LastError = nil
	st.LastAttempt = time.Now()
	st.Phase = PhaseSnapshotOpening
	st.StagingTable = ""
	id := st.AttemptID
	snap := *st
	t.mu.Unlock()

	if err := t.notifyChange(&snap, ""); err != nil {
		return 0, fmt.Errorf("persist state after startAttempt: %w", err)
	}
	return id, nil
}

// setStagingTable 记录当前 attempt 的 staging 表名并持久化。
func (t *tableStateTracker) setStagingTable(schema, table, stagingTable string) error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	st := t.states[tableKey(schema, table)]
	if st == nil {
		t.mu.Unlock()
		return nil
	}
	st.StagingTable = stagingTable
	snap := *st
	t.mu.Unlock()
	return t.notifyChange(&snap, "")
}

// transitionTo 原子转换表状态；持久化失败时返回错误。
func (t *tableStateTracker) transitionTo(schema, table string, phase TableLoadPhase) error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	st := t.states[tableKey(schema, table)]
	if st == nil {
		t.mu.Unlock()
		return nil
	}
	st.Phase = phase
	snap := *st
	t.mu.Unlock()
	if err := t.notifyChange(&snap, ""); err != nil {
		return fmt.Errorf("persist state transition to %s: %w", phase, err)
	}
	return nil
}

// recordError 记录表级失败错误并转入 FAILED（若尚未发布）。
func (t *tableStateTracker) recordError(schema, table string, err error) error {
	if t == nil || err == nil {
		return nil
	}
	t.mu.Lock()
	st := t.states[tableKey(schema, table)]
	if st == nil {
		t.mu.Unlock()
		return nil
	}
	st.LastError = err
	if st.Phase != PhasePublished {
		st.Phase = PhaseFailed
	}
	snap := *st
	errMsg := err.Error()
	t.mu.Unlock()
	return t.notifyChange(&snap, errMsg)
}

// onBatchEnqueued 在 reader q.Put 之前预增 inflight，使队列中的批次对 barrier 可见。
func (t *tableStateTracker) onBatchEnqueued(schema, table string, attemptID int) error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.states[tableKey(schema, table)]
	if st == nil {
		return nil
	}
	if st.AttemptID != attemptID {
		return fmt.Errorf("batch attempt %d rejected; current attempt is %d", attemptID, st.AttemptID)
	}
	st.Inflight++
	return nil
}

// onBatchReleased 回减单个批次的 inflight（提交、回滚、Put 失败、丢弃旧 attempt 均走此路径）。
func (t *tableStateTracker) onBatchReleased(schema, table string, attemptID int) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.states[tableKey(schema, table)]
	if st == nil {
		return
	}
	// 旧 attempt 的释放：若已 start 新 attempt（Inflight 已重置），静默忽略。
	if st.AttemptID != attemptID {
		return
	}
	st.Inflight--
	if st.Inflight < 0 {
		st.Inflight = 0
	}
}

// onBatchesReleased 按批次数回减 inflight。
func (t *tableStateTracker) onBatchesReleased(schema, table string, attemptID int, n int) {
	if t == nil || n <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.states[tableKey(schema, table)]
	if st == nil || st.AttemptID != attemptID {
		return
	}
	st.Inflight -= n
	if st.Inflight < 0 {
		st.Inflight = 0
	}
}

// onBatchesCommitted 标记批次已提交：回减 inflight 并累加 CommittedRows。
func (t *tableStateTracker) onBatchesCommitted(schema, table string, attemptID int, batchCount int, rows int64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.states[tableKey(schema, table)]
	if st == nil || st.AttemptID != attemptID {
		return
	}
	st.Inflight -= batchCount
	if st.Inflight < 0 {
		st.Inflight = 0
	}
	if rows > 0 {
		st.CommittedRows += rows
	}
}

// isCurrentAttempt 判断 attemptID 是否仍为当前 attempt。
func (t *tableStateTracker) isCurrentAttempt(schema, table string, attemptID int) bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.states[tableKey(schema, table)]
	return st != nil && st.AttemptID == attemptID
}

// markReadDone 标记表读取完成。
func (t *tableStateTracker) markReadDone(schema, table string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.states[tableKey(schema, table)]
	if st != nil {
		st.ReadDone = true
	}
}

// waitInflightZero 阻塞等待指定表的 inflight 归零（inflight barrier）。
func (t *tableStateTracker) waitInflightZero(schema, table string, timeout time.Duration) error {
	if t == nil {
		return nil
	}
	deadline := time.Now().Add(timeout)
	for {
		t.mu.Lock()
		st := t.states[tableKey(schema, table)]
		if st == nil {
			t.mu.Unlock()
			return nil
		}
		if st.Inflight <= 0 {
			t.mu.Unlock()
			return nil
		}
		t.mu.Unlock()
		if time.Now().After(deadline) {
			return fmt.Errorf("inflight barrier timeout for %s.%s", schema, table)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// getState 返回表状态快照（调试用）。
func (t *tableStateTracker) getState(schema, table string) *TableLoadState {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.states[tableKey(schema, table)]
	if st == nil {
		return nil
	}
	cpy := *st
	return &cpy
}

// 兼容旧测试名：onBatchCommitted 单次回减。
func (t *tableStateTracker) onBatchCommitted(schema, table string, attemptID int) error {
	t.onBatchReleased(schema, table, attemptID)
	return nil
}
