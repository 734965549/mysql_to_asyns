package fullload

import (
	"fmt"
	"sync"
)

// TableDataReadyCallback 在某张表的全部数据已入队且目标端全部提交后调用。
// 用于逐表重建索引等收尾；回调应尽快返回（重活请异步），返回错误会使引擎失败。
type TableDataReadyCallback func(schema, table string) error

// tableCompletionTracker 跟踪每张表「读完 + 写完」状态，触发 OnTableDataReady。
type tableCompletionTracker struct {
	mu      sync.Mutex
	states  map[string]*tableCompletionState
	onReady TableDataReadyCallback
}

type tableCompletionState struct {
	schema   string
	table    string
	inflight int
	readDone bool
	fired    bool
}

func newTableCompletionTracker(specs []*TableSpec, onReady TableDataReadyCallback) *tableCompletionTracker {
	t := &tableCompletionTracker{
		states:  make(map[string]*tableCompletionState, len(specs)),
		onReady: onReady,
	}
	for _, spec := range specs {
		if spec == nil {
			continue
		}
		key := tableKey(spec.SourceSchema, spec.SourceTable)
		t.states[key] = &tableCompletionState{
			schema: spec.SourceSchema,
			table:  spec.SourceTable,
		}
	}
	return t
}

func (t *tableCompletionTracker) onBatchEnqueued(schema, table string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.states[tableKey(schema, table)]
	if st == nil {
		return
	}
	st.inflight++
}

// onBatchEnqueueAborted 在 Put 失败时回滚已预增的 inflight，避免假死卡住 OnTableDataReady。
func (t *tableCompletionTracker) onBatchEnqueueAborted(schema, table string) error {
	return t.onBatchesCommitted(schema, table, 1)
}

func (t *tableCompletionTracker) onBatchesCommitted(schema, table string, n int) error {
	if t == nil || n <= 0 {
		return nil
	}
	t.mu.Lock()
	st := t.states[tableKey(schema, table)]
	if st == nil {
		t.mu.Unlock()
		return nil
	}
	st.inflight -= n
	if st.inflight < 0 {
		st.inflight = 0
	}
	return t.maybeFireLocked(st)
}

func (t *tableCompletionTracker) markReadDone(schema, table string) error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	st := t.states[tableKey(schema, table)]
	if st == nil {
		t.mu.Unlock()
		return nil
	}
	st.readDone = true
	return t.maybeFireLocked(st)
}

// maybeFireLocked 要求持有 t.mu；若满足条件则释放锁后调用 onReady，再重新持锁标记 fired。
func (t *tableCompletionTracker) maybeFireLocked(st *tableCompletionState) error {
	if st.fired || !st.readDone || st.inflight != 0 {
		t.mu.Unlock()
		return nil
	}
	st.fired = true
	schema, table := st.schema, st.table
	cb := t.onReady
	t.mu.Unlock()
	if cb == nil {
		return nil
	}
	if err := cb(schema, table); err != nil {
		return fmt.Errorf("table data ready callback for %s.%s: %w", schema, table, err)
	}
	return nil
}
