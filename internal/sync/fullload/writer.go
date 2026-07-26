package fullload

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mysql-to-sync/pkg/logger"
)

const writeMaxRetries = 5

const writeConnMaxRetries = 5

const writerConnCloseTimeout = 3 * time.Second

const maxPreparedStatementsPerWriter = 128

// forceCloseWriterConn 关闭 writer 持有的连接；若 Close 因残留事务等阻塞，超时后放弃等待，避免拖死整个写入循环。
func forceCloseWriterConn(conn *sql.Conn) {
	if conn == nil {
		return
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = conn.Close()
	}()
	select {
	case <-done:
	case <-time.After(writerConnCloseTimeout):
		logger.Warn("[FullLoadV2] writer conn close timed out; abandoning connection")
	}
}

// CommitCallback 在一个事务成功提交后按表回调，用于推进进度（提交后才计数）。
type CommitCallback func(schema, table string, rows, bytes int64)

// runWriters 启动写入 worker 池。每个 worker 持有独立的目标连接与会话优化。
//
// P0 正确性：一个事务内可能已成功写入若干批次，后续批次遇到可重试锁错误时，
// 回滚整个事务并重放全部未提交批次，而不是只重试当前批次；只有事务提交成功后
// 才通过 onCommit 推进进度，避免静默缺数。
func runWriters(ctx context.Context, taskID string, targetDB *sql.DB, q *batchQueue, opt Options, stats *Stats, onCommit CommitCallback, tracker *tableCompletionTracker, stateTracker *tableStateTracker, isStopped func() bool, runID string) error {
	if runID == "" {
		return fmt.Errorf("runWriters: run_id is required")
	}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	q.watchContext(workerCtx)

	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once
	setErr := func(err error) {
		if err != nil {
			errOnce.Do(func() {
				firstErr = err
				cancel()
			})
		}
	}

	for w := 0; w < opt.WriteWorkers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			atomic.AddInt64(&stats.ActiveWriters, 1)
			defer atomic.AddInt64(&stats.ActiveWriters, -1)
			if err := writerLoop(workerCtx, taskID, id, targetDB, q, opt, stats, onCommit, tracker, stateTracker, isStopped, runID); err != nil {
				setErr(err)
			}
		}(w)
	}

	wg.Wait()
	return firstErr
}

func writerLoop(ctx context.Context, taskID string, id int, targetDB *sql.DB, q *batchQueue, opt Options, stats *Stats, onCommit CommitCallback, tracker *tableCompletionTracker, stateTracker *tableStateTracker, isStopped func() bool, runID string) error {
	conn, err := targetDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("writer %d get conn: %w", id, err)
	}
	var stmtCache *preparedStmtCache
	defer func() {
		if stmtCache != nil {
			stmtCache.Close()
		}
		if conn != nil {
			// ctx 已取消（流水线停止/失败）时跳过 restoreWriteSession：连接即将被 forceCloseWriterConn
			// 强制关闭，此时在独立 ctx 上执行 SET FK_CHECKS=1 等恢复语句既无意义又会产生告警噪音/阻塞。
			skipRestore := ctx.Err() != nil
			restoreWriteSession(conn, opt.SkipBinlog, taskID, id, skipRestore)
			forceCloseWriterConn(conn)
		}
	}()

	if err := setupWriteSession(ctx, conn, opt.SkipBinlog); err != nil {
		return fmt.Errorf("writer %d setup session: %w", id, err)
	}
	stmtCache = newPreparedStmtCache(conn, maxPreparedStatementsPerWriter)

	// reconnect 在连接失效后换新连接并重建会话/预处理缓存。
	// 未提交事务会随断连被服务端回滚，因此可安全重放；Commit 失败仍禁止重放。
	// 调用方须先结束当前 *sql.Tx（Rollback），否则 Conn.Close 会与 awaitDone 死锁。
	reconnect := func(cause error) error {
		logger.Warn("[Task %s] FullLoadV2 writer %d reconnecting after connection error: %v", taskID, id, cause)
		oldCache := stmtCache
		oldConn := conn
		stmtCache = nil
		conn = nil
		if oldCache != nil {
			oldCache.Close()
		}
		forceCloseWriterConn(oldConn)

		var lastErr error
		for attempt := 1; attempt <= writeConnMaxRetries; attempt++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(25+attempt*25) * time.Millisecond):
			}
			c, cErr := targetDB.Conn(ctx)
			if cErr != nil {
				lastErr = cErr
				continue
			}
			if sErr := setupWriteSession(ctx, c, opt.SkipBinlog); sErr != nil {
				forceCloseWriterConn(c)
				lastErr = sErr
				continue
			}
			conn = c
			stmtCache = newPreparedStmtCache(conn, maxPreparedStatementsPerWriter)
			return nil
		}
		if lastErr == nil {
			lastErr = cause
		}
		return fmt.Errorf("reconnect exhausted after %d attempts: %w", writeConnMaxRetries, lastErr)
	}

	var (
		tx               *sql.Tx
		buf              []*RowBatch
		txRows           int64
		txBytes          int64
		txStart          time.Time
		commitRecoveries int
		txMarkerID       string
		txMarkerSchema   string
	)

	clearTxMarker := func() {
		txMarkerID = ""
		txMarkerSchema = ""
	}

	rollback := func() {
		if tx != nil {
			_ = tx.Rollback()
			tx = nil
		}
		// 回滚未提交批次：必须按批次回减 reader 预增的 attempt inflight。
		if stateTracker != nil {
			for _, b := range buf {
				if b != nil && b.AttemptID > 0 {
					stateTracker.onBatchReleased(b.Schema, b.Table, b.AttemptID)
				}
			}
		}
		buf = nil
		txRows, txBytes = 0, 0
		clearTxMarker()
	}

	var replayAfterFailure func(cause error, replayBatches []*RowBatch) (*sql.Tx, error)

	// commit 提交当前事务。连接类 Commit 错误时结果未知：先换连，再用事务 marker
	// 的锁定当前读判定是否已落库。禁止根据业务行存在性猜测（无主键会误判）。
	var commit func() error
	commit = func() error {
		if tx == nil {
			return nil
		}
		cs := time.Now()
		pending := buf
		markerID := txMarkerID
		markerSchema := txMarkerSchema
		if markerID == "" || markerSchema == "" {
			_ = tx.Rollback()
			tx = nil
			buf = nil
			txRows, txBytes = 0, 0
			clearTxMarker()
			return fmt.Errorf("writer %d commit: missing tx marker (fail-closed)", id)
		}
		if err := insertTxMarker(ctx, tx, markerSchema, markerID, runID); err != nil {
			_ = tx.Rollback()
			tx = nil
			buf = nil
			txRows, txBytes = 0, 0
			clearTxMarker()
			return fmt.Errorf("writer %d commit marker: %w", id, err)
		}
		cErr := tx.Commit()
		tx = nil
		if cErr != nil {
			if !isConnRetryable(cErr) {
				buf = nil
				txRows, txBytes = 0, 0
				clearTxMarker()
				return fmt.Errorf("writer %d commit: %w", id, cErr)
			}
			if commitRecoveries >= writeConnMaxRetries {
				buf = nil
				txRows, txBytes = 0, 0
				clearTxMarker()
				return fmt.Errorf("writer %d commit (outcome unknown; recover exhausted): %w", id, cErr)
			}
			commitRecoveries++
			logger.Warn("[Task %s] FullLoadV2 writer %d commit connection error (outcome unknown), verifying marker: %v", taskID, id, cErr)
			buf = nil
			txRows, txBytes = 0, 0
			if rErr := reconnect(cErr); rErr != nil {
				clearTxMarker()
				return fmt.Errorf("writer %d commit (outcome unknown; reconnect failed): %w", id, rErr)
			}
			applied, vErr := txMarkerApplied(ctx, conn, markerSchema, markerID)
			if vErr != nil {
				clearTxMarker()
				return fmt.Errorf("writer %d commit (outcome unknown; verify failed): %w", id, vErr)
			}
			if applied {
				logger.Warn("[Task %s] FullLoadV2 writer %d commit verified applied via tx marker after connection error", taskID, id)
				rows, bytes := sumBuf(pending)
				stats.addCommit(rows, bytes, time.Since(cs))
				commitRecoveries = 0
				clearTxMarker()
				return reportCommitted(pending, onCommit, tracker, stateTracker)
			}
			logger.Warn("[Task %s] FullLoadV2 writer %d commit verified rolled back via tx marker; replaying %d batches", taskID, id, len(pending))
			clearTxMarker()
			newTx, rErr := replayInFreshTx(ctx, conn, pending, opt, stmtCache, stats)
			if rErr != nil {
				if isConnRetryable(rErr) {
					newTx, rErr = replayAfterFailure(rErr, pending)
				}
				if rErr != nil {
					return fmt.Errorf("writer %d commit (outcome unknown; replay failed): %w", id, rErr)
				}
			}
			tx = newTx
			txStart = time.Now()
			buf = pending
			txRows, txBytes = sumBuf(buf)
			if len(buf) > 0 && buf[0] != nil {
				txMarkerSchema = buf[0].TargetSchema
			}
			var mErr error
			txMarkerID, mErr = newTxMarkerID()
			if mErr != nil {
				rollback()
				return fmt.Errorf("writer %d allocate tx marker after replay: %w", id, mErr)
			}
			return commit()
		}
		dur := time.Since(cs)
		stats.addCommit(txRows, txBytes, dur)
		if err := reportCommitted(pending, onCommit, tracker, stateTracker); err != nil {
			buf = nil
			txRows, txBytes = 0, 0
			clearTxMarker()
			return err
		}
		buf = nil
		txRows, txBytes = 0, 0
		commitRecoveries = 0
		clearTxMarker()
		return nil
	}

	// beginTx 开启事务；连接失效时换连后重试。同时分配本事务唯一 marker。
	beginTx := func() error {
		var lastErr error
		for attempt := 0; attempt <= writeConnMaxRetries; attempt++ {
			if attempt > 0 {
				if rErr := reconnect(lastErr); rErr != nil {
					return rErr
				}
			}
			markerID, mErr := newTxMarkerID()
			if mErr != nil {
				return fmt.Errorf("allocate tx marker: %w", mErr)
			}
			var bErr error
			tx, bErr = conn.BeginTx(ctx, nil)
			if bErr == nil {
				txStart = time.Now()
				txMarkerID = markerID
				txMarkerSchema = ""
				return nil
			}
			lastErr = bErr
			if !isConnRetryable(bErr) {
				return bErr
			}
			tx = nil
			clearTxMarker()
		}
		return lastErr
	}

	// replayAfterFailure 在锁冲突或连接失效后重放未提交批次。
	// 连接失效先换连；重放过程再次断连则继续换连，直到成功或耗尽重试。
	replayAfterFailure = func(cause error, replayBatches []*RowBatch) (*sql.Tx, error) {
		needReconnect := isConnRetryable(cause)
		var lastErr error = cause
		for attempt := 0; attempt <= writeConnMaxRetries; attempt++ {
			if needReconnect {
				if rErr := reconnect(lastErr); rErr != nil {
					return nil, rErr
				}
			}
			newTx, rErr := replayInFreshTx(ctx, conn, replayBatches, opt, stmtCache, stats)
			if rErr == nil {
				return newTx, nil
			}
			lastErr = rErr
			if !isConnRetryable(rErr) {
				return nil, rErr
			}
			needReconnect = true
		}
		return nil, fmt.Errorf("replay after connection error exhausted: %w", lastErr)
	}

	for {
		if ctx.Err() != nil || (isStopped != nil && isStopped()) {
			// 暂停/取消：回滚未提交事务，不把未提交数据计入进度。
			rollback()
			return nil
		}

		var deadline time.Time
		if tx != nil {
			deadline = txStart.Add(opt.CommitInterval)
		}
		batch, ok, timedOut := q.GetUntil(ctx, deadline)
		if ctx.Err() != nil || (isStopped != nil && isStopped()) {
			rollback()
			return nil
		}
		if timedOut {
			if err := commit(); err != nil {
				return err
			}
			continue
		}
		if !ok {
			break
		}

		rows := int64(len(batch.Rows))
		bytes := batch.ApproxBytes

		// 旧 attempt 批次：reader 已预增 inflight；若 attempt 已推进则静默丢弃并释放计数。
		if stateTracker != nil && batch.AttemptID > 0 &&
			!stateTracker.isCurrentAttempt(batch.Schema, batch.Table, batch.AttemptID) {
			stateTracker.onBatchReleased(batch.Schema, batch.Table, batch.AttemptID)
			continue
		}

		ws := time.Now()
		if tx == nil {
			if err := beginTx(); err != nil {
				return fmt.Errorf("writer %d begin tx: %w", id, err)
			}
		}

		wErr := writeBatchInTxCached(ctx, tx, batch, opt, stmtCache)
		if wErr != nil {
			if !isRetryableTxConflict(wErr) && !isConnRetryable(wErr) {
				rollback()
				return fmt.Errorf("writer %d write batch %s: %w", id, batch.ChunkID, wErr)
			}
			if isRetryableTxConflict(wErr) {
				stats.incLockRetries()
			}
			// 无论锁冲突还是连接失效，都先 Rollback 释放 *sql.Tx 对 Conn 的占用；
			// 否则后续 conn.Close()/换连会卡在 awaitDone。断连时 Rollback 可能失败，可忽略。
			if tx != nil {
				_ = tx.Rollback()
				tx = nil
			}
			replayBatches := append(append([]*RowBatch{}, buf...), batch)
			newTx, rErr := replayAfterFailure(wErr, replayBatches)
			if rErr != nil {
				buf = nil
				txRows, txBytes = 0, 0
				return fmt.Errorf("writer %d write batch %s: %w", id, batch.ChunkID, rErr)
			}
			tx = newTx
			txStart = time.Now()
			buf = replayBatches
			txRows, txBytes = sumBuf(buf)
			if txMarkerID == "" {
				mid, mErr := newTxMarkerID()
				if mErr != nil {
					rollback()
					return fmt.Errorf("writer %d allocate tx marker after write replay: %w", id, mErr)
				}
				txMarkerID = mid
			}
			if txMarkerSchema == "" && len(buf) > 0 && buf[0] != nil {
				txMarkerSchema = buf[0].TargetSchema
			}
		} else {
			buf = append(buf, batch)
			if txMarkerSchema == "" {
				txMarkerSchema = batch.TargetSchema
			}
			txRows += rows
			txBytes += bytes
		}
		stats.addWriteBatch(rows, bytes, time.Since(ws))

		if txRows >= opt.CommitRows || txBytes >= opt.CommitBytes || time.Since(txStart) >= opt.CommitInterval {
			if err := commit(); err != nil {
				return err
			}
		}
	}

	// 队列耗尽：提交剩余未提交事务。
	if err := commit(); err != nil {
		return err
	}
	return nil
}

func reportCommitted(buf []*RowBatch, onCommit CommitCallback, tracker *tableCompletionTracker, stateTracker *tableStateTracker) error {
	if onCommit == nil && tracker == nil && stateTracker == nil {
		return nil
	}
	type key struct{ schema, table string }
	agg := make(map[key][2]int64)
	batches := make(map[key]int)
	attempts := make(map[key]int)
	for _, b := range buf {
		k := key{b.Schema, b.Table}
		v := agg[k]
		v[0] += int64(len(b.Rows))
		v[1] += b.ApproxBytes
		agg[k] = v
		batches[k]++
		if b.AttemptID > 0 {
			attempts[k] = b.AttemptID
		}
	}
	if onCommit != nil {
		for k, v := range agg {
			onCommit(k.schema, k.table, v[0], v[1])
		}
	}
	for k, n := range batches {
		if err := tracker.onBatchesCommitted(k.schema, k.table, n); err != nil {
			return err
		}
	}
	// 按实际批次数回减 attempt inflight（同一事务可能含多批）。
	if stateTracker != nil {
		for k, n := range batches {
			attemptID := attempts[k]
			if attemptID <= 0 {
				continue
			}
			rows := agg[k][0]
			stateTracker.onBatchesCommitted(k.schema, k.table, attemptID, n, rows)
		}
	}
	return nil
}

func sumBuf(buf []*RowBatch) (rows, bytes int64) {
	for _, b := range buf {
		rows += int64(len(b.Rows))
		bytes += b.ApproxBytes
	}
	return
}

func replayInFreshTx(ctx context.Context, conn *sql.Conn, batches []*RowBatch, opt Options, stmtCache *preparedStmtCache, stats *Stats) (*sql.Tx, error) {
	var lastErr error
	for attempt := 1; attempt <= writeMaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(attempt*attempt*20) * time.Millisecond):
		}
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}
		stats.incTxReplays()
		ok := true
		for _, b := range batches {
			if err := writeBatchInTxCached(ctx, tx, b, opt, stmtCache); err != nil {
				lastErr = err
				tx.Rollback()
				ok = false
				if !isRetryableTxConflict(err) {
					return nil, err
				}
				stats.incLockRetries()
				break
			}
		}
		if ok {
			return tx, nil
		}
	}
	return nil, fmt.Errorf("replay exhausted retries: %w", lastErr)
}

// writeBatchInTx 用普通多值 INSERT 写入一个批次，按占位符上限与字节上限拆分为多条语句。
func writeBatchInTx(ctx context.Context, tx *sql.Tx, batch *RowBatch, opt Options) error {
	return writeBatchInTxCached(ctx, tx, batch, opt, nil)
}

func writeBatchInTxCached(ctx context.Context, tx *sql.Tx, batch *RowBatch, opt Options, stmtCache *preparedStmtCache) error {
	nCols := len(batch.Columns)
	if nCols == 0 || len(batch.Rows) == 0 {
		return nil
	}
	maxRowsByPlaceholder := mysqlMaxPlaceholders / nCols
	if maxRowsByPlaceholder < 1 {
		maxRowsByPlaceholder = 1
	}
	maxRowsPerStmt := opt.BatchRows
	if maxRowsPerStmt < 1 || maxRowsPerStmt > maxRowsByPlaceholder {
		maxRowsPerStmt = maxRowsByPlaceholder
	}

	prefix := insertPrefix(batch)
	rowPlaceholder := "(" + strings.TrimSuffix(strings.Repeat("?, ", nCols), ", ") + ")"
	for rowIndex, row := range batch.Rows {
		if len(row) != nCols {
			return fmt.Errorf("row %d has %d values, want %d columns", rowIndex, len(row), nCols)
		}
	}

	i := 0
	for i < len(batch.Rows) {
		end := i + maxRowsPerStmt
		if end > len(batch.Rows) {
			end = len(batch.Rows)
		}
		// 字节上限拆分：从 i 起累计到 batchBytes。
		var subBytes int64
		j := i
		for j < end {
			var rb int64
			for _, v := range batch.Rows[j] {
				rb += estimateValueBytes(v)
			}
			if j > i && subBytes+rb > opt.BatchBytes {
				break
			}
			subBytes += rb
			j++
		}
		if j == i {
			j = i + 1
		}

		chunk := batch.Rows[i:j]
		var sb strings.Builder
		sb.Grow(len(prefix) + len(rowPlaceholder)*len(chunk))
		sb.WriteString(prefix)
		args := make([]any, 0, len(chunk)*nCols)
		for k, row := range chunk {
			if k > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(rowPlaceholder)
			args = append(args, row...)
		}
		if err := execInsert(ctx, tx, stmtCache, sb.String(), args...); err != nil {
			return err
		}
		i = j
	}
	return nil
}

func insertPrefix(batch *RowBatch) string {
	cols := make([]string, len(batch.Columns))
	for i, c := range batch.Columns {
		cols[i] = quoteIdentifier(c)
	}
	writeTable := batch.TargetTable
	if batch.StagingTable != "" {
		writeTable = batch.StagingTable
	}
	return fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES ",
		quoteIdentifier(batch.TargetSchema), quoteIdentifier(writeTable), strings.Join(cols, ", "))
}

// isRetryableTxConflict 仅判断事务内语句可安全重放的锁冲突。
func isRetryableTxConflict(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "deadlock") ||
		strings.Contains(s, "lock wait timeout") ||
		strings.Contains(s, "try restarting transaction")
}

// isConnRetryable 判断连接失效类错误。未提交事务随断连被服务端回滚，可换连接后重放。
func isConnRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, driver.ErrBadConn) {
		return true
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "invalid connection") ||
		strings.Contains(s, "bad connection") ||
		strings.Contains(s, "unexpected packet") ||
		strings.Contains(s, "connection was bad") ||
		strings.Contains(s, "connection refused") ||
		strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "connection reset")
}

type preparedStmtCache struct {
	conn    *sql.Conn
	limit   int
	stmts   map[string]*sql.Stmt
	order   []string
	tx      *sql.Tx
	txStmts map[string]*sql.Stmt
}

func newPreparedStmtCache(conn *sql.Conn, limit int) *preparedStmtCache {
	return &preparedStmtCache{conn: conn, limit: limit, stmts: make(map[string]*sql.Stmt)}
}

func (c *preparedStmtCache) statement(ctx context.Context, query string) (*sql.Stmt, error) {
	if stmt := c.stmts[query]; stmt != nil {
		return stmt, nil
	}
	stmt, err := c.conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	if c.limit > 0 && len(c.order) >= c.limit {
		oldest := c.order[0]
		c.order = c.order[1:]
		_ = c.stmts[oldest].Close()
		delete(c.stmts, oldest)
	}
	c.stmts[query] = stmt
	c.order = append(c.order, query)
	return stmt, nil
}

func (c *preparedStmtCache) transactionStatement(ctx context.Context, tx *sql.Tx, query string) (*sql.Stmt, error) {
	if c.tx != tx {
		// database/sql 会在 Commit/Rollback 时关闭由 Tx.StmtContext 派生的语句。
		c.tx = tx
		c.txStmts = make(map[string]*sql.Stmt)
	}
	if stmt := c.txStmts[query]; stmt != nil {
		return stmt, nil
	}
	parent, err := c.statement(ctx, query)
	if err != nil {
		return nil, err
	}
	stmt := tx.StmtContext(ctx, parent)
	c.txStmts[query] = stmt
	return stmt, nil
}

func (c *preparedStmtCache) Close() {
	if c == nil {
		return
	}
	for _, stmt := range c.stmts {
		_ = stmt.Close()
	}
	c.stmts = nil
	c.order = nil
	c.tx = nil
	c.txStmts = nil
}

func execInsert(ctx context.Context, tx *sql.Tx, cache *preparedStmtCache, query string, args ...any) error {
	if cache == nil {
		_, err := tx.ExecContext(ctx, query, args...)
		return err
	}
	stmt, err := cache.transactionStatement(ctx, tx, query)
	if err != nil {
		return err
	}
	_, err = stmt.ExecContext(ctx, args...)
	return err
}
