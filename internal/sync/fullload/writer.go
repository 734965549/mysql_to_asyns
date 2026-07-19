package fullload

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const writeMaxRetries = 5

const maxPreparedStatementsPerWriter = 128

// CommitCallback 在一个事务成功提交后按表回调，用于推进进度（提交后才计数）。
type CommitCallback func(schema, table string, rows, bytes int64)

// runWriters 启动写入 worker 池。每个 worker 持有独立的目标连接与会话优化。
//
// P0 正确性：一个事务内可能已成功写入若干批次，后续批次遇到可重试锁错误时，
// 回滚整个事务并重放全部未提交批次，而不是只重试当前批次；只有事务提交成功后
// 才通过 onCommit 推进进度，避免静默缺数。
func runWriters(ctx context.Context, targetDB *sql.DB, q *batchQueue, opt Options, stats *Stats, onCommit CommitCallback, isStopped func() bool) error {
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
			if err := writerLoop(workerCtx, id, targetDB, q, opt, stats, onCommit, isStopped); err != nil {
				setErr(err)
			}
		}(w)
	}

	wg.Wait()
	return firstErr
}

func writerLoop(ctx context.Context, id int, targetDB *sql.DB, q *batchQueue, opt Options, stats *Stats, onCommit CommitCallback, isStopped func() bool) error {
	conn, err := targetDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("writer %d get conn: %w", id, err)
	}
	defer conn.Close()

	if err := setupWriteSession(ctx, conn, opt.SkipBinlog); err != nil {
		return fmt.Errorf("writer %d setup session: %w", id, err)
	}
	defer restoreWriteSession(conn, opt.SkipBinlog)
	stmtCache := newPreparedStmtCache(conn, maxPreparedStatementsPerWriter)
	defer stmtCache.Close()

	var (
		tx      *sql.Tx
		buf     []*RowBatch
		txRows  int64
		txBytes int64
		txStart time.Time
	)

	rollback := func() {
		if tx != nil {
			tx.Rollback()
			tx = nil
		}
		buf = nil
		txRows, txBytes = 0, 0
	}

	commit := func() error {
		if tx == nil {
			return nil
		}
		cs := time.Now()
		cErr := tx.Commit()
		if cErr != nil {
			tx = nil
			// Commit 返回连接类错误时，服务端是否已经提交不可判定。普通 INSERT 不是幂等写，
			// 此处禁止重放，否则可能把已提交事务再插入一次，尤其会静默复制无主键行。
			return fmt.Errorf("writer %d commit (outcome unknown; transaction not replayed): %w", id, cErr)
		}
		dur := time.Since(cs)
		stats.addCommit(txRows, txBytes, dur)
		reportCommitted(buf, onCommit)
		tx = nil
		buf = nil
		txRows, txBytes = 0, 0
		return nil
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

		ws := time.Now()
		if tx == nil {
			tx, err = conn.BeginTx(ctx, nil)
			if err != nil {
				return fmt.Errorf("writer %d begin tx: %w", id, err)
			}
			txStart = time.Now()
		}

		wErr := writeBatchInTxCached(ctx, tx, batch, opt, stmtCache)
		if wErr != nil {
			if !isRetryableTxConflict(wErr) {
				rollback()
				return fmt.Errorf("writer %d write batch %s: %w", id, batch.ChunkID, wErr)
			}
			stats.incLockRetries()
			// 回滚整个事务并重放全部未提交批次 + 当前批次。
			tx.Rollback()
			tx = nil
			replayBatches := append(append([]*RowBatch{}, buf...), batch)
			newTx, rErr := replayInFreshTx(ctx, conn, replayBatches, opt, stmtCache, stats)
			if rErr != nil {
				buf = nil
				txRows, txBytes = 0, 0
				return fmt.Errorf("writer %d replay tx: %w", id, rErr)
			}
			tx = newTx
			txStart = time.Now()
			buf = replayBatches
			txRows, txBytes = sumBuf(buf)
		} else {
			buf = append(buf, batch)
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

func reportCommitted(buf []*RowBatch, onCommit CommitCallback) {
	if onCommit == nil {
		return
	}
	type key struct{ schema, table string }
	agg := make(map[key][2]int64)
	for _, b := range buf {
		k := key{b.Schema, b.Table}
		v := agg[k]
		v[0] += int64(len(b.Rows))
		v[1] += b.ApproxBytes
		agg[k] = v
	}
	for k, v := range agg {
		onCommit(k.schema, k.table, v[0], v[1])
	}
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
	return fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES ",
		quoteIdentifier(batch.TargetSchema), quoteIdentifier(batch.TargetTable), strings.Join(cols, ", "))
}

// isRetryableTxConflict 仅判断事务内语句可安全重放的锁冲突。
// 连接错误和 Commit 错误的提交结果可能未知，不能用普通 INSERT 自动重放。
func isRetryableTxConflict(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "deadlock") ||
		strings.Contains(s, "lock wait timeout") ||
		strings.Contains(s, "try restarting transaction")
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
