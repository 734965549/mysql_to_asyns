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

// CommitCallback 在一个事务成功提交后按表回调，用于推进进度（提交后才计数）。
type CommitCallback func(schema, table string, rows, bytes int64)

// runWriters 启动写入 worker 池。每个 worker 持有独立的目标连接与会话优化。
//
// P0 正确性：一个事务内可能已成功写入若干批次，后续批次遇到可重试锁错误时，
// 回滚整个事务并重放全部未提交批次，而不是只重试当前批次；只有事务提交成功后
// 才通过 onCommit 推进进度，避免静默缺数。
func runWriters(ctx context.Context, targetDB *sql.DB, q *batchQueue, opt Options, stats *Stats, onCommit CommitCallback, isStopped func() bool) error {
	var wg sync.WaitGroup
	var firstErr atomic.Value
	setErr := func(err error) {
		if err != nil {
			firstErr.CompareAndSwap(nil, err)
		}
	}

	for w := 0; w < opt.WriteWorkers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			atomic.AddInt64(&stats.ActiveWriters, 1)
			defer atomic.AddInt64(&stats.ActiveWriters, -1)
			if err := writerLoop(ctx, id, targetDB, q, opt, stats, onCommit, isStopped); err != nil {
				setErr(err)
			}
		}(w)
	}

	wg.Wait()
	if v := firstErr.Load(); v != nil {
		return v.(error)
	}
	return nil
}

func writerLoop(ctx context.Context, id int, targetDB *sql.DB, q *batchQueue, opt Options, stats *Stats, onCommit CommitCallback, isStopped func() bool) error {
	conn, err := targetDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("writer %d get conn: %w", id, err)
	}
	defer conn.Close()

	label := fmt.Sprintf("v2-writer-%d", id)
	if err := setupWriteSession(ctx, conn, opt.SkipBinlog); err != nil {
		return fmt.Errorf("writer %d setup session: %w", id, err)
	}
	defer restoreWriteSession(conn, opt.SkipBinlog)
	_ = label

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
			tx.Rollback()
			tx = nil
			if !isRetryableWriteErr(cErr) {
				return fmt.Errorf("writer %d commit: %w", id, cErr)
			}
			// 提交阶段的可重试错误：重放整个事务缓冲后重试提交。
			newTx, rErr := replayInFreshTx(ctx, conn, buf, opt)
			if rErr != nil {
				return fmt.Errorf("writer %d replay after commit failure: %w", id, rErr)
			}
			stats.incTxReplays()
			cs = time.Now()
			if cErr2 := newTx.Commit(); cErr2 != nil {
				newTx.Rollback()
				tx = nil
				return fmt.Errorf("writer %d commit after replay: %w", id, cErr2)
			}
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

		batch, ok := q.Get(ctx)
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

		wErr := writeBatchInTx(ctx, tx, batch, opt)
		if wErr != nil {
			if !isRetryableWriteErr(wErr) {
				rollback()
				return fmt.Errorf("writer %d write batch %s: %w", id, batch.ChunkID, wErr)
			}
			stats.incLockRetries()
			// 回滚整个事务并重放全部未提交批次 + 当前批次。
			tx.Rollback()
			tx = nil
			replayBatches := append(append([]*RowBatch{}, buf...), batch)
			newTx, rErr := replayInFreshTx(ctx, conn, replayBatches, opt)
			if rErr != nil {
				buf = nil
				txRows, txBytes = 0, 0
				return fmt.Errorf("writer %d replay tx: %w", id, rErr)
			}
			tx = newTx
			txStart = time.Now()
			buf = replayBatches
			txRows, txBytes = sumBuf(buf)
			stats.incTxReplays()
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

func replayInFreshTx(ctx context.Context, conn *sql.Conn, batches []*RowBatch, opt Options) (*sql.Tx, error) {
	var lastErr error
	for attempt := 1; attempt <= writeMaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(attempt*attempt*20) * time.Millisecond):
		}
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			lastErr = err
			continue
		}
		ok := true
		for _, b := range batches {
			if err := writeBatchInTx(ctx, tx, b, opt); err != nil {
				lastErr = err
				tx.Rollback()
				ok = false
				if !isRetryableWriteErr(err) {
					return nil, err
				}
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
		if _, err := tx.ExecContext(ctx, sb.String(), args...); err != nil {
			return err
		}
		i = j
	}
	return nil
}

func insertPrefix(batch *RowBatch) string {
	cols := make([]string, len(batch.Columns))
	for i, c := range batch.Columns {
		cols[i] = "`" + strings.ReplaceAll(c, "`", "``") + "`"
	}
	return fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES ",
		batch.TargetSchema, batch.TargetTable, strings.Join(cols, ", "))
}

// isRetryableWriteErr 判断写入错误是否为可重试的锁冲突或连接失效。
func isRetryableWriteErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "deadlock") ||
		strings.Contains(s, "lock wait timeout") ||
		strings.Contains(s, "try restarting transaction") ||
		strings.Contains(s, "bad connection") ||
		strings.Contains(s, "invalid connection")
}
