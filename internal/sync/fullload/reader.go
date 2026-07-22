package fullload

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mysql-to-sync/pkg/logger"
)

// chunkReader 以 map-free 方式读取单个 chunk，直接扫描成 [][]any。
type chunkReader struct {
	queryer    snapshotQueryer
	chunk      *Chunk
	cols       []string // 固定列顺序
	selectSQL  string   // "`c1`, `c2`, ..."
	batchRows  int
	batchBytes int64

	cursorCols []string
	cursorIdx  []int // cursorCols 在 cols 中的位置

	stream *sql.Rows // 仅无主键流式使用
	cursor []any     // keyset 游标当前值
	done   bool
}

func newChunkReader(queryer snapshotQueryer, chunk *Chunk, batchRows int, batchBytes int64) (*chunkReader, error) {
	id := chunk.Spec.Identity
	if id == nil || len(id.Columns) == 0 {
		return nil, fmt.Errorf("chunk %s has no table columns", chunk.ID)
	}
	cols := make([]string, 0, len(id.Columns))
	for _, c := range id.Columns {
		cols = append(cols, c.Name)
	}
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = quoteIdentifier(c)
	}

	cr := &chunkReader{
		queryer:    queryer,
		chunk:      chunk,
		cols:       cols,
		selectSQL:  strings.Join(parts, ", "),
		batchRows:  batchRows,
		batchBytes: batchBytes,
	}
	if !chunk.NoPK {
		cr.cursorCols = id.EffectiveCursorCols()
		for _, cc := range cr.cursorCols {
			for i, c := range cols {
				if c == cc {
					cr.cursorIdx = append(cr.cursorIdx, i)
					break
				}
			}
		}
		if len(cr.cursorCols) == 0 || len(cr.cursorIdx) != len(cr.cursorCols) {
			return nil, fmt.Errorf("chunk %s cursor columns are missing from table columns", chunk.ID)
		}
	}
	return cr, nil
}

func (r *chunkReader) close() {
	if r.stream != nil {
		_ = r.stream.Close()
		r.stream = nil
	}
}

// nextBatch 返回下一批；无更多数据时返回 (nil, nil)。
func (r *chunkReader) nextBatch(ctx context.Context) (*RowBatch, error) {
	if r.done {
		return nil, nil
	}
	if r.chunk.NoPK {
		return r.nextStreamBatch(ctx)
	}
	return r.nextKeysetBatch(ctx)
}

func (r *chunkReader) nextStreamBatch(ctx context.Context) (*RowBatch, error) {
	if r.stream == nil {
		q := fmt.Sprintf("SELECT %s FROM %s.%s", r.selectSQL,
			quoteIdentifier(r.chunk.Spec.SourceSchema), quoteIdentifier(r.chunk.Spec.SourceTable))
		rows, err := r.queryer.QueryContext(ctx, q)
		if err != nil {
			return nil, err
		}
		r.stream = rows
	}
	rowsData, bytes, exhausted, err := scanUpTo(r.stream, len(r.cols), r.batchRows, r.batchBytes)
	if err != nil {
		r.stream.Close()
		r.stream = nil
		r.done = true
		return nil, err
	}
	if len(rowsData) == 0 {
		r.stream.Close()
		r.stream = nil
		r.done = true
		return nil, nil
	}
	if exhausted {
		r.stream.Close()
		r.stream = nil
		r.done = true
	}
	return r.makeBatch(rowsData, bytes), nil
}

func (r *chunkReader) nextKeysetBatch(ctx context.Context) (*RowBatch, error) {
	where, args := r.buildWhere()
	q := fmt.Sprintf("SELECT %s FROM %s.%s", r.selectSQL,
		quoteIdentifier(r.chunk.Spec.SourceSchema), quoteIdentifier(r.chunk.Spec.SourceTable))
	if where != "" {
		q += " WHERE " + where
	}
	q += " ORDER BY " + r.orderBy() + " LIMIT ?"
	args = append(args, r.batchRows)

	rows, err := r.queryer.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	rowsData, bytes, exhausted, err := scanUpTo(rows, len(r.cols), r.batchRows, r.batchBytes)
	rows.Close()
	if err != nil {
		return nil, err
	}
	if len(rowsData) == 0 {
		r.done = true
		return nil, nil
	}

	// 推进游标到最后一行的游标列值。
	lastRow := rowsData[len(rowsData)-1]
	r.cursor = make([]any, len(r.cursorIdx))
	for i, idx := range r.cursorIdx {
		r.cursor[i] = lastRow[idx]
	}
	if exhausted {
		r.done = true
	}
	return r.makeBatch(rowsData, bytes), nil
}

// buildWhere 构建 keyset 下界（游标或 chunk.Start）与单列上界（chunk.End）。
func (r *chunkReader) buildWhere() (string, []any) {
	var conds []string
	var args []any

	// 下界：优先使用已推进的游标，否则使用 chunk.Start。
	lower := r.cursor
	if lower == nil {
		lower = r.chunk.Start
	}
	if len(lower) > 0 {
		lw, la := buildKeysetLower(r.cursorCols, lower)
		conds = append(conds, "("+lw+")")
		args = append(args, la...)
	}

	// 上界：仅单列 chunk（值域/边界切分）携带 End。
	if len(r.chunk.End) == 1 && len(r.cursorCols) == 1 {
		conds = append(conds, fmt.Sprintf("%s <= ?", quoteIdentifier(r.cursorCols[0])))
		args = append(args, r.chunk.End[0])
	} else if len(r.chunk.End) == len(r.cursorCols) && len(r.cursorCols) > 1 {
		upper, upperArgs := buildKeysetUpperInclusive(r.cursorCols, r.chunk.End)
		conds = append(conds, "("+upper+")")
		args = append(args, upperArgs...)
	}

	return strings.Join(conds, " AND "), args
}

func (r *chunkReader) orderBy() string {
	return orderByColumns(r.cursorCols)
}

func (r *chunkReader) makeBatch(rowsData [][]any, bytes int64) *RowBatch {
	b := &RowBatch{
		Schema:       r.chunk.Spec.SourceSchema,
		Table:        r.chunk.Spec.SourceTable,
		TargetSchema: r.chunk.Spec.TargetSchema,
		TargetTable:  r.chunk.Spec.TargetTable,
		Columns:      r.cols,
		Rows:         rowsData,
		ApproxBytes:  bytes,
		ChunkID:      r.chunk.ID,
	}
	if len(r.cursorIdx) > 0 {
		b.StartKey = cursorKey(rowsData[0], r.cursorIdx)
		b.EndKey = cursorKey(rowsData[len(rowsData)-1], r.cursorIdx)
	}
	return b
}

func cursorKey(row []any, indices []int) []any {
	key := make([]any, len(indices))
	for i, idx := range indices {
		key[i] = row[idx]
	}
	return key
}

// buildKeysetLower 构建 (c1,...,cN) > (v1,...,vN) 的展开 OR 表达式（严格大于，排他下界）。
func buildKeysetLower(cols []string, vals []any) (string, []any) {
	n := len(cols)
	if n == 1 {
		return fmt.Sprintf("%s > ?", quoteIdentifier(cols[0])), []any{vals[0]}
	}
	var branches []string
	var args []any
	for k := n; k >= 1; k-- {
		var conds []string
		for j := 0; j < k-1; j++ {
			conds = append(conds, fmt.Sprintf("%s = ?", quoteIdentifier(cols[j])))
			args = append(args, vals[j])
		}
		conds = append(conds, fmt.Sprintf("%s > ?", quoteIdentifier(cols[k-1])))
		args = append(args, vals[k-1])
		branches = append(branches, "("+strings.Join(conds, " AND ")+")")
	}
	return strings.Join(branches, " OR "), args
}

// buildKeysetUpperInclusive 构建复合游标的字典序 <= 上界。
func buildKeysetUpperInclusive(cols []string, vals []any) (string, []any) {
	if len(cols) == 1 {
		return fmt.Sprintf("%s <= ?", quoteIdentifier(cols[0])), []any{vals[0]}
	}
	var branches []string
	var args []any
	for k := 0; k < len(cols); k++ {
		conds := make([]string, 0, k+1)
		for j := 0; j < k; j++ {
			conds = append(conds, fmt.Sprintf("%s = ?", quoteIdentifier(cols[j])))
			args = append(args, vals[j])
		}
		op := "<"
		if k == len(cols)-1 {
			op = "<="
		}
		conds = append(conds, fmt.Sprintf("%s %s ?", quoteIdentifier(cols[k]), op))
		args = append(args, vals[k])
		branches = append(branches, "("+strings.Join(conds, " AND ")+")")
	}
	return strings.Join(branches, " OR "), args
}

// scanUpTo 从结果集中扫描至多 maxRows 行到 [][]any，并估算字节数。
func scanUpTo(rows *sql.Rows, nCols, maxRows int, maxBytes int64) ([][]any, int64, bool, error) {
	var out [][]any
	var bytes int64
	for i := 0; i < maxRows; i++ {
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return out, bytes, false, err
			}
			return out, bytes, true, nil
		}
		vals := make([]any, nCols)
		ptrs := make([]any, nCols)
		for j := range vals {
			ptrs[j] = &vals[j]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return out, bytes, false, err
		}
		for j := range vals {
			if b, ok := vals[j].([]byte); ok {
				cp := make([]byte, len(b))
				copy(cp, b)
				vals[j] = cp
			}
			bytes += estimateValueBytes(vals[j])
		}
		out = append(out, vals)
		if maxBytes > 0 && bytes >= maxBytes {
			break
		}
	}
	return out, bytes, false, nil
}

// runTableReaders 按表打开一致性快照：表间并发由 ReadWorkers/信号量限制；
// 超大表可在单表内用对齐多连接并行读 chunk（禁止跨表窃取事务）。
func runTableReaders(ctx context.Context, db *sql.DB, jobs []*tableReadJob, q *batchQueue, eng *Engine, opt Options, stats *Stats, isStopped func() bool) error {
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	q.watchContext(workerCtx)

	lim := eng.limiter
	if lim == nil {
		lim = newSnapshotLimiter(opt.MaxSnapshotGroups, opt.MaxSnapshotConns)
	}

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

	jobCh := make(chan *tableReadJob, len(jobs))
	for _, job := range jobs {
		jobCh <- job
	}
	close(jobCh)

	workers := opt.ReadWorkers
	if workers < 1 {
		workers = 1
	}
	if workers > opt.MaxSnapshotGroups && opt.MaxSnapshotGroups > 0 {
		workers = opt.MaxSnapshotGroups
	}

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			atomic.AddInt64(&stats.ActiveReaders, 1)
			defer atomic.AddInt64(&stats.ActiveReaders, -1)

			for job := range jobCh {
				if workerCtx.Err() != nil || (isStopped != nil && isStopped()) {
					cancel()
					return
				}
				if err := readTableWithSnapshot(workerCtx, db, job, q, eng, lim, opt, stats, isStopped, cancel); err != nil {
					setErr(err)
					return
				}
			}
		}()
	}

	wg.Wait()
	return firstErr
}

type tableReadJob struct {
	spec   *TableSpec
	chunks []*Chunk
}

func readTableWithSnapshot(ctx context.Context, db *sql.DB, job *tableReadJob, q *batchQueue, eng *Engine, lim *snapshotLimiter, opt Options, stats *Stats, isStopped func() bool, cancel context.CancelFunc) error {
	if job == nil || job.spec == nil || job.spec.Identity == nil || len(job.spec.Identity.Columns) == 0 {
		return fmt.Errorf("invalid table read job")
	}

	lease, err := lim.acquireGroup(ctx)
	if err != nil {
		return err
	}
	defer lease.release()

	readers := decideTableReadersForSpec(job.spec, opt)
	captureHWM := eng != nil && eng.CaptureTableHWM && isNoPKSpec(job.spec)
	// 无 PK/UK + ALL：必须单连接 + 表锁绑 HWM，不允许降级到无锁路径。
	if captureHWM {
		readers = 1
	}

	firstCol := job.spec.Identity.Columns[0].Name
	snapOpt := SnapshotOptions{
		CaptureHWM:         captureHWM,
		LockWaitTimeoutSec: opt.LockWaitTimeoutSec,
	}
	if eng != nil {
		snapOpt.OnReady = eng.OnTableSnapshotReady
	}

	snaps, reserved, err := openTableSnapshotsWithLimiter(ctx, db, job.spec.SourceSchema, job.spec.SourceTable, firstCol, readers, snapOpt, lim, opt, stats)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			rollbackSnapshots(ctx, snaps)
		}
		n := len(snaps)
		closeSnapshots(snaps)
		if n > 0 {
			atomic.AddInt64(&stats.ActiveSnapshotTxns, -int64(n))
		}
		lim.releaseConns(reserved)
	}()

	// 在快照事务内规划 chunk，使边界与读取共享同一 ReadView。
	targetChunks := opt.ReadWorkers * opt.ChunkOvershoot
	if targetChunks < 1 {
		targetChunks = 1
	}
	planner := NewPlanner(snaps[0].conn)
	chunks, err := planner.planTable(ctx, job.spec, targetChunks)
	if err != nil {
		return fmt.Errorf("plan chunks in snapshot for %s.%s: %w", job.spec.SourceSchema, job.spec.SourceTable, err)
	}
	job.chunks = chunks
	atomic.AddInt64(&stats.ChunksTotal, int64(len(chunks)))
	logger.Info("[FullLoadV2] planned %d chunk(s) in-snapshot for %s.%s (readers=%d)",
		len(chunks), job.spec.SourceSchema, job.spec.SourceTable, len(snaps))

	tracker := (*tableCompletionTracker)(nil)
	if eng != nil {
		tracker = eng.tracker
	}

	if len(chunks) == 0 {
		if err := tracker.markReadDone(job.spec.SourceSchema, job.spec.SourceTable); err != nil {
			return err
		}
	} else if len(snaps) == 1 {
		for _, chunk := range chunks {
			if ctx.Err() != nil || (isStopped != nil && isStopped()) {
				cancel()
				return nil
			}
			if err := readChunk(ctx, snaps[0].conn, chunk, q, opt, stats, tracker, isStopped, cancel); err != nil {
				return fmt.Errorf("read table %s.%s chunk %s: %w", job.spec.SourceSchema, job.spec.SourceTable, chunk.ID, err)
			}
		}
	} else {
		// 并行度不超过 chunk 数：多余快照连接空闲提交即可。
		active := snaps
		if len(chunks) < len(snaps) {
			active = snaps[:len(chunks)]
		}
		if err := readChunksParallel(ctx, active, chunks, q, opt, stats, tracker, isStopped, cancel); err != nil {
			return fmt.Errorf("read table %s.%s parallel: %w", job.spec.SourceSchema, job.spec.SourceTable, err)
		}
	}

	if err := tracker.markReadDone(job.spec.SourceSchema, job.spec.SourceTable); err != nil {
		return err
	}

	if err := commitSnapshots(ctx, snaps); err != nil {
		return fmt.Errorf("commit snapshot for %s.%s: %w", job.spec.SourceSchema, job.spec.SourceTable, err)
	}
	committed = true
	return nil
}

func openTableSnapshotsWithLimiter(ctx context.Context, db *sql.DB, schema, table, firstCol string, readers int, snapOpt SnapshotOptions, lim *snapshotLimiter, opt Options, stats *Stats) ([]*tableSnapshot, int, error) {
	tryOpen := func(n int) ([]*tableSnapshot, int, error) {
		needLock := n > 1 || snapOpt.CaptureHWM
		reserve := n
		if needLock {
			reserve = n + 1 // 预留协调锁连接，避免自死锁
		}
		if err := lim.acquireConns(ctx, reserve); err != nil {
			return nil, 0, err
		}
		snaps, err := openAlignedTableSnapshots(ctx, db, schema, table, firstCol, n, snapOpt)
		if err != nil {
			lim.releaseConns(reserve)
			return nil, 0, err
		}
		// 锁已释放：归还协调连接槽位，仅保留 N 条快照连接。
		if needLock {
			lim.releaseConns(1)
			reserve = n
		}
		atomic.AddInt64(&stats.ActiveSnapshotTxns, int64(n))
		return snaps, reserve, nil
	}

	snaps, reserved, err := tryOpen(readers)
	if err == nil {
		return snaps, reserved, nil
	}

	// 对齐多连接取锁失败：可降级为单连接（仍保持一致性快照）。CaptureHWM 路径禁止降级。
	if readers > 1 && opt.DegradeOnAlignLockFail && !snapOpt.CaptureHWM {
		logger.Warn("[FullLoadV2] align lock for %s.%s failed (%v); degrade to single-reader snapshot", schema, table, err)
		stats.incSnapshotAlignDegrades()
		snaps, reserved, err2 := tryOpen(1)
		if err2 != nil {
			return nil, 0, fmt.Errorf("open aligned snapshots for %s.%s failed (%v); single-reader degrade also failed: %w", schema, table, err, err2)
		}
		return snaps, reserved, nil
	}
	return nil, 0, err
}

func readChunksParallel(ctx context.Context, snaps []*tableSnapshot, chunks []*Chunk, q *batchQueue, opt Options, stats *Stats, tracker *tableCompletionTracker, isStopped func() bool, cancel context.CancelFunc) error {
	chunkCh := make(chan *Chunk, len(chunks))
	for _, c := range chunks {
		chunkCh <- c
	}
	close(chunkCh)

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

	for i, snap := range snaps {
		wg.Add(1)
		go func(idx int, s *tableSnapshot) {
			defer wg.Done()
			for chunk := range chunkCh {
				if ctx.Err() != nil || (isStopped != nil && isStopped()) {
					cancel()
					return
				}
				if err := readChunk(ctx, s.conn, chunk, q, opt, stats, tracker, isStopped, cancel); err != nil {
					setErr(fmt.Errorf("reader[%d] chunk %s: %w", idx, chunk.ID, err))
					return
				}
			}
		}(i, snap)
	}
	wg.Wait()
	return firstErr
}

func readChunk(ctx context.Context, queryer snapshotQueryer, chunk *Chunk, q *batchQueue, opt Options, stats *Stats, tracker *tableCompletionTracker, isStopped func() bool, cancel context.CancelFunc) error {
	cr, err := newChunkReader(queryer, chunk, opt.BatchRows, opt.BatchBytes)
	if err != nil {
		return err
	}
	defer cr.close()

	for {
		if ctx.Err() != nil || (isStopped != nil && isStopped()) {
			cancel()
			return nil
		}
		start := time.Now()
		batch, err := cr.nextBatch(ctx)
		if err != nil {
			return fmt.Errorf("read chunk %s: %w", chunk.ID, err)
		}
		if batch == nil {
			stats.incChunkDone()
			return nil
		}
		stats.addReadBatch(int64(len(batch.Rows)), batch.ApproxBytes, time.Since(start))

		// 必须先增加 inflight，再 Put：否则 writer 可能先提交并把计数从 0 减到 0，
		// 随后 reader 再 +1，markReadDone 会永远看到 inflight>0，OnTableDataReady 不触发。
		if tracker != nil {
			tracker.onBatchEnqueued(batch.Schema, batch.Table)
		}
		enq := time.Now()
		if err := q.Put(ctx, batch); err != nil {
			if tracker != nil {
				if decErr := tracker.onBatchEnqueueAborted(batch.Schema, batch.Table); decErr != nil {
					return decErr
				}
			}
			return nil
		}
		stats.addEnqueueWait(time.Since(enq))
	}
}
