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

// chunkReader 以 map-free 方式读取单个 chunk，直接扫描成 [][]any。
type chunkReader struct {
	db         *sql.DB
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

func newChunkReader(db *sql.DB, chunk *Chunk, batchRows int, batchBytes int64) (*chunkReader, error) {
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
		db:         db,
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
		rows, err := r.db.QueryContext(ctx, q)
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

	rows, err := r.db.QueryContext(ctx, q, args...)
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

// runReaders 启动读取 worker 池：从 chunkCh 领取 chunk（工作窃取），读取批次投入队列。
func runReaders(ctx context.Context, db *sql.DB, chunkCh <-chan *Chunk, q *batchQueue, opt Options, stats *Stats, isStopped func() bool) error {
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

	for w := 0; w < opt.ReadWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			atomic.AddInt64(&stats.ActiveReaders, 1)
			defer atomic.AddInt64(&stats.ActiveReaders, -1)

			for chunk := range chunkCh {
				if workerCtx.Err() != nil || (isStopped != nil && isStopped()) {
					cancel()
					return
				}
				if err := readChunk(workerCtx, db, chunk, q, opt, stats, isStopped, cancel); err != nil {
					setErr(err)
					return
				}
			}
		}()
	}

	wg.Wait()
	return firstErr
}

func readChunk(ctx context.Context, db *sql.DB, chunk *Chunk, q *batchQueue, opt Options, stats *Stats, isStopped func() bool, cancel context.CancelFunc) error {
	cr, err := newChunkReader(db, chunk, opt.BatchRows, opt.BatchBytes)
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

		enq := time.Now()
		if err := q.Put(ctx, batch); err != nil {
			return nil
		}
		stats.addEnqueueWait(time.Since(enq))
	}
}
