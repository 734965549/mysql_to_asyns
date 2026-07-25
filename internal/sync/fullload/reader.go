package fullload

import (
	"context"
	"database/sql"
	"errors"
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
	opt        Options // 查询超时和慢查询阈值
	attemptID  int     // 表级重试序号（P2.2），填充到每个 RowBatch
	stats      *Stats  // P3.6: 用于查询超时/慢查询计数

	cursorCols []string
	cursorIdx  []int // cursorCols 在 cols 的位置

	stream       *sql.Rows // 仅无主键流式使用
	cursor       []any     // keyset 游标当前值
	done         bool
	slowWarnOnce bool // 标记是否已输出慢查询告警，避免重复刷屏

	// queryCancel/slowCancel 必须存活到 Rows.Close，覆盖整个结果集消费周期。
	queryCancel  context.CancelFunc
	slowCancel   context.CancelFunc
	queryCtx     context.Context // 当前查询超时 ctx；Scan 超时分类前勿取消
	queryPhase   string
	queryTimeout time.Duration
	queryStart   time.Time
	streamWatch  *streamWatch // 仅 stream：无进展/最长时长看门狗
	// streamEnqueueWatched 表示已为当前 queryCtx 注册队列唤醒，避免每批重复起 goroutine。
	streamEnqueueWatched bool
}

// effectiveBatchRows 根据表列类型动态计算实际批次行数，对含大字段表降低单批量，
// 减少单次结果集体积，缓解大 JSON/BLOB 表拖死查询的问题。下限 25，上限不超过 opt.BatchRows。
func effectiveBatchRows(spec *TableSpec, opt Options) int {
	if spec == nil || spec.Identity == nil {
		return opt.BatchRows
	}
	largeCount := 0
	hasLongField := false
	for _, col := range spec.Identity.Columns {
		dt := strings.ToLower(strings.TrimSpace(col.DataType))
		if i := strings.IndexByte(dt, '('); i >= 0 {
			dt = dt[:i]
		}
		dt = strings.TrimSpace(dt)
		switch dt {
		case "longtext", "longblob":
			hasLongField = true
			largeCount++
		case "json", "text", "mediumtext", "blob", "mediumblob":
			largeCount++
		}
	}
	base := opt.BatchRows
	switch {
	case hasLongField:
		base = base / 20
	case largeCount >= 2:
		base = base / 10
	case largeCount == 1:
		base = base / 4
	}
	if base < 25 {
		base = 25
	}
	if base > opt.BatchRows {
		base = opt.BatchRows
	}
	return base
}

func newChunkReader(queryer snapshotQueryer, chunk *Chunk, batchRows int, batchBytes int64, opt Options, attemptID int, stats *Stats) (*chunkReader, error) {
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
		opt:        opt,
		attemptID:  attemptID,
		stats:      stats,
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

// finishQuery 取消查询超时 context 与慢查询监控；必须在 Rows.Close 之后调用。
func (r *chunkReader) finishQuery() {
	if r == nil {
		return
	}
	if r.streamWatch != nil {
		r.streamWatch.stop()
		r.streamWatch = nil
	}
	r.streamEnqueueWatched = false
	if r.slowCancel != nil {
		r.slowCancel()
		r.slowCancel = nil
	}
	if r.queryCancel != nil {
		r.queryCancel()
		r.queryCancel = nil
	}
	r.queryCtx = nil
	r.queryPhase = ""
	r.queryTimeout = 0
	r.queryStart = time.Time{}
}

// classifyScanError 将结果集消费阶段的查询超时包装为可重试的 ReadQueryTimeoutError。
// 须在 finishQuery/closeRows 取消 queryCtx 之前调用；父 ctx 已取消/超时时不包装，避免误重试。
// enqueueContext 返回投递写队列时应监听的 context。
// stream 查询存活期间改用 queryCtx，使 max_duration/空闲超时能唤醒阻塞的 Put。
func (r *chunkReader) enqueueContext(parent context.Context) context.Context {
	if r != nil && r.streamWatch != nil && r.queryCtx != nil {
		return r.queryCtx
	}
	return parent
}

// ensureStreamEnqueueWatch 为当前 stream queryCtx 注册队列唤醒（每查询一次）。
func (r *chunkReader) ensureStreamEnqueueWatch(q *batchQueue) {
	if r == nil || q == nil || r.streamWatch == nil || r.queryCtx == nil || r.streamEnqueueWatched {
		return
	}
	q.watchContext(r.queryCtx)
	r.streamEnqueueWatched = true
}

// watchTimeoutError 在 stream 看门狗已触发时构造 ReadQueryTimeoutError；未触发返回 nil。
func (r *chunkReader) watchTimeoutError() error {
	if r == nil || r.streamWatch == nil || !r.streamWatch.wasFired() {
		return nil
	}
	elapsed := time.Duration(0)
	if !r.queryStart.IsZero() {
		elapsed = time.Since(r.queryStart)
	}
	timeout := r.queryTimeout
	if lim := r.streamWatch.limitOnFire(); lim > 0 {
		timeout = lim
	}
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	phase := r.queryPhase
	if phase == "" {
		phase = "stream"
	}
	if r.stats != nil {
		atomic.AddInt64(&r.stats.QueryTimeouts, 1)
	}
	return &ReadQueryTimeoutError{
		Schema:  r.chunk.Spec.SourceSchema,
		Table:   r.chunk.Spec.SourceTable,
		ChunkID: r.chunk.ID,
		Phase:   phase,
		Cursor:  r.cursor,
		Start:   r.chunk.Start,
		End:     r.chunk.End,
		Timeout: timeout,
		Elapsed: elapsed,
	}
}

func (r *chunkReader) classifyScanError(parentCtx context.Context, err error) error {
	if err == nil || r == nil {
		return err
	}
	watchFired := r.streamWatch != nil && r.streamWatch.wasFired()
	queryTimedOut := watchFired || (r.queryCtx != nil && r.queryCtx.Err() == context.DeadlineExceeded)
	if !queryTimedOut && !errorsIsDeadline(err) {
		return err
	}
	if parentCtx != nil && parentCtx.Err() != nil {
		return err
	}
	elapsed := time.Duration(0)
	if !r.queryStart.IsZero() {
		elapsed = time.Since(r.queryStart)
	}
	timeout := r.queryTimeout
	if watchFired {
		if lim := r.streamWatch.limitOnFire(); lim > 0 {
			timeout = lim
		}
	}
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	phase := r.queryPhase
	if phase == "" {
		phase = "scan"
	}
	if r.stats != nil {
		atomic.AddInt64(&r.stats.QueryTimeouts, 1)
	}
	return &ReadQueryTimeoutError{
		Schema:  r.chunk.Spec.SourceSchema,
		Table:   r.chunk.Spec.SourceTable,
		ChunkID: r.chunk.ID,
		Phase:   phase,
		Cursor:  r.cursor,
		Start:   r.chunk.Start,
		End:     r.chunk.End,
		Timeout: timeout,
		Elapsed: elapsed,
	}
}

// runQuery 执行源端查询，带超时控制和慢查询监控。
// keyset/pk_probe/payload_fetch 使用绝对超时；stream 请走 runStreamQuery。
// 成功返回的 *sql.Rows 对应的超时 context 会挂在 chunkReader 上，直到 closeRows/close 才取消。
// 零值 QueryTimeout/SlowQueryWarnThreshold 会被防护。
func (r *chunkReader) runQuery(ctx context.Context, phase string, query string, args ...any) (*sql.Rows, time.Duration, error) {
	if phase == "stream" {
		return r.runStreamQuery(ctx, query, args...)
	}
	timeout := r.opt.QueryTimeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	slowWarn := r.opt.SlowQueryWarnThreshold
	if slowWarn <= 0 {
		slowWarn = 30 * time.Second
	}

	// 上一次查询若未 finish（异常路径），先清理。
	r.finishQuery()

	queryCtx, queryCancel := context.WithTimeout(ctx, timeout)
	slowCtx, slowCancel := context.WithCancel(ctx)
	queryStart := time.Now()
	r.startSlowQueryWarn(slowCtx, slowWarn, queryStart, phase)

	rows, err := r.queryer.QueryContext(queryCtx, query, args...)
	elapsed := time.Since(queryStart)

	if err != nil {
		slowCancel()
		queryCancel()
		if queryCtx.Err() == context.DeadlineExceeded || errorsIsDeadline(err) {
			if r.stats != nil {
				atomic.AddInt64(&r.stats.QueryTimeouts, 1)
			}
			return nil, elapsed, &ReadQueryTimeoutError{
				Schema:  r.chunk.Spec.SourceSchema,
				Table:   r.chunk.Spec.SourceTable,
				ChunkID: r.chunk.ID,
				Phase:   phase,
				Cursor:  r.cursor,
				Start:   r.chunk.Start,
				End:     r.chunk.End,
				Timeout: timeout,
				Elapsed: elapsed,
			}
		}
		return nil, elapsed, err
	}

	// 成功：超时 context 必须覆盖 Rows.Next/Scan 直至 Close。
	r.queryCancel = queryCancel
	r.slowCancel = slowCancel
	r.queryCtx = queryCtx
	r.queryPhase = phase
	r.queryTimeout = timeout
	r.queryStart = queryStart
	return rows, elapsed, nil
}

// runStreamQuery 打开无主键全表流式查询。
// 打开阶段使用 QueryTimeout；打开成功后改为 StreamIdleTimeout 无进展超时，
// 可选 StreamMaxDuration 作为绝对最长时长（0=不限制）。
func (r *chunkReader) runStreamQuery(ctx context.Context, query string, args ...any) (*sql.Rows, time.Duration, error) {
	openTimeout := r.opt.QueryTimeout
	if openTimeout <= 0 {
		openTimeout = 5 * time.Minute
	}
	idleTimeout := r.opt.StreamIdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = openTimeout
	}
	slowWarn := r.opt.SlowQueryWarnThreshold
	if slowWarn <= 0 {
		slowWarn = 30 * time.Second
	}

	r.finishQuery()

	queryCtx, queryCancel := context.WithCancel(ctx)
	slowCtx, slowCancel := context.WithCancel(ctx)
	queryStart := time.Now()
	watch := newStreamWatch(queryCancel, idleTimeout, r.opt.StreamMaxDuration)
	// 打开阶段超时必须上报 openTimeout，不能走 fire() 默认的 idleTimeout。
	openTimer := time.AfterFunc(openTimeout, func() { watch.fireWith(openTimeout) })
	r.startSlowQueryWarn(slowCtx, slowWarn, queryStart, "stream")

	rows, err := r.queryer.QueryContext(queryCtx, query, args...)
	openTimer.Stop()
	elapsed := time.Since(queryStart)

	failOpenTimeout := func() (*sql.Rows, time.Duration, error) {
		if rows != nil {
			_ = rows.Close()
		}
		limit := openTimeout
		if watch.wasFired() {
			if lim := watch.limitOnFire(); lim > 0 {
				limit = lim
			}
		}
		watch.stop()
		slowCancel()
		queryCancel()
		if r.stats != nil {
			atomic.AddInt64(&r.stats.QueryTimeouts, 1)
		}
		return nil, elapsed, &ReadQueryTimeoutError{
			Schema:  r.chunk.Spec.SourceSchema,
			Table:   r.chunk.Spec.SourceTable,
			ChunkID: r.chunk.ID,
			Phase:   "stream",
			Cursor:  r.cursor,
			Start:   r.chunk.Start,
			End:     r.chunk.End,
			Timeout: limit,
			Elapsed: elapsed,
		}
	}

	if err != nil {
		if watch.wasFired() || errorsIsDeadline(err) || queryCtx.Err() != nil {
			if ctx.Err() != nil {
				watch.stop()
				slowCancel()
				queryCancel()
				return nil, elapsed, err
			}
			return failOpenTimeout()
		}
		watch.stop()
		slowCancel()
		queryCancel()
		return nil, elapsed, err
	}
	if watch.wasFired() {
		// 打开刚成功但 open 超时已触发：不要带着已取消 ctx 继续消费。
		return failOpenTimeout()
	}

	// 打开成功：挂上可重置无进展看门狗，覆盖后续 Rows.Next/Scan。
	r.queryCancel = queryCancel
	r.slowCancel = slowCancel
	r.queryCtx = queryCtx
	r.queryPhase = "stream"
	r.queryTimeout = idleTimeout
	r.queryStart = queryStart
	r.streamWatch = watch
	watch.armIdle()
	return rows, elapsed, nil
}

func (r *chunkReader) startSlowQueryWarn(slowCtx context.Context, slowWarn time.Duration, queryStart time.Time, phase string) {
	go func() {
		select {
		case <-time.After(slowWarn):
			if !r.slowWarnOnce {
				r.slowWarnOnce = true
				if r.stats != nil {
					atomic.AddInt64(&r.stats.SlowQueries, 1)
				}
				if phase == "keyset" || phase == "pk_probe" || phase == "payload_fetch" {
					logger.Warn("[FullLoadV2] slow source query: table=%s.%s chunk=%s phase=%s cursor=%v end=%v elapsed=%s batch_rows=%d",
						r.chunk.Spec.SourceSchema, r.chunk.Spec.SourceTable, r.chunk.ID, phase,
						r.cursor, r.chunk.End, time.Since(queryStart), r.batchRows)
				} else {
					logger.Warn("[FullLoadV2] slow source query: table=%s.%s chunk=%s phase=%s elapsed=%s batch_rows=%d",
						r.chunk.Spec.SourceSchema, r.chunk.Spec.SourceTable, r.chunk.ID, phase,
						time.Since(queryStart), r.batchRows)
				}
			}
		case <-slowCtx.Done():
			return
		}
	}()
}

func errorsIsDeadline(err error) bool {
	return errors.Is(err, context.DeadlineExceeded)
}

func (r *chunkReader) closeRows(rows *sql.Rows) {
	if rows != nil {
		_ = rows.Close()
	}
	r.finishQuery()
}

func (r *chunkReader) close() {
	if r.stream != nil {
		_ = r.stream.Close()
		r.stream = nil
	}
	r.finishQuery()
}

// nextBatch 返回下一批；无更多数据时返回 (nil, nil)。
func (r *chunkReader) nextBatch(ctx context.Context) (*RowBatch, error) {
	if r.done {
		return nil, nil
	}
	if r.chunk.NoPK {
		return r.nextStreamBatch(ctx)
	}
	// P1.1：单列 PK + TwoPhaseRead 启用时走两阶段路径（pk_probe + payload_fetch）
	if r.opt.TwoPhaseRead && len(r.cursorCols) == 1 && !r.chunk.Sequential {
		return r.nextTwoPhaseKeysetBatch(ctx)
	}
	return r.nextKeysetBatch(ctx)
}

func (r *chunkReader) nextStreamBatch(ctx context.Context) (*RowBatch, error) {
	if r.stream == nil {
		q := fmt.Sprintf("SELECT %s FROM %s.%s", r.selectSQL,
			quoteIdentifier(r.chunk.Spec.SourceSchema), quoteIdentifier(r.chunk.Spec.SourceTable))

		rows, elapsed, err := r.runStreamQuery(ctx, q)
		if err != nil {
			return nil, err
		}
		r.stream = rows

		logger.Debug("[FullLoadV2] source query started: table=%s.%s chunk=%s phase=stream elapsed=%s",
			r.chunk.Spec.SourceSchema, r.chunk.Spec.SourceTable, r.chunk.ID, elapsed)
	} else if r.streamWatch != nil {
		// 上一批返回后曾 pause（覆盖写队列等待）；继续读取前恢复无进展计时。
		r.streamWatch.resume()
	}
	rowsData, bytes, exhausted, err := scanUpTo(r.stream, len(r.cols), r.batchRows, r.batchBytes, r.streamWatch.noteProgress)
	if err != nil {
		err = r.classifyScanError(ctx, err)
		r.closeRows(r.stream)
		r.stream = nil
		r.done = true
		return nil, err
	}
	if len(rowsData) == 0 {
		r.closeRows(r.stream)
		r.stream = nil
		r.done = true
		return nil, nil
	}
	if exhausted {
		r.closeRows(r.stream)
		r.stream = nil
		r.done = true
	} else if r.streamWatch != nil {
		// 批次未读完：暂停无进展计时，避免写队列背压被算进源端空闲超时。
		r.streamWatch.pause()
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

	rows, elapsed, err := r.runQuery(ctx, "keyset", q, args...)
	if err != nil {
		return nil, err
	}

	logger.Debug("[FullLoadV2] source query completed: table=%s.%s chunk=%s phase=keyset elapsed=%s",
		r.chunk.Spec.SourceSchema, r.chunk.Spec.SourceTable, r.chunk.ID, elapsed)

	rowsData, bytes, exhausted, err := scanUpTo(rows, len(r.cols), r.batchRows, r.batchBytes, nil)
	if err != nil {
		err = r.classifyScanError(ctx, err)
	}
	r.closeRows(rows)
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

// nextTwoPhaseKeysetBatch 两阶段读取：先查主键值（pk_probe），再 IN 查完整行（payload_fetch）。
// 仅在单列 PK 且 TwoPhaseRead=true 时调用；复合 PK/无 PK 降级为标准 keyset/stream。
func (r *chunkReader) nextTwoPhaseKeysetBatch(ctx context.Context) (*RowBatch, error) {
	where, args := r.buildWhere()
	pkCol := r.cursorCols[0]

	// Phase 1: pk_probe - 只查主键列，快速扫过稀疏/删除行
	pkQuery := fmt.Sprintf("SELECT %s FROM %s.%s", quoteIdentifier(pkCol),
		quoteIdentifier(r.chunk.Spec.SourceSchema), quoteIdentifier(r.chunk.Spec.SourceTable))
	if where != "" {
		pkQuery += " WHERE " + where
	}
	pkQuery += " ORDER BY " + quoteIdentifier(pkCol) + " LIMIT ?"
	pkArgs := append(args, r.batchRows)

	pkRows, elapsed, err := r.runQuery(ctx, "pk_probe", pkQuery, pkArgs...)
	if err != nil {
		return nil, err
	}
	logger.Debug("[FullLoadV2] pk_probe completed: table=%s.%s chunk=%s elapsed=%s",
		r.chunk.Spec.SourceSchema, r.chunk.Spec.SourceTable, r.chunk.ID, elapsed)

	// pk_probe 只取主键列，PK 值极小（远低于 batchBytes），maxBytes 传 0 确保取满 batchRows。
	pkValues, _, pkExhausted, err := scanUpTo(pkRows, 1, r.batchRows, 0, nil)
	if err != nil {
		err = r.classifyScanError(ctx, err)
	}
	r.closeRows(pkRows)
	if err != nil {
		return nil, err
	}
	if len(pkValues) == 0 {
		r.done = true
		return nil, nil
	}

	// Phase 2: payload_fetch - 用 IN 一次性获取完整行数据
	placeholders := make([]string, len(pkValues))
	inArgs := make([]any, len(pkValues))
	for i, row := range pkValues {
		placeholders[i] = "?"
		inArgs[i] = row[0]
	}

	payloadQuery := fmt.Sprintf("SELECT %s FROM %s.%s WHERE %s IN (%s) ORDER BY %s",
		r.selectSQL,
		quoteIdentifier(r.chunk.Spec.SourceSchema), quoteIdentifier(r.chunk.Spec.SourceTable),
		quoteIdentifier(pkCol), strings.Join(placeholders, ", "), quoteIdentifier(pkCol))

	payloadRows, elapsed2, err := r.runQuery(ctx, "payload_fetch", payloadQuery, inArgs...)
	if err != nil {
		return nil, err
	}
	logger.Debug("[FullLoadV2] payload_fetch completed: table=%s.%s chunk=%s probe_pks=%d elapsed=%s",
		r.chunk.Spec.SourceSchema, r.chunk.Spec.SourceTable, r.chunk.ID, len(pkValues), elapsed2)

	// payload 可能被 batchBytes 截断（大 JSON）；未截断时 payloadExhausted=true。
	rowsData, bytes, payloadExhausted, err := scanUpTo(payloadRows, len(r.cols), r.batchRows, r.batchBytes, nil)
	if err != nil {
		err = r.classifyScanError(ctx, err)
	}
	r.closeRows(payloadRows)
	if err != nil {
		return nil, err
	}
	if len(rowsData) == 0 {
		r.done = true
		return nil, nil
	}

	// 推进游标到最后一行的主键列值（与 nextKeysetBatch 逻辑一致）。
	// 若 payload 被 batchBytes 截断，游标停在已扫描的最后一行，下一轮 pk_probe 从该 PK 之后继续，
	// 未扫描的 PK 会被重新探测，不丢行。
	lastRow := rowsData[len(rowsData)-1]
	r.cursor = make([]any, 1)
	r.cursor[0] = lastRow[r.cursorIdx[0]]

	// 仅当 payload 已完整扫完且 pk_probe 是尾批（不足 batchRows）时，才判定 chunk 结束。
	if payloadExhausted && pkExhausted {
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
		AttemptID:    r.attemptID,
	}
	if r.opt.StagingEnabled && r.attemptID > 0 {
		b.StagingTable = stagingTableName(r.chunk.Spec.TargetTable, r.attemptID)
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
func scanUpTo(rows *sql.Rows, nCols, maxRows int, maxBytes int64, onProgress func()) ([][]any, int64, bool, error) {
	var out [][]any
	var bytes int64
	for i := 0; i < maxRows; i++ {
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return out, bytes, false, err
			}
			return out, bytes, true, nil
		}
		if onProgress != nil {
			onProgress()
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

	// P2.5: 表级状态跟踪器（用于重试和 inflight barrier）
	var stateTracker *tableStateTracker
	if eng != nil {
		stateTracker = eng.stateTracker
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
				if err := readTableWithRetry(workerCtx, db, job, q, eng, lim, opt, stats, isStopped, cancel, stateTracker); err != nil {
					setErr(err)
					return
				}
				// note: setErr cancels workerCtx (task-level) only after retry exhausted / non-retryable
			}
		}()
	}

	wg.Wait()
	return firstErr
}

type tableReadJob struct {
	spec      *TableSpec
	chunks    []*Chunk
	AttemptID int // P2.2: 表级重试序号，传递给所有 RowBatch
}

// readTableWithSnapshot 在表级一致性快照内读取。
// taskCancel：用户停止/致命错误时取消整个任务流水线。
// attemptCancel：可重试的表内错误只取消当前 attempt（可为 nil，此时退化为 taskCancel）。
func readTableWithSnapshot(ctx context.Context, db *sql.DB, job *tableReadJob, q *batchQueue, eng *Engine, lim *snapshotLimiter, opt Options, stats *Stats, isStopped func() bool, taskCancel context.CancelFunc, attemptCancel context.CancelFunc) error {
	if job == nil || job.spec == nil || job.spec.Identity == nil || len(job.spec.Identity.Columns) == 0 {
		return fmt.Errorf("invalid table read job")
	}
	if attemptCancel == nil {
		attemptCancel = taskCancel
	}

	lease, err := lim.acquireGroup(ctx)
	if err != nil {
		return err
	}
	defer lease.release()

	readers := decideTableReadersForSpec(job.spec, opt)
	// captureHWM 条件: 无PK/UK 表必须捕获 HWM(ALL 模式增量依赖);
	// 启用表级重试时所有表也捕获 HWM，确保每次新 attempt 的快照位点能原子覆盖旧值。
	captureHWM := eng != nil && eng.CaptureTableHWM && (isNoPKSpec(job.spec) || opt.ReadRetryTimes > 0)
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
	var stateTracker *tableStateTracker
	if eng != nil {
		tracker = eng.tracker
		stateTracker = eng.stateTracker
	}

	if len(chunks) == 0 {
		if err := tracker.markReadDone(job.spec.SourceSchema, job.spec.SourceTable); err != nil {
			return err
		}
	} else if len(snaps) == 1 {
		for _, chunk := range chunks {
			if isStopped != nil && isStopped() {
				if taskCancel != nil {
					taskCancel()
				}
				return nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := readChunk(ctx, snaps[0].conn, chunk, q, opt, stats, tracker, stateTracker, isStopped, taskCancel, job.AttemptID); err != nil {
				return fmt.Errorf("read table %s.%s chunk %s: %w", job.spec.SourceSchema, job.spec.SourceTable, chunk.ID, err)
			}
		}
	} else {
		active := snaps
		if len(chunks) < len(snaps) {
			active = snaps[:len(chunks)]
		}
		if err := readChunksParallel(ctx, active, chunks, q, opt, stats, tracker, stateTracker, isStopped, taskCancel, attemptCancel, job.AttemptID); err != nil {
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

func readChunksParallel(ctx context.Context, snaps []*tableSnapshot, chunks []*Chunk, q *batchQueue, opt Options, stats *Stats, tracker *tableCompletionTracker, stateTracker *tableStateTracker, isStopped func() bool, taskCancel, attemptCancel context.CancelFunc, attemptID int) error {
	chunkCh := make(chan *Chunk, len(chunks))
	for _, c := range chunks {
		chunkCh <- c
	}
	close(chunkCh)

	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once
	// 表内并行错误只取消 attemptCtx，保留任务级 ctx 以便表级重试。
	setErr := func(err error) {
		if err != nil {
			errOnce.Do(func() {
				firstErr = err
				if attemptCancel != nil {
					attemptCancel()
				}
			})
		}
	}

	for i, snap := range snaps {
		wg.Add(1)
		go func(idx int, s *tableSnapshot) {
			defer wg.Done()
			for chunk := range chunkCh {
				if isStopped != nil && isStopped() {
					if taskCancel != nil {
						taskCancel()
					}
					return
				}
				if ctx.Err() != nil {
					return
				}
				if err := readChunk(ctx, s.conn, chunk, q, opt, stats, tracker, stateTracker, isStopped, taskCancel, attemptID); err != nil {
					setErr(fmt.Errorf("reader[%d] chunk %s: %w", idx, chunk.ID, err))
					return
				}
			}
		}(i, snap)
	}
	wg.Wait()
	return firstErr
}

func readChunk(ctx context.Context, queryer snapshotQueryer, chunk *Chunk, q *batchQueue, opt Options, stats *Stats, tracker *tableCompletionTracker, stateTracker *tableStateTracker, isStopped func() bool, taskCancel context.CancelFunc, attemptID int) error {
	batchRows := effectiveBatchRows(chunk.Spec, opt)
	cr, err := newChunkReader(queryer, chunk, batchRows, opt.BatchBytes, opt, attemptID, stats)
	if err != nil {
		return err
	}
	defer cr.close()

	for {
		if isStopped != nil && isStopped() {
			if taskCancel != nil {
				taskCancel()
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
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

		// completion tracker：先 +1 再 Put，避免 writer 先提交导致 OnTableDataReady 假死。
		if tracker != nil {
			tracker.onBatchEnqueued(batch.Schema, batch.Table)
		}
		// stateTracker：在 Put 前预增，使队列中批次对 inflight barrier 可见。
		if stateTracker != nil && batch.AttemptID > 0 {
			if eErr := stateTracker.onBatchEnqueued(batch.Schema, batch.Table, batch.AttemptID); eErr != nil {
				if tracker != nil {
					_ = tracker.onBatchEnqueueAborted(batch.Schema, batch.Table)
				}
				return eErr
			}
		}
		cr.ensureStreamEnqueueWatch(q)
		enqCtx := cr.enqueueContext(ctx)
		enq := time.Now()
		if err := q.Put(enqCtx, batch); err != nil {
			if tracker != nil {
				if decErr := tracker.onBatchEnqueueAborted(batch.Schema, batch.Table); decErr != nil {
					return decErr
				}
			}
			if stateTracker != nil && batch.AttemptID > 0 {
				stateTracker.onBatchReleased(batch.Schema, batch.Table, batch.AttemptID)
			}
			// stream 看门狗（含 max_duration）在队列背压期间触发：必须返回超时错误，不能当成功退出。
			if to := cr.watchTimeoutError(); to != nil {
				return fmt.Errorf("read chunk %s: %w", chunk.ID, to)
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return nil
		}
		stats.addEnqueueWait(time.Since(enq))
	}
}
