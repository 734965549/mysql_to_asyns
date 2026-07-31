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

	"mysql-to-sync/internal/metadata/domain/entity"
	"mysql-to-sync/pkg/logger"
)

// snapshotQueryer 是 chunk 读取使用的查询接口；可绑定单连接（如 db.Conn）或整个连接池（*sql.DB）。
type snapshotQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// isNoPKSpec 判断表是否无 PK/UK（走全列匹配策略）。
func isNoPKSpec(spec *TableSpec) bool {
	return spec != nil && spec.Identity != nil && spec.Identity.Strategy == entity.FullColumnsStrategy
}

// decideTableReadersForSpec 在规划 chunk 之前，仅根据估算行数决定单表内并行读连接数。
// 无 PK 表始终 1；明确中小表 1；超大表或估算未知时用 TableParallelReaders。
func decideTableReadersForSpec(spec *TableSpec, opt Options) int {
	if spec == nil || isNoPKSpec(spec) {
		return 1
	}
	if opt.LargeTableRows > 0 && spec.EstimatedRows > 0 && spec.EstimatedRows < opt.LargeTableRows {
		return 1
	}
	n := opt.TableParallelReaders
	if n < 1 {
		n = 1
	}
	return n
}

// decideTableReaders 在已有 chunk 时收紧并行度（不超过 chunk 数）。
func decideTableReaders(job *tableReadJob, opt Options) int {
	n := decideTableReadersForSpec(job.spec, opt)
	if job == nil || len(job.chunks) <= 1 {
		return 1
	}
	if n > len(job.chunks) {
		n = len(job.chunks)
	}
	return n
}

// chunkReader 以 map-free 方式读取单个 chunk，直接扫描成 [][]any。
type chunkReader struct {
	queryer      snapshotQueryer
	chunk        *Chunk
	cols         []string // 固定列顺序（含生成列，供 SELECT / 游标索引）
	writableCols []string // 可写入列顺序（RowBatch 输出）
	writableIdx  []int    // writableCols 在 cols 的下标
	selectSQL    string   // "`c1`, `c2`, ..."
	batchRows    int
	batchBytes   int64
	opt          Options // 查询超时和慢查询阈值
	attemptID    int     // 表级重试序号（P2.2），填充到每个 RowBatch
	stats        *Stats  // P3.6: 用于查询超时/慢查询计数
	sink         EventSink

	cursorCols []string
	cursorIdx  []int // cursorCols 在 cols 的位置

	stream            *sql.Rows // 仅无主键流式使用
	cursor            []any     // keyset 游标当前值
	done              bool
	slowWarnOnce      bool // 标记是否已输出慢查询告警，避免重复刷屏
	twoPhaseWide      bool // 宽表自动两阶段读（每 chunkReader 仅 emit 一次事件）
	rowExceedWarnOnce bool // 超大单行 WARN 事件节流

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

// shouldUseTwoPhaseRead 判定是否走两阶段读：单列 PK +（显式开启或宽表自动启用）。
// full_load_two_phase_read=true 强制开启；false（默认）时宽表（含 JSON/BLOB/TEXT）自动启用。
func shouldUseTwoPhaseRead(spec *TableSpec, chunk *Chunk, opt Options) bool {
	if chunk == nil || chunk.NoPK || chunk.Sequential {
		return false
	}
	if spec == nil || spec.Identity == nil {
		return false
	}
	if len(spec.Identity.EffectiveCursorCols()) != 1 {
		return false
	}
	if opt.TwoPhaseRead {
		return true
	}
	return hasLargeColumnTypes(spec)
}

func (r *chunkReader) maybeEmitWideTableTwoPhase() {
	if r == nil || r.twoPhaseWide || r.sink == nil || r.chunk == nil || r.chunk.Spec == nil {
		return
	}
	if !hasLargeColumnTypes(r.chunk.Spec) {
		return
	}
	r.twoPhaseWide = true
	tableEvent(r.sink, r.chunk.Spec.SourceSchema, r.chunk.Spec.SourceTable,
		EventCodeWideTableTwoPhaseEnabled, EventCategoryTable, EventSeverityInfo,
		"wide table two-phase read auto-enabled (pk_probe + payload_fetch)",
		map[string]interface{}{
			"chunk_id":    r.chunk.ID,
			"batch_rows":  r.batchRows,
			"batch_bytes": r.batchBytes,
		})
}

func (r *chunkReader) maybeEmitRowExceedsBatchBytes(rowBytes int64) {
	if r == nil || r.rowExceedWarnOnce || r.sink == nil || r.batchBytes <= 0 || rowBytes <= r.batchBytes {
		return
	}
	if r.chunk == nil || r.chunk.Spec == nil {
		return
	}
	r.rowExceedWarnOnce = true
	tableEvent(r.sink, r.chunk.Spec.SourceSchema, r.chunk.Spec.SourceTable,
		EventCodeRowExceedsBatchBytes, EventCategoryTable, EventSeverityWarn,
		fmt.Sprintf("single row approx %d bytes exceeds batch_bytes limit %d", rowBytes, r.batchBytes),
		map[string]interface{}{
			"chunk_id":    r.chunk.ID,
			"row_bytes":   rowBytes,
			"batch_bytes": r.batchBytes,
		})
}

func newChunkReader(queryer snapshotQueryer, chunk *Chunk, batchRows int, batchBytes int64, opt Options, attemptID int, stats *Stats, sink EventSink) (*chunkReader, error) {
	id := chunk.Spec.Identity
	if id == nil || len(id.Columns) == 0 {
		return nil, fmt.Errorf("chunk %s has no table columns", chunk.ID)
	}
	cols := make([]string, 0, len(id.Columns))
	writableCols := make([]string, 0, len(id.Columns))
	writableIdx := make([]int, 0, len(id.Columns))
	for i, c := range id.Columns {
		cols = append(cols, c.Name)
		if c.IsWritable() {
			writableCols = append(writableCols, c.Name)
			writableIdx = append(writableIdx, i)
		}
	}
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = quoteIdentifier(c)
	}

	cr := &chunkReader{
		queryer:      queryer,
		chunk:        chunk,
		cols:         cols,
		writableCols: writableCols,
		writableIdx:  writableIdx,
		selectSQL:    strings.Join(parts, ", "),
		batchRows:    batchRows,
		batchBytes:   batchBytes,
		opt:          opt,
		attemptID:    attemptID,
		stats:        stats,
		sink:         sink,
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
				elapsed := time.Since(queryStart)
				tableEvent(r.sink, r.chunk.Spec.SourceSchema, r.chunk.Spec.SourceTable,
					EventCodeSlowSourceQuery, EventCategoryTable, EventSeverityWarn,
					fmt.Sprintf("slow source query phase=%s elapsed=%s batch_rows=%d", phase, elapsed, r.batchRows),
					map[string]interface{}{
						"chunk_id":   r.chunk.ID,
						"phase":      phase,
						"elapsed_ms": elapsed.Milliseconds(),
						"batch_rows": r.batchRows,
					})
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
	if shouldUseTwoPhaseRead(r.chunk.Spec, r.chunk, r.opt) && len(r.cursorCols) == 1 {
		r.maybeEmitWideTableTwoPhase()
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
	rowsData, bytes, exhausted, err := scanUpTo(r.stream, len(r.cols), r.batchRows, r.batchBytes, r.streamWatch.noteProgress, r.noteRowBytes)
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

	rowsData, bytes, exhausted, err := scanUpTo(rows, len(r.cols), r.batchRows, r.batchBytes, nil, r.noteRowBytes)
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
	pkValues, _, pkExhausted, err := scanUpTo(pkRows, 1, r.batchRows, 0, nil, nil)
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
	rowsData, bytes, payloadExhausted, err := scanUpTo(payloadRows, len(r.cols), r.batchRows, r.batchBytes, nil, r.noteRowBytes)
	if err != nil {
		err = r.classifyScanError(ctx, err)
	}
	r.closeRows(payloadRows)
	if err != nil {
		return nil, err
	}
	if len(rowsData) == 0 {
		// probe 到的键在 payload 查询前已全部删除/变更：跳过这批键继续扫描。
		lastProbe := pkValues[len(pkValues)-1][0]
		r.cursor = make([]any, 1)
		r.cursor[0] = lastProbe
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
	outRows := rowsData
	outCols := r.writableCols
	if len(r.writableIdx) > 0 && len(r.writableIdx) != len(r.cols) {
		outRows = make([][]any, len(rowsData))
		for i, row := range rowsData {
			filtered := make([]any, len(r.writableIdx))
			for j, idx := range r.writableIdx {
				filtered[j] = row[idx]
			}
			outRows[i] = filtered
		}
	} else if len(r.writableIdx) == 0 && len(rowsData) > 0 {
		outRows = make([][]any, len(rowsData))
		for i := range rowsData {
			outRows[i] = []any{}
		}
	}
	b := &RowBatch{
		Schema:       r.chunk.Spec.SourceSchema,
		Table:        r.chunk.Spec.SourceTable,
		TargetSchema: r.chunk.Spec.TargetSchema,
		TargetTable:  r.chunk.Spec.TargetTable,
		Columns:      outCols,
		Rows:         outRows,
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

// scanUpTo 从结果集中扫描至多 maxRows 行到 [][]any，并按 maxBytes 拆批。
// onRowBytes 可选，用于超大单行告警（每 reader 一次）。
func scanUpTo(rows *sql.Rows, nCols, maxRows int, maxBytes int64, onProgress func(), onRowBytes func(int64)) ([][]any, int64, bool, error) {
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
		var rowBytes int64
		for j := range vals {
			if b, ok := vals[j].([]byte); ok {
				cp := make([]byte, len(b))
				copy(cp, b)
				vals[j] = cp
			}
			rowBytes += estimateValueBytes(vals[j])
		}
		if onRowBytes != nil && maxBytes > 0 && rowBytes > maxBytes {
			onRowBytes(rowBytes)
		}
		out = append(out, vals)
		bytes += rowBytes
		if maxBytes > 0 && bytes >= maxBytes {
			break
		}
	}
	return out, bytes, false, nil
}

func (r *chunkReader) noteRowBytes(rowBytes int64) {
	r.maybeEmitRowExceedsBatchBytes(rowBytes)
}

// runTableReaders 用全局读取预算 + 公平 chunk 调度并行读取多张表。
func runTableReaders(ctx context.Context, db *sql.DB, jobs []*tableReadJob, q *batchQueue, eng *Engine, opt Options, stats *Stats, isStopped func() bool) error {
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	q.watchContext(workerCtx)

	var stateTracker *tableStateTracker
	var tracker *tableCompletionTracker
	var sink EventSink
	if eng != nil {
		stateTracker = eng.stateTracker
		tracker = eng.tracker
		sink = eng.EventSink
	}

	coord := newReadCoordinator(workerCtx, db, q, opt, stats, tracker, stateTracker, sink, isStopped, cancel)
	coord.startWorkers(opt.GlobalReadBudget)

	tableSem := make(chan struct{}, opt.TableWorkers)

	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	setErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		firstErr = preferReaderError(firstErr, err)
		errMu.Unlock()
		cancel()
	}

	for _, job := range jobs {
		wg.Add(1)
		go func(job *tableReadJob) {
			defer wg.Done()
			select {
			case tableSem <- struct{}{}:
				defer func() { <-tableSem }()
			case <-workerCtx.Done():
				return
			}
			if workerCtx.Err() != nil || (isStopped != nil && isStopped()) {
				cancel()
				return
			}
			if err := readTableWithRetry(workerCtx, db, job, q, eng, opt, stats, isStopped, cancel, stateTracker, coord); err != nil {
				setErr(err)
			}
		}(job)
	}

	wg.Wait()
	if err := coord.finish(); err != nil {
		setErr(err)
	}
	errMu.Lock()
	defer errMu.Unlock()
	return firstErr
}

func preferReaderError(current, candidate error) error {
	if candidate == nil {
		return current
	}
	if current == nil || (isCancelError(current) && !isCancelError(candidate)) {
		return candidate
	}
	return current
}

type tableReadJob struct {
	spec      *TableSpec
	chunks    []*Chunk
	AttemptID int // P2.2: 表级重试序号，传递给所有 RowBatch
}

// readTable 按普通连接池短查询读取整张表。
// taskCancel：用户停止/致命错误时取消整个任务流水线。
// attemptCancel：可重试的表内错误只取消当前 attempt（可为 nil，此时退化为 taskCancel）。
func readTable(ctx context.Context, db *sql.DB, job *tableReadJob, q *batchQueue, eng *Engine, opt Options, stats *Stats, isStopped func() bool, taskCancel context.CancelFunc, attemptCancel context.CancelFunc, coord *readCoordinator) error {
	return readTablePlain(ctx, db, job, q, eng, opt, stats, isStopped, taskCancel, attemptCancel, coord)
}

// readTablePlain 无表级快照读取：普通连接规划 chunk，短查询并行读。
// 同表不同 chunk 允许看到 T1/T2/T3；失败不得降级改变用户配置的并发度。
func readTablePlain(ctx context.Context, db *sql.DB, job *tableReadJob, q *batchQueue, eng *Engine, opt Options, stats *Stats, isStopped func() bool, taskCancel context.CancelFunc, attemptCancel context.CancelFunc, coord *readCoordinator) error {
	if job == nil || job.spec == nil || job.spec.Identity == nil || len(job.spec.Identity.Columns) == 0 {
		return fmt.Errorf("invalid table read job")
	}
	if attemptCancel == nil {
		attemptCancel = taskCancel
	}
	if db == nil {
		return fmt.Errorf("full read failed: table=%s.%s stage=connect cause=nil source db", job.spec.SourceSchema, job.spec.SourceTable)
	}

	schema := job.spec.SourceSchema
	table := job.spec.SourceTable
	readers := decideTableReadersForSpec(job.spec, opt)
	strategy := string(job.spec.Identity.Strategy)

	targetChunks := opt.GlobalReadBudget * opt.ChunkOvershoot
	if targetChunks < 1 {
		targetChunks = 1
	}

	if err := coord.acquirePlanBudget(ctx, schema, table, readers); err != nil {
		return fmt.Errorf("full read failed: table=%s.%s strategy=%s stage=plan readers=%d cause=%w", schema, table, strategy, readers, err)
	}

	planConn, err := db.Conn(ctx)
	if err != nil {
		coord.releasePlanBudget(schema, table)
		return fmt.Errorf("full read failed: table=%s.%s strategy=%s stage=connect readers=%d cause=%w", schema, table, strategy, readers, err)
	}
	planner := NewPlannerWithSink(planConn, nil)
	var eventSink EventSink
	if eng != nil {
		eventSink = eng.EventSink
		planner.sink = eventSink
	}
	chunks, planErr := planner.planTable(ctx, job.spec, targetChunks)
	_ = planConn.Close()
	coord.releasePlanBudget(schema, table)
	if planErr != nil {
		return fmt.Errorf("full read failed: table=%s.%s strategy=%s stage=plan readers=%d cause=%w", schema, table, strategy, readers, planErr)
	}
	job.chunks = chunks
	atomic.AddInt64(&stats.ChunksTotal, int64(len(chunks)))
	logger.Info("[FullLoadV2] planned %d chunk(s) plain for %s.%s (readers=%d)", len(chunks), schema, table, readers)

	desiredReaders := decideTableReadersForSpec(job.spec, opt)
	effectiveReaders := decideTableReaders(job, opt)
	readers = effectiveReaders
	if isNoPKSpec(job.spec) {
		tableEvent(eventSink, schema, table, EventCodeNOPKSequentialFallback, EventCategoryTable,
			EventSeverityWarn,
			"no PK/UK table uses single-worker streaming read + INSERT IGNORE (best-effort idempotency)",
			map[string]interface{}{"strategy": strategy, "readers": 1})
	} else if effectiveReaders < desiredReaders {
		tableEvent(eventSink, schema, table, EventCodeTableParallelismReduced, EventCategoryTable,
			EventSeverityInfo,
			fmt.Sprintf("table parallel readers reduced %d->%d (chunks=%d)", desiredReaders, effectiveReaders, len(chunks)),
			map[string]interface{}{
				"desired_readers":   desiredReaders,
				"effective_readers": effectiveReaders,
				"chunk_count":       len(chunks),
			})
	}
	tableEvent(eventSink, schema, table, EventCodeTablePlanCreated, EventCategoryTable,
		EventSeverityInfo,
		fmt.Sprintf("chunk plan ready: chunks=%d readers=%d strategy=%s", len(chunks), effectiveReaders, strategy),
		map[string]interface{}{
			"chunk_count":       len(chunks),
			"effective_readers": effectiveReaders,
			"strategy":          strategy,
			"estimated_rows":    job.spec.EstimatedRows,
		})

	tracker := (*tableCompletionTracker)(nil)
	if eng != nil {
		tracker = eng.tracker
	}

	if len(chunks) == 0 {
		if err := tracker.markReadDone(schema, table); err != nil {
			return err
		}
		return nil
	}

	if coord == nil {
		return fmt.Errorf("full read failed: table=%s.%s internal=nil read coordinator", schema, table)
	}
	return coord.submitTable(ctx, schema, table, chunks, job.AttemptID, attemptCancel)
}

// readChunksParallelPlain 用池连接并行读 chunk，不开一致性快照事务。
func readChunksParallelPlain(ctx context.Context, db *sql.DB, readers int, chunks []*Chunk, q *batchQueue, opt Options, stats *Stats, tracker *tableCompletionTracker, stateTracker *tableStateTracker, isStopped func() bool, taskCancel, attemptCancel context.CancelFunc, attemptID int, sink EventSink) error {
	if readers < 1 {
		readers = 1
	}
	if readers > len(chunks) {
		readers = len(chunks)
	}

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
				if attemptCancel != nil {
					attemptCancel()
				}
			})
		}
	}

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(idx int) {
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
				// 每个短查询通过连接池获取连接，坏连接可由 database/sql 丢弃并替换。
				if err := readChunk(ctx, db, chunk, q, opt, stats, tracker, stateTracker, isStopped, taskCancel, attemptID, sink); err != nil {
					setErr(fmt.Errorf("reader[%d] chunk %s: %w", idx, chunk.ID, err))
					return
				}
			}
		}(i)
	}
	wg.Wait()
	return firstErr
}

const maxKeysetBatchRetries = 3

// keysetBatchRetryBackoff 仅用于未成功产出当前批次时的瞬时错误重试。
// 时间保持较短，因为 database/sql 会在下一次查询时直接换掉坏连接。
func keysetBatchRetryBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	wait := 100 * time.Millisecond << (attempt - 1)
	if wait > 2*time.Second {
		wait = 2 * time.Second
	}
	return wait
}

func readChunk(ctx context.Context, queryer snapshotQueryer, chunk *Chunk, q *batchQueue, opt Options, stats *Stats, tracker *tableCompletionTracker, stateTracker *tableStateTracker, isStopped func() bool, taskCancel context.CancelFunc, attemptID int, sink EventSink) error {
	batchRows := logicalWindowRows(opt)
	cr, err := newChunkReader(queryer, chunk, batchRows, opt.BatchBytes, opt, attemptID, stats, sink)
	if err != nil {
		return err
	}
	defer cr.close()

	transientRetries := 0
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
			// PK/UK keyset 的游标只在一批完整扫描成功后推进。当前批次失败时尚未入队，
			// 因而可在连接池换新连接后从原游标安全重试，不会重放已提交批次。
			// 无主键 stream 已经持续推进结果集，不能用此路径重开查询，否则可能重复行。
			if !chunk.NoPK && ctx.Err() == nil && isRetryableReadError(err) && transientRetries < maxKeysetBatchRetries {
				transientRetries++
				cr.finishQuery()
				backoff := keysetBatchRetryBackoff(transientRetries)
				logger.Warn("[FullLoadV2] retry keyset batch after transient read error: table=%s.%s chunk=%s attempt=%d/%d backoff=%s error=%v",
					chunk.Spec.SourceSchema, chunk.Spec.SourceTable, chunk.ID, transientRetries, maxKeysetBatchRetries, backoff, err)
				retryEvent(sink, chunk.Spec.SourceSchema, chunk.Spec.SourceTable, EventCodeTableReadBatchRetry,
					fmt.Sprintf("keyset batch retry %d/%d backoff=%s", transientRetries, maxKeysetBatchRetries, backoff),
					EventSeverityWarn,
					map[string]interface{}{
						"chunk_id": chunk.ID,
						"attempt":  transientRetries,
						"max":      maxKeysetBatchRetries,
						"backoff":  backoff.String(),
						"error":    err.Error(),
					})
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoff):
				}
				continue
			}
			return fmt.Errorf("read chunk %s: %w", chunk.ID, err)
		}
		transientRetries = 0
		if batch == nil {
			stats.incChunkDone()
			return nil
		}
		stats.addReadBatchForTable(batch.Schema, batch.Table, int64(len(batch.Rows)), batch.ApproxBytes, time.Since(start))

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
			// 父 context 取消优先于 watch 超时，与 classifyScanError 一致。
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// stream 看门狗（含 max_duration）在队列背压期间触发：必须返回超时错误，不能当成功退出。
			if to := cr.watchTimeoutError(); to != nil {
				return fmt.Errorf("read chunk %s: %w", chunk.ID, to)
			}
			return nil
		}
		stats.addEnqueueWait(time.Since(enq))
	}
}
