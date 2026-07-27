package fullload

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"mysql-to-sync/internal/metadata/domain/entity"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestChunkReaderCompositeBoundsAndBatchKeys(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	spec := &TableSpec{
		SourceSchema: "s", SourceTable: "t", TargetSchema: "d", TargetTable: "u",
		Identity: &entity.TableIdentity{
			Strategy: entity.PKStrategy, IdentifyCols: []string{"a", "b"}, CursorCols: []string{"a", "b"},
			Columns: []entity.ColumnMeta{{Name: "a"}, {Name: "b"}, {Name: "payload"}},
		},
	}
	chunk := &Chunk{ID: "c1", Spec: spec, Start: []any{1, 10}, End: []any{2, 20}}
	mock.ExpectQuery("SELECT `a`, `b`, `payload` FROM `s`.`t` WHERE").
		WithArgs(1, 10, 1, 2, 2, 20, 2).
		WillReturnRows(sqlmock.NewRows([]string{"a", "b", "payload"}).
			AddRow(1, 11, "x").AddRow(2, 20, "y"))
	cr, err := newChunkReader(db, chunk, 2, defaultBatchBytes, Options{}, 1, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cr.close()
	batch, err := cr.nextBatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Rows) != 2 || batch.StartKey[0] != int64(1) || batch.StartKey[1] != int64(11) ||
		batch.EndKey[0] != int64(2) || batch.EndKey[1] != int64(20) {
		t.Fatalf("unexpected batch keys/rows: %+v", batch)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestChunkReaderCloseClosesNoPKStream(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	spec := &TableSpec{
		SourceSchema: "s`1", SourceTable: "t`1", TargetSchema: "d", TargetTable: "u",
		Identity: &entity.TableIdentity{
			Strategy: entity.FullColumnsStrategy,
			Columns:  []entity.ColumnMeta{{Name: "c`1"}},
		},
	}
	rows := sqlmock.NewRows([]string{"c`1"}).AddRow("a").AddRow("b")
	mock.ExpectQuery("SELECT `c``1` FROM `s``1`.`t``1`").WillReturnRows(rows).RowsWillBeClosed()
	cr, err := newChunkReader(db, &Chunk{ID: "c1", Spec: spec, NoPK: true}, 1, defaultBatchBytes, Options{}, 1, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := cr.nextBatch(context.Background())
	if err != nil || len(batch.Rows) != 1 {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
	cr.close()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestNewChunkReaderRejectsMissingCursorColumn(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	spec := &TableSpec{Identity: &entity.TableIdentity{
		Strategy: entity.PKStrategy, CursorCols: []string{"missing"}, Columns: []entity.ColumnMeta{{Name: "id"}},
	}}
	if _, err := newChunkReader(db, &Chunk{ID: "bad", Spec: spec}, 10, defaultBatchBytes, Options{}, 1, nil, nil); err == nil {
		t.Fatal("expected missing cursor column error")
	}
}

func TestChunkReaderSplitsWideRowsByBytesWithoutLosingStreamRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	spec := &TableSpec{SourceSchema: "s", SourceTable: "t", Identity: &entity.TableIdentity{
		Strategy: entity.FullColumnsStrategy,
		Columns:  []entity.ColumnMeta{{Name: "payload"}},
	}}
	mock.ExpectQuery("SELECT `payload` FROM `s`.`t`").WillReturnRows(
		sqlmock.NewRows([]string{"payload"}).AddRow("12345678").AddRow("abcdefgh").AddRow("ABCDEFGH"))
	cr, err := newChunkReader(db, &Chunk{ID: "wide", Spec: spec, NoPK: true}, 10, 10, Options{}, 1, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cr.close()
	var got []string
	for {
		batch, err := cr.nextBatch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if batch == nil {
			break
		}
		if len(batch.Rows) != 1 || batch.ApproxBytes < 10 {
			t.Fatalf("batch must respect byte target with one-row overshoot: %+v", batch)
		}
		value := batch.Rows[0][0]
		if b, ok := value.([]byte); ok {
			got = append(got, string(b))
		} else {
			got = append(got, value.(string))
		}
	}
	if len(got) != 3 || got[0] != "12345678" || got[2] != "ABCDEFGH" {
		t.Fatalf("stream rows lost or reordered: %v", got)
	}
}

// TestChunkReaderKeysetQueryTimeoutReturnsStructuredError 验证 keyset 查询超时返回 ReadQueryTimeoutError，
// 且错误包含表、chunk、cursor/end 上下文。
func TestChunkReaderKeysetQueryTimeoutReturnsStructuredError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	spec := &TableSpec{
		SourceSchema: "s", SourceTable: "t", TargetSchema: "d", TargetTable: "u",
		Identity: &entity.TableIdentity{
			Strategy: entity.PKStrategy, IdentifyCols: []string{"id"}, CursorCols: []string{"id"},
			Columns: []entity.ColumnMeta{{Name: "id"}, {Name: "payload"}},
		},
	}
	chunk := &Chunk{ID: "c1", Spec: spec, Start: []any{100}, End: []any{200}}
	mock.ExpectQuery("SELECT `id`, `payload` FROM `s`.`t` WHERE").
		WillDelayFor(200 * time.Millisecond).
		WillReturnRows(sqlmock.NewRows([]string{"id", "payload"}).AddRow(101, "x"))

	opt := Options{QueryTimeout: 20 * time.Millisecond, SlowQueryWarnThreshold: time.Hour}
	cr, err := newChunkReader(db, chunk, 10, defaultBatchBytes, opt, 1, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cr.close()

	_, err = cr.nextBatch(context.Background())
	if err == nil {
		t.Fatal("expected query timeout error, got nil")
	}
	if !IsReadQueryTimeout(err) {
		t.Fatalf("expected ReadQueryTimeoutError, got %T: %v", err, err)
	}
	te := err.(*ReadQueryTimeoutError)
	if te.Schema != "s" || te.Table != "t" || te.ChunkID != "c1" || te.Phase != "keyset" {
		t.Fatalf("timeout error missing context: %+v", te)
	}
	if len(te.End) != 1 || te.End[0] != 200 {
		t.Fatalf("timeout error missing end bound: %+v", te)
	}
}

// TestChunkReaderStreamQueryTimeoutReturnsStructuredError 验证无主键流式查询超时同样返回结构化错误。
func TestChunkReaderStreamQueryTimeoutReturnsStructuredError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	spec := &TableSpec{SourceSchema: "s", SourceTable: "t", Identity: &entity.TableIdentity{
		Strategy: entity.FullColumnsStrategy,
		Columns:  []entity.ColumnMeta{{Name: "payload"}},
	}}
	mock.ExpectQuery("SELECT `payload` FROM `s`.`t`").
		WillDelayFor(200 * time.Millisecond).
		WillReturnRows(sqlmock.NewRows([]string{"payload"}).AddRow("x"))

	// stream 打开阶段仍受 QueryTimeout 约束（非整表绝对超时）。
	opt := Options{
		QueryTimeout:           20 * time.Millisecond,
		StreamIdleTimeout:      time.Hour,
		SlowQueryWarnThreshold: time.Hour,
	}
	cr, err := newChunkReader(db, &Chunk{ID: "c1", Spec: spec, NoPK: true}, 10, defaultBatchBytes, opt, 1, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cr.close()

	_, err = cr.nextBatch(context.Background())
	if !IsReadQueryTimeout(err) {
		t.Fatalf("expected ReadQueryTimeoutError, got %T: %v", err, err)
	}
	if te := err.(*ReadQueryTimeoutError); te.Phase != "stream" {
		t.Fatalf("expected phase=stream, got %+v", te)
	}
}

// TestChunkReaderStreamMaxDurationDuringOpenReportsActualLimit 验证打开阶段被
// StreamMaxDuration 取消时，错误中的 Timeout 为 max duration 而非 openTimeout。
func TestChunkReaderStreamMaxDurationDuringOpenReportsActualLimit(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	spec := &TableSpec{SourceSchema: "s", SourceTable: "t", Identity: &entity.TableIdentity{
		Strategy: entity.FullColumnsStrategy,
		Columns:  []entity.ColumnMeta{{Name: "payload"}},
	}}
	mock.ExpectQuery("SELECT `payload` FROM `s`.`t`").
		WillDelayFor(300 * time.Millisecond).
		WillReturnRows(sqlmock.NewRows([]string{"payload"}).AddRow("x"))

	maxDur := 40 * time.Millisecond
	opt := Options{
		QueryTimeout:           5 * time.Second, // 远大于 max duration
		StreamIdleTimeout:      time.Hour,
		StreamMaxDuration:      maxDur,
		SlowQueryWarnThreshold: time.Hour,
	}
	cr, err := newChunkReader(db, &Chunk{ID: "c1", Spec: spec, NoPK: true}, 10, defaultBatchBytes, opt, 1, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cr.close()

	_, err = cr.nextBatch(context.Background())
	if !IsReadQueryTimeout(err) {
		t.Fatalf("expected ReadQueryTimeoutError, got %T: %v", err, err)
	}
	te := err.(*ReadQueryTimeoutError)
	if te.Timeout != maxDur {
		t.Fatalf("expected Timeout=%v (max duration), got %v (elapsed=%v)", maxDur, te.Timeout, te.Elapsed)
	}
}

// TestChunkReaderEnqueueObservesStreamWatchCancel 验证写队列背压期间 watch 触发会
// 唤醒 Put，并转为 ReadQueryTimeoutError（而不是当成功退出）。
// TestReadChunkStreamMaxDurationDuringQueueBackpressure 通过 readChunk 真实路径验证：
// 未耗尽的 stream 读到首批后，写队列背压期间 max_duration 触发 ReadQueryTimeoutError。
func TestReadChunkStreamMaxDurationDuringQueueBackpressure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	spec := &TableSpec{
		SourceSchema: "s", SourceTable: "t",
		TargetSchema: "d", TargetTable: "u",
		Identity: &entity.TableIdentity{
			Strategy: entity.FullColumnsStrategy,
			Columns:  []entity.ColumnMeta{{Name: "payload"}},
		},
	}
	chunk := &Chunk{ID: "c1", Spec: spec, NoPK: true}

	rows := sqlmock.NewRows([]string{"payload"})
	for i := 0; i < 50; i++ {
		rows.AddRow("payload-data")
	}
	mock.ExpectQuery("SELECT `payload` FROM `s`.`t`").WillReturnRows(rows)

	maxDur := 40 * time.Millisecond
	opt := Options{
		BatchRows:              5,
		BatchBytes:             defaultBatchBytes,
		QueryTimeout:           time.Hour,
		StreamIdleTimeout:      time.Hour,
		StreamMaxDuration:      maxDur,
		SlowQueryWarnThreshold: time.Hour,
	}

	q := newBatchQueue(10, &Stats{})
	if err := q.Put(context.Background(), &RowBatch{ApproxBytes: 10}); err != nil {
		t.Fatal(err)
	}

	stats := &Stats{}
	err = readChunk(context.Background(), db, chunk, q, opt, stats, nil, nil, nil, nil, 1, nil)
	if !IsReadQueryTimeout(err) {
		t.Fatalf("expected ReadQueryTimeoutError, got %T: %v", err, err)
	}
	var te *ReadQueryTimeoutError
	if !errors.As(err, &te) {
		t.Fatalf("expected wrapped ReadQueryTimeoutError, got %v", err)
	}
	if te.Timeout != maxDur {
		t.Fatalf("expected Timeout=%v, got %v", maxDur, te.Timeout)
	}
	if te.Phase != "stream" {
		t.Fatalf("expected phase=stream, got %s", te.Phase)
	}
}

// TestChunkReaderUserCancelNotReportedAsTimeout 验证用户取消 context 时不被误判为查询超时。
func TestChunkReaderUserCancelNotReportedAsTimeout(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	spec := &TableSpec{
		SourceSchema: "s", SourceTable: "t",
		Identity: &entity.TableIdentity{
			Strategy: entity.PKStrategy, IdentifyCols: []string{"id"}, CursorCols: []string{"id"},
			Columns: []entity.ColumnMeta{{Name: "id"}},
		},
	}
	chunk := &Chunk{ID: "c1", Spec: spec, Start: []any{1}, End: []any{100}}
	mock.ExpectQuery("SELECT `id` FROM `s`.`t` WHERE").
		WillDelayFor(500 * time.Millisecond).
		WillReturnError(context.Canceled)

	// QueryTimeout 足够长，确保超时不会先触发；用户取消应优先。
	opt := Options{QueryTimeout: 10 * time.Second, SlowQueryWarnThreshold: time.Hour}
	cr, err := newChunkReader(db, chunk, 10, defaultBatchBytes, opt, 1, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cr.close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	_, err = cr.nextBatch(ctx)
	if err == nil {
		t.Fatal("expected cancellation error, got nil")
	}
	// 关键断言：用户取消绝不能被误判为查询超时。
	if IsReadQueryTimeout(err) {
		t.Fatalf("user cancel must not be reported as query timeout: %v", err)
	}
	// 底层驱动取消返回的错误文案可能是 "canceling query due to user request"，
	// 精确匹配 context.Canceled 不可靠；此处断言父 ctx 确已取消即可。
	if ctx.Err() != context.Canceled {
		t.Fatalf("expected parent ctx canceled, got %v", ctx.Err())
	}
}

// TestChunkReaderZeroOptionTimeoutGuarded 验证零值 Options 时 runQuery 使用默认防护，不会立即超时。
func TestChunkReaderZeroOptionTimeoutGuarded(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	spec := &TableSpec{
		SourceSchema: "s", SourceTable: "t",
		Identity: &entity.TableIdentity{
			Strategy: entity.PKStrategy, IdentifyCols: []string{"id"}, CursorCols: []string{"id"},
			Columns: []entity.ColumnMeta{{Name: "id"}},
		},
	}
	chunk := &Chunk{ID: "c1", Spec: spec}
	mock.ExpectQuery("SELECT `id` FROM `s`.`t`").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))

	// 零值 Options：QueryTimeout=0 应被防护为默认值，不能立即超时。
	cr, err := newChunkReader(db, chunk, 10, defaultBatchBytes, Options{}, 1, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cr.close()

	batch, err := cr.nextBatch(context.Background())
	if err != nil {
		t.Fatalf("zero-option query must not time out immediately: %v", err)
	}
	if batch == nil || len(batch.Rows) != 1 {
		t.Fatalf("expected 1 row, got %+v", batch)
	}
}

// TestLogicalWindowRows 验证逻辑窗口固定为 batch_size，不再按列类型缩小。
func TestLogicalWindowRows(t *testing.T) {
	opt := Options{BatchRows: 1000}
	if got := logicalWindowRows(opt); got != 1000 {
		t.Fatalf("logicalWindowRows=%d want 1000", got)
	}
	opt.BatchRows = 0
	if got := logicalWindowRows(opt); got != defaultBatchRows {
		t.Fatalf("zero batch rows should fall back to default, got %d", got)
	}
}

// TestHasLargeColumnTypes 验证宽列判定。
func TestHasLargeColumnTypes(t *testing.T) {
	cases := []struct {
		name    string
		columns []entity.ColumnMeta
		want    bool
	}{
		{"no_large", []entity.ColumnMeta{{Name: "id", DataType: "bigint"}, {Name: "name", DataType: "varchar(64)"}}, false},
		{"json", []entity.ColumnMeta{{Name: "id", DataType: "bigint"}, {Name: "data", DataType: "json"}}, true},
		{"longtext", []entity.ColumnMeta{{Name: "body", DataType: "longtext"}}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			spec := &TableSpec{Identity: &entity.TableIdentity{Columns: c.columns}}
			if got := hasLargeColumnTypes(spec); got != c.want {
				t.Fatalf("hasLargeColumnTypes=%v want %v", got, c.want)
			}
		})
	}
}

// TestShouldUseTwoPhaseRead 验证宽表自动两阶段与显式开关。
func TestShouldUseTwoPhaseRead(t *testing.T) {
	wideSpec := &TableSpec{
		Identity: &entity.TableIdentity{
			Strategy: entity.PKStrategy, CursorCols: []string{"id"},
			Columns: []entity.ColumnMeta{{Name: "id", DataType: "bigint"}, {Name: "payload", DataType: "json"}},
		},
	}
	narrowSpec := &TableSpec{
		Identity: &entity.TableIdentity{
			Strategy: entity.PKStrategy, CursorCols: []string{"id"},
			Columns: []entity.ColumnMeta{{Name: "id", DataType: "bigint"}, {Name: "name", DataType: "varchar(32)"}},
		},
	}
	chunk := &Chunk{Spec: wideSpec}
	if !shouldUseTwoPhaseRead(wideSpec, chunk, Options{}) {
		t.Fatal("wide table should auto-enable two-phase")
	}
	if shouldUseTwoPhaseRead(narrowSpec, &Chunk{Spec: narrowSpec}, Options{}) {
		t.Fatal("narrow table should not use two-phase by default")
	}
	if !shouldUseTwoPhaseRead(narrowSpec, &Chunk{Spec: narrowSpec}, Options{TwoPhaseRead: true}) {
		t.Fatal("TwoPhaseRead=true should force enable")
	}
}

// TestChunkReaderTwoPhaseKeysetBatch 验证两阶段读取：pk_probe → payload_fetch，游标正确推进，返回完整行。
func TestChunkReaderTwoPhaseKeysetBatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	spec := &TableSpec{
		SourceSchema: "s", SourceTable: "t", TargetSchema: "d", TargetTable: "u",
		Identity: &entity.TableIdentity{
			Strategy: entity.PKStrategy, IdentifyCols: []string{"id"}, CursorCols: []string{"id"},
			Columns: []entity.ColumnMeta{{Name: "id"}, {Name: "payload", DataType: "json"}},
		},
	}
	chunk := &Chunk{ID: "c1", Spec: spec, Start: []any{0}, End: []any{100}}

	// Phase 1: pk_probe — 只查主键列
	mock.ExpectQuery("SELECT `id` FROM `s`.`t` WHERE").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(1)).AddRow(int64(2)))
	// Phase 2: payload_fetch — IN 查询完整行
	mock.ExpectQuery("SELECT `id`, `payload` FROM `s`.`t` WHERE `id` IN").
		WillReturnRows(sqlmock.NewRows([]string{"id", "payload"}).AddRow(int64(1), "x").AddRow(int64(2), "y"))

	opt := Options{QueryTimeout: 10 * time.Second, SlowQueryWarnThreshold: time.Hour}
	cr, err := newChunkReader(db, chunk, 10, defaultBatchBytes, opt, 1, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cr.close()

	batch, err := cr.nextBatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if batch == nil || len(batch.Rows) != 2 {
		t.Fatalf("expected 2 rows from two-phase read, got %+v", batch)
	}
	if len(cr.cursor) != 1 || cr.cursor[0].(int64) != 2 {
		t.Fatalf("cursor should advance to last pk=2, got %v", cr.cursor)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestReadChunksParallelPlainRetriesBadConnectionWithFreshPoolConn(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	spec := &TableSpec{
		SourceSchema: "s", SourceTable: "t", TargetSchema: "d", TargetTable: "u",
		Identity: &entity.TableIdentity{
			Strategy: entity.PKStrategy, IdentifyCols: []string{"id"}, CursorCols: []string{"id"},
			Columns: []entity.ColumnMeta{{Name: "id"}, {Name: "payload"}},
		},
	}
	chunk := &Chunk{ID: "c1", Spec: spec, Start: []any{0}, End: []any{100}}

	// 第一次结果集在扫描时断线；该批次尚未完成，也没有推进 keyset 游标。
	mock.ExpectQuery("SELECT `id`, `payload` FROM `s`.`t` WHERE").
		WillReturnRows(sqlmock.NewRows([]string{"id", "payload"}).
			AddRow(int64(1), "discarded").
			RowError(0, driver.ErrBadConn))
	// 重试必须经 *sql.DB 获取新连接，并从原游标重新读取完整批次。
	mock.ExpectQuery("SELECT `id`, `payload` FROM `s`.`t` WHERE").
		WillReturnRows(sqlmock.NewRows([]string{"id", "payload"}).AddRow(int64(1), "ok"))

	opt := Options{
		BatchRows:              10,
		BatchBytes:             defaultBatchBytes,
		QueryTimeout:           10 * time.Second,
		SlowQueryWarnThreshold: time.Hour,
	}
	stats := &Stats{}
	q := newBatchQueue(defaultBufferBytes, stats)

	err = readChunksParallelPlain(context.Background(), db, 1, []*Chunk{chunk}, q, opt, stats, nil, nil, nil, nil, nil, 0, nil)
	if err != nil {
		t.Fatalf("expected bad connection to recover through the pool, got %v", err)
	}

	batch, ok := q.Get(context.Background())
	if !ok || batch == nil || len(batch.Rows) != 1 {
		t.Fatalf("expected one recovered batch, got ok=%v batch=%+v", ok, batch)
	}
	if got := batch.Rows[0][1]; got != "ok" {
		t.Fatalf("failed partial batch must be discarded, got payload %v", got)
	}
	if stats.ChunksDone != 1 {
		t.Fatalf("expected one completed chunk, got %d", stats.ChunksDone)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestClassifyScanError_QueryTimeoutRetryable 验证 Scan 阶段查询超时包装为 ReadQueryTimeoutError。
func TestClassifyScanError_QueryTimeoutRetryable(t *testing.T) {
	spec := &TableSpec{
		SourceSchema: "s", SourceTable: "t",
		Identity: &entity.TableIdentity{
			Strategy: entity.PKStrategy, IdentifyCols: []string{"id"}, CursorCols: []string{"id"},
			Columns: []entity.ColumnMeta{{Name: "id"}},
		},
	}
	cr := &chunkReader{
		chunk:        &Chunk{ID: "c1", Spec: spec, Start: []any{1}, End: []any{100}},
		queryPhase:   "keyset",
		queryTimeout: 50 * time.Millisecond,
		queryStart:   time.Now().Add(-80 * time.Millisecond),
	}
	qctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(2 * time.Millisecond)
	cr.queryCtx = qctx

	got := cr.classifyScanError(context.Background(), context.DeadlineExceeded)
	if !IsReadQueryTimeout(got) {
		t.Fatalf("expected ReadQueryTimeoutError, got %T: %v", got, got)
	}
	if !isRetryableReadError(got) {
		t.Fatalf("scan-phase query timeout must be retryable: %v", got)
	}
	te := got.(*ReadQueryTimeoutError)
	if te.Phase != "keyset" || te.ChunkID != "c1" {
		t.Fatalf("missing context: %+v", te)
	}
}

func TestChunkReaderUsesSnapshotQueryer(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	spec := &TableSpec{
		SourceSchema: "s", SourceTable: "t", TargetSchema: "d", TargetTable: "u",
		Identity: &entity.TableIdentity{
			Strategy: entity.PKStrategy, IdentifyCols: []string{"id"}, CursorCols: []string{"id"},
			Columns: []entity.ColumnMeta{{Name: "id"}, {Name: "payload"}},
		},
	}
	chunk := &Chunk{ID: "c1", Spec: spec, Start: []any{int64(0)}, End: []any{int64(10)}}
	mock.ExpectQuery("SELECT `id`, `payload` FROM `s`.`t` WHERE").
		WithArgs(int64(0), int64(10), 10).
		WillReturnRows(sqlmock.NewRows([]string{"id", "payload"}).AddRow(int64(1), "x"))

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	cr, err := newChunkReader(conn, chunk, 10, defaultBatchBytes, Options{}, 1, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cr.close()
	batch, err := cr.nextBatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Rows) != 1 {
		t.Fatalf("rows=%d", len(batch.Rows))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPlanIntegerRange_UsesPlanQueryer(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	spec := &TableSpec{
		SourceSchema: "s", SourceTable: "t", EstimatedRows: 100,
		Identity: &entity.TableIdentity{
			Strategy: entity.PKStrategy, IdentifyCols: []string{"id"}, CursorCols: []string{"id"},
			Columns: []entity.ColumnMeta{{Name: "id", DataType: "bigint"}},
		},
	}
	mock.ExpectQuery("SELECT MIN\\(`id`\\), MAX\\(`id`\\) FROM `s`.`t`").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(int64(1), int64(100)))

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	chunks, err := NewPlanner(conn).planTable(context.Background(), spec, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 4 {
		t.Fatalf("chunks=%d want 4", len(chunks))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestDecideTableReaders 验证按估算行数/chunk 数决定单表内并行读连接数。
func TestDecideTableReaders(t *testing.T) {
	opt := ResolveOptions(RawOptions{ReadWorkers: 4})
	job := &tableReadJob{
		spec: &TableSpec{
			EstimatedRows: 50,
			Identity:      &entity.TableIdentity{Strategy: entity.PKStrategy, Columns: []entity.ColumnMeta{{Name: "id"}}},
		},
		chunks: []*Chunk{{ID: "1"}, {ID: "2"}, {ID: "3"}},
	}
	if got := decideTableReaders(job, opt); got != 1 {
		t.Fatalf("small table readers=%d want 1", got)
	}
	job.spec.EstimatedRows = defaultLargeTableRows
	if got := decideTableReaders(job, opt); got != 3 {
		t.Fatalf("large table readers=%d want 3 (min chunks)", got)
	}
	if got := decideTableReadersForSpec(job.spec, opt); got != 4 {
		t.Fatalf("large table pre-plan readers=%d want 4", got)
	}
	job.chunks = []*Chunk{{ID: "only"}}
	if got := decideTableReaders(job, opt); got != 1 {
		t.Fatalf("single chunk readers=%d want 1", got)
	}
}

// TestClassifyScanError_ParentCancelNotTimeout 验证父 ctx 取消时不误判为查询超时。
func TestClassifyScanError_ParentCancelNotTimeout(t *testing.T) {
	spec := &TableSpec{
		SourceSchema: "s", SourceTable: "t",
		Identity: &entity.TableIdentity{Columns: []entity.ColumnMeta{{Name: "id"}}},
	}
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	cr := &chunkReader{
		chunk:      &Chunk{ID: "c1", Spec: spec},
		queryPhase: "stream",
		queryCtx:   parent,
	}
	got := cr.classifyScanError(parent, context.Canceled)
	if IsReadQueryTimeout(got) {
		t.Fatalf("parent cancel must not become query timeout: %v", got)
	}
}

func TestScanUpTo_BytesSplitAndOversizedRowCallback(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	large := make([]byte, 5000)
	rows := sqlmock.NewRows([]string{"id", "payload"}).
		AddRow(int64(1), large).
		AddRow(int64(2), []byte("x"))
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	r, err := db.Query("SELECT id, payload FROM s.t")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	var oversized int64
	got, bytes, _, err := scanUpTo(r, 2, 1000, 4096, nil, func(n int64) { oversized = n })
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || bytes <= 4096 {
		t.Fatalf("expected byte-truncated batch with oversized first row, rows=%d bytes=%d", len(got), bytes)
	}
	if oversized <= 4096 {
		t.Fatalf("expected oversized row callback, got %d", oversized)
	}
}
