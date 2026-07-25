package fullload

import (
	"context"
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
	cr, err := newChunkReader(db, chunk, 2, defaultBatchBytes, Options{}, 1, nil)
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
	cr, err := newChunkReader(db, &Chunk{ID: "c1", Spec: spec, NoPK: true}, 1, defaultBatchBytes, Options{}, 1, nil)
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
	if _, err := newChunkReader(db, &Chunk{ID: "bad", Spec: spec}, 10, defaultBatchBytes, Options{}, 1, nil); err == nil {
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
	cr, err := newChunkReader(db, &Chunk{ID: "wide", Spec: spec, NoPK: true}, 10, 10, Options{}, 1, nil)
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
	cr, err := newChunkReader(db, chunk, 10, defaultBatchBytes, opt, 1, nil)
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

	opt := Options{QueryTimeout: 20 * time.Millisecond, SlowQueryWarnThreshold: time.Hour}
	cr, err := newChunkReader(db, &Chunk{ID: "c1", Spec: spec, NoPK: true}, 10, defaultBatchBytes, opt, 1, nil)
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
	cr, err := newChunkReader(db, chunk, 10, defaultBatchBytes, opt, 1, nil)
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
	cr, err := newChunkReader(db, chunk, 10, defaultBatchBytes, Options{}, 1, nil)
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

// TestEffectiveBatchRows_LargeFields 验证含大字段表按规则缩小 BatchRows。
func TestEffectiveBatchRows_LargeFields(t *testing.T) {
	opt := Options{BatchRows: 1000}
	cases := []struct {
		name    string
		columns []entity.ColumnMeta
		want    int
	}{
		{"no_large", []entity.ColumnMeta{{Name: "id", DataType: "bigint"}, {Name: "name", DataType: "varchar(64)"}}, 1000},
		{"one_json", []entity.ColumnMeta{{Name: "id", DataType: "bigint"}, {Name: "data", DataType: "json"}}, 250},
		{"two_json", []entity.ColumnMeta{{Name: "id", DataType: "bigint"}, {Name: "req", DataType: "json"}, {Name: "resp", DataType: "json"}}, 100},
		{"longtext", []entity.ColumnMeta{{Name: "id", DataType: "bigint"}, {Name: "body", DataType: "longtext"}}, 50},
		{"one_text", []entity.ColumnMeta{{Name: "id", DataType: "bigint"}, {Name: "note", DataType: "text"}}, 250},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			spec := &TableSpec{Identity: &entity.TableIdentity{Columns: c.columns}}
			got := effectiveBatchRows(spec, opt)
			if got != c.want {
				t.Fatalf("effectiveBatchRows=%d want %d", got, c.want)
			}
		})
	}
}

// TestEffectiveBatchRows_FloorAt25 验证极小 BatchRows 下大字段表不低于下限 25。
func TestEffectiveBatchRows_FloorAt25(t *testing.T) {
	opt := Options{BatchRows: 100}
	spec := &TableSpec{Identity: &entity.TableIdentity{Columns: []entity.ColumnMeta{
		{Name: "id", DataType: "bigint"}, {Name: "body", DataType: "longblob"},
	}}}
	// 100/20=5，应被抬到下限 25
	if got := effectiveBatchRows(spec, opt); got != 25 {
		t.Fatalf("effectiveBatchRows=%d want floor 25", got)
	}
}

// TestEffectiveBatchRows_NilSpec 验证 nil spec/identity 回退到 opt.BatchRows。
func TestEffectiveBatchRows_NilSpec(t *testing.T) {
	opt := Options{BatchRows: 777}
	if got := effectiveBatchRows(nil, opt); got != 777 {
		t.Fatalf("nil spec should return opt.BatchRows, got %d", got)
	}
	if got := effectiveBatchRows(&TableSpec{}, opt); got != 777 {
		t.Fatalf("nil identity should return opt.BatchRows, got %d", got)
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
			Columns: []entity.ColumnMeta{{Name: "id"}, {Name: "payload"}},
		},
	}
	chunk := &Chunk{ID: "c1", Spec: spec, Start: []any{0}, End: []any{100}}

	// Phase 1: pk_probe — 只查主键列
	mock.ExpectQuery("SELECT `id` FROM `s`.`t` WHERE").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(1)).AddRow(int64(2)))
	// Phase 2: payload_fetch — IN 查询完整行
	mock.ExpectQuery("SELECT `id`, `payload` FROM `s`.`t` WHERE `id` IN").
		WillReturnRows(sqlmock.NewRows([]string{"id", "payload"}).AddRow(int64(1), "x").AddRow(int64(2), "y"))

	opt := Options{QueryTimeout: 10 * time.Second, SlowQueryWarnThreshold: time.Hour, TwoPhaseRead: true}
	cr, err := newChunkReader(db, chunk, 10, defaultBatchBytes, opt, 1, nil)
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
