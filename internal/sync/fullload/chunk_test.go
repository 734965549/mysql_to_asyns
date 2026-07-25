package fullload

import (
	"context"
	"math"
	"testing"

	"mysql-to-sync/internal/metadata/domain/entity"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestIsIntegerColumn(t *testing.T) {
	id := &entity.TableIdentity{Columns: []entity.ColumnMeta{
		{Name: "id", DataType: "bigint"},
		{Name: "code", DataType: "varchar(64)"},
		{Name: "cnt", DataType: "int unsigned"},
	}}
	if !isIntegerColumn(id, "id") {
		t.Error("bigint should be integer")
	}
	if isIntegerColumn(id, "code") {
		t.Error("varchar should not be integer")
	}
	if isIntegerColumn(id, "cnt") {
		t.Error("unsigned integer should use keyset planning because NullInt64 cannot cover its full range")
	}
}

func TestBuildKeysetLower_Single(t *testing.T) {
	where, args := buildKeysetLower([]string{"id"}, []any{int64(10)})
	if where != "`id` > ?" {
		t.Errorf("where=%q", where)
	}
	if len(args) != 1 || args[0].(int64) != 10 {
		t.Errorf("args=%v", args)
	}
}

func TestBuildKeysetLower_Composite(t *testing.T) {
	where, args := buildKeysetLower([]string{"a", "b"}, []any{1, 2})
	// (a = ? AND b > ?) OR (a > ?)
	want := "(`a` = ? AND `b` > ?) OR (`a` > ?)"
	if where != want {
		t.Errorf("where=%q want %q", where, want)
	}
	if len(args) != 3 {
		t.Errorf("args len=%d", len(args))
	}
}

func TestBuildKeysetUpperInclusive_Composite(t *testing.T) {
	where, args := buildKeysetUpperInclusive([]string{"a", "b"}, []any{1, 2})
	want := "(`a` < ?) OR (`a` = ? AND `b` <= ?)"
	if where != want {
		t.Errorf("where=%q want %q", where, want)
	}
	if len(args) != 3 || args[0] != 1 || args[1] != 1 || args[2] != 2 {
		t.Errorf("args=%v", args)
	}
}

func TestPlanTable_NoPK(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	p := NewPlanner(db)
	spec := &TableSpec{SourceSchema: "s", SourceTable: "t", Identity: &entity.TableIdentity{Strategy: entity.FullColumnsStrategy}}
	chunks, err := p.planTable(context.Background(), spec, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 || !chunks[0].NoPK || !chunks[0].Sequential {
		t.Fatalf("nopk plan wrong: %+v", chunks)
	}
}

func TestPlanTable_Composite(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	p := NewPlanner(db)
	spec := &TableSpec{SourceSchema: "s", SourceTable: "t", EstimatedRows: 3000, Identity: &entity.TableIdentity{
		Strategy:     entity.PKStrategy,
		IdentifyCols: []string{"a", "b"},
		CursorCols:   []string{"a", "b"},
		Columns:      []entity.ColumnMeta{{Name: "a", DataType: "int"}, {Name: "b", DataType: "int"}},
	}}
	mock.ExpectQuery("SELECT `a`, `b`").WillReturnRows(sqlmock.NewRows([]string{"a", "b"}).AddRow(1, 1000))
	mock.ExpectQuery("SELECT `a`, `b`").WillReturnRows(sqlmock.NewRows([]string{"a", "b"}).AddRow(2, 2000))
	mock.ExpectQuery("SELECT `a`, `b`").WillReturnRows(sqlmock.NewRows([]string{"a", "b"}))
	chunks, err := p.planTable(context.Background(), spec, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 3 || chunks[0].Sequential || chunks[0].NoPK {
		t.Fatalf("composite plan wrong: %+v", chunks)
	}
	if chunks[0].Start != nil || chunks[2].End != nil {
		t.Fatalf("first/last boundaries wrong: %+v", chunks)
	}
	for i := 1; i < len(chunks); i++ {
		if len(chunks[i-1].End) != 2 || len(chunks[i].Start) != 2 ||
			chunks[i-1].End[0] != chunks[i].Start[0] || chunks[i-1].End[1] != chunks[i].Start[1] {
			t.Fatalf("chunk boundary gap/overlap at %d: prev=%v next=%v", i, chunks[i-1].End, chunks[i].Start)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPlanIntegerRangeFullInt64DoesNotOverflow(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	p := NewPlanner(db)
	spec := &TableSpec{SourceSchema: "s", SourceTable: "t", Identity: &entity.TableIdentity{
		Strategy:     entity.PKStrategy,
		IdentifyCols: []string{"id"},
		CursorCols:   []string{"id"},
		Columns:      []entity.ColumnMeta{{Name: "id", DataType: "bigint"}},
	}}
	mock.ExpectQuery("SELECT MIN").WillReturnRows(
		sqlmock.NewRows([]string{"min", "max"}).AddRow(int64(math.MinInt64), int64(math.MaxInt64)))
	chunks, err := p.planTable(context.Background(), spec, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 8 || chunks[0].Start != nil || chunks[len(chunks)-1].End != nil {
		t.Fatalf("unexpected chunks: %+v", chunks)
	}
	for i := 1; i < len(chunks); i++ {
		if chunks[i-1].End[0] != chunks[i].Start[0] {
			t.Fatalf("boundary mismatch at %d: %v vs %v", i, chunks[i-1].End, chunks[i].Start)
		}
	}
}

func TestPlanIntegerRange(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	p := NewPlanner(db)
	spec := &TableSpec{SourceSchema: "s", SourceTable: "t", Identity: &entity.TableIdentity{
		Strategy:     entity.PKStrategy,
		IdentifyCols: []string{"id"},
		CursorCols:   []string{"id"},
		Columns:      []entity.ColumnMeta{{Name: "id", DataType: "bigint"}},
	}}

	rows := sqlmock.NewRows([]string{"MIN(`id`)", "MAX(`id`)"}).AddRow(int64(1), int64(100))
	mock.ExpectQuery("SELECT MIN\\(`id`\\), MAX\\(`id`\\)").WillReturnRows(rows)

	chunks, err := p.planTable(context.Background(), spec, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) < 2 {
		t.Fatalf("expected multiple integer-range chunks, got %d", len(chunks))
	}
	// 第一个 chunk 无下界，最后一个 chunk 无上界。
	if chunks[0].Start != nil {
		t.Errorf("first chunk should have nil Start")
	}
	if chunks[len(chunks)-1].End != nil {
		t.Errorf("last chunk should have nil End")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestPlanIntegerRange_EmptyTable(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	p := NewPlanner(db)
	spec := &TableSpec{SourceSchema: "s", SourceTable: "t", Identity: &entity.TableIdentity{
		Strategy:     entity.PKStrategy,
		IdentifyCols: []string{"id"},
		CursorCols:   []string{"id"},
		Columns:      []entity.ColumnMeta{{Name: "id", DataType: "int"}},
	}}
	rows := sqlmock.NewRows([]string{"min", "max"}).AddRow(nil, nil)
	mock.ExpectQuery("SELECT MIN").WillReturnRows(rows)
	chunks, err := p.planTable(context.Background(), spec, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 {
		t.Fatalf("empty table should give 1 chunk, got %d", len(chunks))
	}
}

// TestPlanIntegerRange_SparseDetection 验证稀疏 PK 检测：值域超过估算行数 4 倍时切换为 keyset 采样。
func TestPlanIntegerRange_SparseDetection(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	p := NewPlanner(db)
	spec := &TableSpec{
		SourceSchema: "s", SourceTable: "t",
		EstimatedRows: 5000, // 估算 5000 行（须 > defaultBatchRows 才会真正采样切分）
		Identity: &entity.TableIdentity{
			Strategy:     entity.PKStrategy,
			IdentifyCols: []string{"id"},
			CursorCols:   []string{"id"},
			Columns:      []entity.ColumnMeta{{Name: "id", DataType: "bigint"}},
		},
	}
	// 值域 1~100000，span=100000，阈值=5000×4=20000，100000>20000 触发稀疏检测，切换到 keyset 采样。
	minMaxRows := sqlmock.NewRows([]string{"MIN(`id`)", "MAX(`id`)"}).AddRow(int64(1), int64(100000))
	mock.ExpectQuery("SELECT MIN\\(`id`\\), MAX\\(`id`\\)").WillReturnRows(minMaxRows)
	// keyset 采样按 step 步进（est/targetChunks，下限 1000）执行 OFFSET 查询，取两个边界后 ErrNoRows 结束。
	mock.ExpectQuery("SELECT `id` FROM `s`.`t` ORDER BY `id` ASC LIMIT 1 OFFSET").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(1667)))
	mock.ExpectQuery("SELECT `id` FROM `s`.`t` WHERE .* ORDER BY `id` ASC LIMIT 1 OFFSET").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(3334)))
	mock.ExpectQuery("SELECT `id` FROM `s`.`t` WHERE .* ORDER BY `id` ASC LIMIT 1 OFFSET").
		WillReturnRows(sqlmock.NewRows([]string{"id"}))

	chunks, err := p.planTable(context.Background(), spec, 3)
	if err != nil {
		t.Fatal(err)
	}
	// keyset 采样生成的 chunk：2 个边界 → 3 个 chunk（含末尾无上界 chunk）
	if len(chunks) < 2 {
		t.Fatalf("expected keyset sampling chunks, got %d", len(chunks))
	}
	// 验证走的是 keyset 采样：第一个 chunk 无 Start，最后一个 chunk 无 End
	if chunks[0].Start != nil || chunks[len(chunks)-1].End != nil {
		t.Fatalf("keyset boundaries wrong: %+v", chunks)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestPlanIntegerRange_DenseNoFallback 验证密集表不触发稀疏检测。
func TestPlanIntegerRange_DenseNoFallback(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	p := NewPlanner(db)
	spec := &TableSpec{
		SourceSchema: "s", SourceTable: "t",
		EstimatedRows: 1000, // 估算 1000 行
		Identity: &entity.TableIdentity{
			Strategy:     entity.PKStrategy,
			IdentifyCols: []string{"id"},
			CursorCols:   []string{"id"},
			Columns:      []entity.ColumnMeta{{Name: "id", DataType: "bigint"}},
		},
	}
	// 值域 1~1500，span=1500，阈值=1000×4=4000，1500<4000 不触发，继续等值域切分。
	minMaxRows := sqlmock.NewRows([]string{"MIN(`id`)", "MAX(`id`)"}).AddRow(int64(1), int64(1500))
	mock.ExpectQuery("SELECT MIN\\(`id`\\), MAX\\(`id`\\)").WillReturnRows(minMaxRows)

	chunks, err := p.planTable(context.Background(), spec, 4)
	if err != nil {
		t.Fatal(err)
	}
	// 应该走等值域切分，不会有额外的 keyset 采样查询
	if len(chunks) < 2 {
		t.Fatalf("expected range-split chunks, got %d", len(chunks))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
