package fullload

import (
	"context"
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
	if !isIntegerColumn(id, "cnt") {
		t.Error("int unsigned should be integer")
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
	db, _, _ := sqlmock.New()
	defer db.Close()
	p := NewPlanner(db)
	spec := &TableSpec{SourceSchema: "s", SourceTable: "t", Identity: &entity.TableIdentity{
		Strategy:     entity.PKStrategy,
		IdentifyCols: []string{"a", "b"},
		CursorCols:   []string{"a", "b"},
		Columns:      []entity.ColumnMeta{{Name: "a", DataType: "int"}, {Name: "b", DataType: "int"}},
	}}
	chunks, err := p.planTable(context.Background(), spec, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 || !chunks[0].Sequential || chunks[0].NoPK {
		t.Fatalf("composite plan wrong: %+v", chunks)
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
