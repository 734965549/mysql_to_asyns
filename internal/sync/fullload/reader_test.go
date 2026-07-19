package fullload

import (
	"context"
	"testing"

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
	cr, err := newChunkReader(db, chunk, 2, defaultBatchBytes)
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
	cr, err := newChunkReader(db, &Chunk{ID: "c1", Spec: spec, NoPK: true}, 1, defaultBatchBytes)
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
	if _, err := newChunkReader(db, &Chunk{ID: "bad", Spec: spec}, 10, defaultBatchBytes); err == nil {
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
	cr, err := newChunkReader(db, &Chunk{ID: "wide", Spec: spec, NoPK: true}, 10, 10)
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
