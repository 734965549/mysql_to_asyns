package fullload

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestInsertPrefix(t *testing.T) {
	b := &RowBatch{TargetSchema: "db", TargetTable: "t", Columns: []string{"id", "name"}}
	got := insertPrefix(b)
	want := "INSERT INTO `db`.`t` (`id`, `name`) VALUES "
	if got != want {
		t.Fatalf("prefix=%q want %q", got, want)
	}
}

func TestIsRetryableWriteErr(t *testing.T) {
	cases := map[string]bool{
		"Error 1213: Deadlock found when trying to get lock":     true,
		"Error 1205: Lock wait timeout exceeded":                 true,
		"try restarting transaction":                             true,
		"invalid connection":                                     true,
		"bad connection":                                         true,
		"Error 1062: Duplicate entry '1' for key 'PRIMARY'":      false,
		"syntax error":                                           false,
	}
	for msg, want := range cases {
		if got := isRetryableWriteErr(errors.New(msg)); got != want {
			t.Errorf("isRetryableWriteErr(%q)=%v want %v", msg, got, want)
		}
	}
	if isRetryableWriteErr(nil) {
		t.Error("nil should be non-retryable")
	}
}

func TestWriteBatchInTx_MultiValueInsert(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	batch := &RowBatch{
		TargetSchema: "db",
		TargetTable:  "t",
		Columns:      []string{"id", "name"},
		Rows: [][]any{
			{int64(1), "a"},
			{int64(2), "b"},
		},
	}

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `db`.`t`").
		WithArgs(int64(1), "a", int64(2), "b").
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	opt := ResolveOptions(RawOptions{BatchSize: 1000})
	if err := writeBatchInTx(context.Background(), tx, batch, opt); err != nil {
		t.Fatalf("writeBatchInTx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestWriteBatchInTx_SplitByPlaceholder(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 3 行、每行 1 列，限制每条语句最多 2 行 → 拆成 2 条 INSERT。
	batch := &RowBatch{
		TargetSchema: "db", TargetTable: "t",
		Columns: []string{"id"},
		Rows:    [][]any{{int64(1)}, {int64(2)}, {int64(3)}},
	}
	opt := ResolveOptions(RawOptions{BatchSize: 2})

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `db`.`t`").WithArgs(int64(1), int64(2)).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec("INSERT INTO `db`.`t`").WithArgs(int64(3)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	tx, _ := db.Begin()
	if err := writeBatchInTx(context.Background(), tx, batch, opt); err != nil {
		t.Fatalf("writeBatchInTx: %v", err)
	}
	tx.Commit()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
