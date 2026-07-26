package fullload

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestWriteSessionSkipBinlogAndRestore(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	expectWriteSession(mock, true)
	expectRestoreWriteSession(mock, true)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := setupWriteSession(context.Background(), conn, true); err != nil {
		t.Fatal(err)
	}
	restoreWriteSession(conn, true, "", -1, false)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestWriteSessionSetupFailureRestoresChangedChecks(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectExec("SET @@SESSION.FOREIGN_KEY_CHECKS=0").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET @@SESSION.UNIQUE_CHECKS=0").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT @@SESSION.FOREIGN_KEY_CHECKS").WillReturnRows(
		sqlmock.NewRows([]string{"fk", "uk"}).AddRow(1, 0))
	expectRestoreWriteSession(mock, false)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := setupWriteSession(context.Background(), conn, false); !errors.Is(err, errForeignKeyChecksStillOn) {
		t.Fatalf("expected foreign key verification error, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
