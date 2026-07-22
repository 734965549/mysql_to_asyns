package fullload

import (
	"github.com/DATA-DOG/go-sqlmock"
)

func expectInnoDBTable(mock sqlmock.Sqlmock, schema, table string) {
	mock.ExpectQuery("SELECT ENGINE FROM information_schema.TABLES").
		WithArgs(schema, table).
		WillReturnRows(sqlmock.NewRows([]string{"ENGINE"}).AddRow("InnoDB"))
}

func expectInnoDBTableEngine(mock sqlmock.Sqlmock, schema, table, engine string) {
	mock.ExpectQuery("SELECT ENGINE FROM information_schema.TABLES").
		WithArgs(schema, table).
		WillReturnRows(sqlmock.NewRows([]string{"ENGINE"}).AddRow(engine))
}

func expectConsistentSnapshot(mock sqlmock.Sqlmock, schema, table, firstCol string) {
	mock.ExpectExec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT `" + firstCol + "` FROM `" + schema + "`.`" + table + "` LIMIT 1").
		WillReturnRows(sqlmock.NewRows([]string{firstCol}))
}

func expectLockWaitTimeout(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT @@SESSION.lock_wait_timeout").
		WillReturnRows(sqlmock.NewRows([]string{"lock_wait_timeout"}).AddRow(50))
	mock.ExpectExec("SET SESSION lock_wait_timeout").
		WillReturnResult(sqlmock.NewResult(0, 0))
}

func expectRestoreLockWaitTimeout(mock sqlmock.Sqlmock) {
	mock.ExpectExec("SET SESSION lock_wait_timeout").
		WillReturnResult(sqlmock.NewResult(0, 0))
}

func expectSnapshotCommit(mock sqlmock.Sqlmock) {
	mock.ExpectExec("COMMIT").WillReturnResult(sqlmock.NewResult(0, 0))
}
