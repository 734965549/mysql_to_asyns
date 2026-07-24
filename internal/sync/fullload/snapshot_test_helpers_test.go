package fullload

import (
	"regexp"

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

// expectTxMarkerTableOK 匹配 ensureTxMarkerTables 建表后的结构校验查询。
func expectTxMarkerTableOK(mock sqlmock.Sqlmock, schema string) {
	mock.ExpectQuery(regexp.QuoteMeta("SELECT TABLE_TYPE, ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?")).
		WithArgs(schema, txMarkerTableName).
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_TYPE", "ENGINE"}).AddRow("BASE TABLE", "InnoDB"))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?")).
		WithArgs(schema, txMarkerTableName, "id").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME", "IS_NULLABLE", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH"}).
			AddRow("id", "NO", "char", int64(36)))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?")).
		WithArgs(schema, txMarkerTableName, "run_id").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME", "IS_NULLABLE", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH"}).
			AddRow("run_id", "NO", "varchar", int64(txMarkerRunIDMaxLen)))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT INDEX_NAME, SUB_PART FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ? AND NON_UNIQUE = 0 AND SEQ_IN_INDEX = 1")).
		WithArgs(schema, txMarkerTableName, "id").
		WillReturnRows(sqlmock.NewRows([]string{"INDEX_NAME", "SUB_PART"}).AddRow("PRIMARY", nil))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ?")).
		WithArgs(schema, txMarkerTableName, "PRIMARY").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
}

// expectTxMarkerSchemaLock 匹配对目标 schema 的 wait_timeout 准备 + GET_LOCK。
func expectTxMarkerSchemaLock(mock sqlmock.Sqlmock, schema string) {
	name := txMarkerSchemaLockName(schema)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT @@SESSION.wait_timeout")).
		WillReturnRows(sqlmock.NewRows([]string{"wait_timeout"}).AddRow(int64(28800)))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT GET_LOCK(?, ?)")).
		WithArgs(name, txMarkerSchemaLockTimeoutSec).
		WillReturnRows(sqlmock.NewRows([]string{"GET_LOCK(?, ?)"}).AddRow(1))
}

func expectTxMarkerSchemaUnlock(mock sqlmock.Sqlmock, schema string) {
	name := txMarkerSchemaLockName(schema)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT RELEASE_LOCK(?)")).
		WithArgs(name).
		WillReturnRows(sqlmock.NewRows([]string{"RELEASE_LOCK(?)"}).AddRow(1))
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
