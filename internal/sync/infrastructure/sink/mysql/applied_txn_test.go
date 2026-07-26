package mysql

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sinkDomain "mysql-to-sync/internal/sync/domain/sink"
)

func TestMySQLSink_MarkAppliedTxnInSameTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=0").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS .*_mts_applied_txn.*").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT .*_mts_applied_txn.*").
		WithArgs("task-1").WillReturnRows(sqlmock.NewRows([]string{"binlog_file", "binlog_pos"}))
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO .*_mts_applied_txn.*").
		WithArgs("task-1", "mysql-bin.000001", uint32(500)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO .*users.*ON DUPLICATE KEY UPDATE").
		WithArgs(int64(1), "Alice").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=1").WillReturnResult(sqlmock.NewResult(0, 0))

	s := NewMySQLSink(db, mysqlSinkAnalyzer{identity: testIdentity()}, 10)
	require.NoError(t, s.Open(context.Background()))
	require.NoError(t, s.PrepareTables(context.Background(), map[string]string{"source": "target"}, []string{"users"}))
	assert.Equal(t, "target", s.metaSchema)

	// 与生产路径一致：先 HasAppliedTxn（建表）再 Begin，避免事务内 DDL。
	applied, err := s.HasAppliedTxn(context.Background(), "task-1", mysql.Position{Name: "mysql-bin.000001", Pos: 500})
	require.NoError(t, err)
	assert.False(t, applied)

	require.NoError(t, s.BeginTransaction(context.Background()))
	require.NoError(t, s.Write(context.Background(), &sinkDomain.ChangeEvent{
		SourceSchema: "source", SourceTable: "users", EventType: "INSERT",
		After: map[string]interface{}{"id": int64(1), "name": "Alice"},
	}))
	require.NoError(t, s.MarkAppliedTxn(context.Background(), "task-1", mysql.Position{Name: "mysql-bin.000001", Pos: 500}))
	require.NoError(t, s.CommitTransaction(context.Background()))
	require.NoError(t, s.Close(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMySQLSink_HasAppliedTxnDetectsCommittedPosition(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=0").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS .*_mts_applied_txn.*").
		WillReturnResult(sqlmock.NewResult(0, 0))
	rows := sqlmock.NewRows([]string{"binlog_file", "binlog_pos"}).
		AddRow("mysql-bin.000001", uint64(500))
	mock.ExpectQuery("SELECT .*_mts_applied_txn.*").
		WithArgs("task-1").WillReturnRows(rows)
	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=1").WillReturnResult(sqlmock.NewResult(0, 0))

	s := NewMySQLSink(db, mysqlSinkAnalyzer{identity: testIdentity()}, 10)
	require.NoError(t, s.Open(context.Background()))
	require.NoError(t, s.PrepareTables(context.Background(), map[string]string{"source": "target"}, []string{"users"}))

	applied, err := s.HasAppliedTxn(context.Background(), "task-1", mysql.Position{Name: "mysql-bin.000001", Pos: 500})
	require.NoError(t, err)
	assert.True(t, applied)

	require.NoError(t, s.Close(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMySQLSink_HasAppliedTxnReturnsFalseWhenMissing(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=0").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS .*_mts_applied_txn.*").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT .*_mts_applied_txn.*").
		WithArgs("task-1").WillReturnRows(sqlmock.NewRows([]string{"binlog_file", "binlog_pos"}))
	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=1").WillReturnResult(sqlmock.NewResult(0, 0))

	s := NewMySQLSink(db, mysqlSinkAnalyzer{identity: testIdentity()}, 10)
	require.NoError(t, s.Open(context.Background()))
	require.NoError(t, s.PrepareTables(context.Background(), map[string]string{"source": "target"}, []string{"users"}))

	applied, err := s.HasAppliedTxn(context.Background(), "task-1", mysql.Position{Name: "mysql-bin.000001", Pos: 100})
	require.NoError(t, err)
	assert.False(t, applied)
	require.NoError(t, s.Close(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}
