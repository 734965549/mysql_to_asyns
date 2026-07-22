package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	sinkDomain "mysql-to-sync/internal/sync/domain/sink"
)

func TestMySQLSink_CloseRollbacksActiveTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=0").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectBegin()
	mock.ExpectRollback()
	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=1").WillReturnResult(sqlmock.NewResult(0, 0))

	s := NewMySQLSink(db, mysqlSinkAnalyzer{identity: testIdentity()}, 10)
	require.NoError(t, s.Open(context.Background()))
	require.NoError(t, s.PrepareTables(context.Background(), map[string]string{"source": "target"}, []string{"users"}))
	require.NoError(t, s.BeginTransaction(context.Background()))
	require.NoError(t, s.Write(context.Background(), &sinkDomain.ChangeEvent{
		SourceSchema: "source", SourceTable: "users", EventType: "INSERT",
		After: map[string]interface{}{"id": int64(1), "name": "Alice"},
	}))
	require.NoError(t, s.Close(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMySQLSink_TransactionDefersCommitUntilCommitTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=0").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO .*users.*ON DUPLICATE KEY UPDATE").
		WithArgs(int64(1), "Alice", int64(2), "Bob").WillReturnResult(sqlmock.NewResult(2, 2))
	mock.ExpectCommit()
	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=1").WillReturnResult(sqlmock.NewResult(0, 0))

	s := NewMySQLSink(db, mysqlSinkAnalyzer{identity: testIdentity()}, 10)
	require.NoError(t, s.Open(context.Background()))
	require.NoError(t, s.PrepareTables(context.Background(), map[string]string{"source": "target"}, []string{"users"}))

	require.NoError(t, s.BeginTransaction(context.Background()))
	require.NoError(t, s.Write(context.Background(), &sinkDomain.ChangeEvent{
		SourceSchema: "source", SourceTable: "users", EventType: "INSERT",
		After: map[string]interface{}{"id": int64(1), "name": "Alice"},
	}))
	require.NoError(t, s.Write(context.Background(), &sinkDomain.ChangeEvent{
		SourceSchema: "source", SourceTable: "users", EventType: "INSERT",
		After: map[string]interface{}{"id": int64(2), "name": "Bob"},
	}))
	require.NoError(t, s.CommitTransaction(context.Background()))
	require.NoError(t, s.Close(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMySQLSink_RollbackTransactionDiscardsBufferedWrites(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=0").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectBegin()
	mock.ExpectRollback()
	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=1").WillReturnResult(sqlmock.NewResult(0, 0))

	s := NewMySQLSink(db, mysqlSinkAnalyzer{identity: testIdentity()}, 10)
	require.NoError(t, s.Open(context.Background()))
	require.NoError(t, s.PrepareTables(context.Background(), map[string]string{"source": "target"}, []string{"users"}))

	require.NoError(t, s.BeginTransaction(context.Background()))
	require.NoError(t, s.Write(context.Background(), &sinkDomain.ChangeEvent{
		SourceSchema: "source", SourceTable: "users", EventType: "INSERT",
		After: map[string]interface{}{"id": int64(1), "name": "Alice"},
	}))
	require.NoError(t, s.RollbackTransaction(context.Background()))
	require.NoError(t, s.Close(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMySQLSink_TransactionInsertThenUpdatePreservesOrder(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=0").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO .*users.*ON DUPLICATE KEY UPDATE").
		WithArgs(int64(1), "A").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("UPDATE .*users.*").
		WithArgs("B", int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=1").WillReturnResult(sqlmock.NewResult(0, 0))

	s := NewMySQLSink(db, mysqlSinkAnalyzer{identity: testIdentity()}, 10)
	require.NoError(t, s.Open(context.Background()))
	require.NoError(t, s.PrepareTables(context.Background(), map[string]string{"source": "target"}, []string{"users"}))

	require.NoError(t, s.BeginTransaction(context.Background()))
	require.NoError(t, s.Write(context.Background(), &sinkDomain.ChangeEvent{
		SourceSchema: "source", SourceTable: "users", EventType: "INSERT",
		After: map[string]interface{}{"id": int64(1), "name": "A"},
	}))
	require.NoError(t, s.Write(context.Background(), &sinkDomain.ChangeEvent{
		SourceSchema: "source", SourceTable: "users", EventType: "UPDATE",
		Before: map[string]interface{}{"id": int64(1), "name": "A"},
		After:  map[string]interface{}{"id": int64(1), "name": "B"},
	}))
	require.NoError(t, s.CommitTransaction(context.Background()))
	require.NoError(t, s.Close(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMySQLSink_TransactionInsertThenDeletePreservesOrder(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=0").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO .*users.*ON DUPLICATE KEY UPDATE").
		WithArgs(int64(1), "A").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("DELETE FROM .*users.*").
		WithArgs(int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=1").WillReturnResult(sqlmock.NewResult(0, 0))

	s := NewMySQLSink(db, mysqlSinkAnalyzer{identity: testIdentity()}, 10)
	require.NoError(t, s.Open(context.Background()))
	require.NoError(t, s.PrepareTables(context.Background(), map[string]string{"source": "target"}, []string{"users"}))

	require.NoError(t, s.BeginTransaction(context.Background()))
	require.NoError(t, s.Write(context.Background(), &sinkDomain.ChangeEvent{
		SourceSchema: "source", SourceTable: "users", EventType: "INSERT",
		After: map[string]interface{}{"id": int64(1), "name": "A"},
	}))
	require.NoError(t, s.Write(context.Background(), &sinkDomain.ChangeEvent{
		SourceSchema: "source", SourceTable: "users", EventType: "DELETE",
		Before: map[string]interface{}{"id": int64(1), "name": "A"},
	}))
	require.NoError(t, s.CommitTransaction(context.Background()))
	require.NoError(t, s.Close(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMySQLSink_BeginPausesAutoFlushUntilRollback(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=0").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectBegin()
	mock.ExpectRollback()
	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=1").WillReturnResult(sqlmock.NewResult(0, 0))

	s := NewMySQLSink(db, mysqlSinkAnalyzer{identity: testIdentity()}, 10)
	require.NoError(t, s.Open(context.Background()))
	require.NoError(t, s.PrepareTables(context.Background(), map[string]string{"source": "target"}, []string{"users"}))

	require.NoError(t, s.BeginTransaction(context.Background()))
	require.NoError(t, s.Write(context.Background(), &sinkDomain.ChangeEvent{
		SourceSchema: "source", SourceTable: "users", EventType: "INSERT",
		After: map[string]interface{}{"id": int64(1), "name": "Alice"},
	}))

	// Begin 后 auto-flush 已暂停：等待超过默认 500ms interval，不应出现 INSERT。
	time.Sleep(600 * time.Millisecond)

	require.NoError(t, s.RollbackTransaction(context.Background()))
	require.NoError(t, s.Close(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}
