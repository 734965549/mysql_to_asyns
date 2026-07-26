package mysql

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	metadataEntity "mysql-to-sync/internal/metadata/domain/entity"
	sinkDomain "mysql-to-sync/internal/sync/domain/sink"
)

type mysqlSinkAnalyzer struct {
	identity *metadataEntity.TableIdentity
	err      error
}

func (a mysqlSinkAnalyzer) AnalyzeTable(string, string) (*metadataEntity.TableIdentity, error) {
	return a.identity, a.err
}
func (a mysqlSinkAnalyzer) GetAllTables(string) ([]metadataEntity.TableInfo, error) { return nil, nil }
func (a mysqlSinkAnalyzer) GetAllDatabases() ([]string, error)                      { return nil, nil }

func testIdentity() *metadataEntity.TableIdentity {
	return &metadataEntity.TableIdentity{
		TableName:    "users",
		Strategy:     metadataEntity.PKStrategy,
		IdentifyCols: []string{"id"},
		HasPK:        true,
		Columns: []metadataEntity.ColumnMeta{
			{Name: "id", IsPrimaryKey: true},
			{Name: "name"},
		},
	}
}

func TestMySQLSink_InsertUpdateDeleteAndClose(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=0").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT INTO .*users.*ON DUPLICATE KEY UPDATE").
		WithArgs(int64(1), "Alice").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("UPDATE .*users.*").
		WithArgs("Alicia", int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("DELETE FROM .*users.*").
		WithArgs(int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=1").WillReturnResult(sqlmock.NewResult(0, 0))

	s := NewMySQLSink(db, mysqlSinkAnalyzer{identity: testIdentity()}, 10)
	require.NoError(t, s.Open(context.Background()))
	require.NoError(t, s.PrepareTables(context.Background(), map[string]string{"source": "target"}, []string{"users"}))
	require.NoError(t, s.Write(context.Background(), &sinkDomain.ChangeEvent{
		SourceSchema: "source", SourceTable: "users", EventType: "INSERT",
		After: map[string]interface{}{"id": int64(1), "name": "Alice"},
	}))
	require.NoError(t, s.Flush(context.Background()))
	require.NoError(t, s.Write(context.Background(), &sinkDomain.ChangeEvent{
		SourceSchema: "source", SourceTable: "users", EventType: "UPDATE",
		Before: map[string]interface{}{"id": int64(1), "name": "Alice"},
		After:  map[string]interface{}{"id": int64(1), "name": "Alicia"},
	}))
	require.NoError(t, s.Write(context.Background(), &sinkDomain.ChangeEvent{
		SourceSchema: "source", SourceTable: "users", EventType: "DELETE",
		Before: map[string]interface{}{"id": int64(1), "name": "Alicia"},
	}))
	require.NoError(t, s.Close(context.Background()))
	require.NoError(t, s.Close(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMySQLSink_IdentityChangeUsesTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=0").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM .*users.*").WithArgs(int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO .*users.*ON DUPLICATE KEY UPDATE").
		WithArgs(int64(2), "Alice").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=1").WillReturnResult(sqlmock.NewResult(0, 0))

	s := NewMySQLSink(db, mysqlSinkAnalyzer{identity: testIdentity()}, 10)
	require.NoError(t, s.Open(context.Background()))
	require.NoError(t, s.PrepareTables(context.Background(), map[string]string{"source": "target"}, []string{"users"}))
	require.NoError(t, s.Write(context.Background(), &sinkDomain.ChangeEvent{
		SourceSchema: "source", SourceTable: "users", EventType: "UPDATE",
		Before: map[string]interface{}{"id": int64(1), "name": "Alice"},
		After:  map[string]interface{}{"id": int64(2), "name": "Alice"},
	}))
	require.NoError(t, s.Close(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMySQLSink_NoPrimaryKeyUsesBeforeImageAndLimitOne(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	identity := &metadataEntity.TableIdentity{
		TableName: "logs", Strategy: metadataEntity.FullColumnsStrategy,
		IdentifyCols: []string{"message", "level"},
		Columns:      []metadataEntity.ColumnMeta{{Name: "message"}, {Name: "level"}},
	}
	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=0").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("UPDATE .*logs.*LIMIT 1").
		WithArgs("hello", "info", "hello", "debug").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("DELETE FROM .*logs.*LIMIT 1").
		WithArgs("hello", "info").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=1").WillReturnResult(sqlmock.NewResult(0, 0))

	s := NewMySQLSink(db, mysqlSinkAnalyzer{identity: identity}, 10)
	require.NoError(t, s.Open(context.Background()))
	require.NoError(t, s.PrepareTables(context.Background(), map[string]string{"source": "target"}, []string{"logs"}))
	require.NoError(t, s.Write(context.Background(), &sinkDomain.ChangeEvent{
		TaskID: "task", SourceSchema: "source", SourceTable: "logs", EventType: "UPDATE",
		Before: map[string]interface{}{"message": "hello", "level": "debug"},
		After:  map[string]interface{}{"message": "hello", "level": "info"},
	}))
	require.NoError(t, s.Write(context.Background(), &sinkDomain.ChangeEvent{
		TaskID: "task", SourceSchema: "source", SourceTable: "logs", EventType: "DELETE",
		Before: map[string]interface{}{"message": "hello", "level": "info"},
	}))
	require.NoError(t, s.Close(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMySQLSink_RejectsMissingImagesUnknownAndUnpreparedTables(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=0").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=1").WillReturnResult(sqlmock.NewResult(0, 0))
	s := NewMySQLSink(db, mysqlSinkAnalyzer{identity: testIdentity()}, 10)
	require.NoError(t, s.Open(context.Background()))

	for _, event := range []*sinkDomain.ChangeEvent{
		nil,
		{SourceSchema: "source", SourceTable: "missing", EventType: "INSERT", After: map[string]interface{}{"id": 1}},
	} {
		require.Error(t, s.Write(context.Background(), event))
	}
	require.NoError(t, s.PrepareTables(context.Background(), map[string]string{"source": "target"}, []string{"users"}))
	for _, event := range []*sinkDomain.ChangeEvent{
		{SourceSchema: "source", SourceTable: "users", EventType: "INSERT"},
		{SourceSchema: "source", SourceTable: "users", EventType: "UPDATE"},
		{SourceSchema: "source", SourceTable: "users", EventType: "DELETE"},
		{SourceSchema: "source", SourceTable: "users", EventType: "TRUNCATE"},
	} {
		require.Error(t, s.Write(context.Background(), event))
	}
	require.NoError(t, s.Close(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMySQLSink_RejectsInvalidLifecycleAndEvents(t *testing.T) {
	t.Run("open validation", func(t *testing.T) {
		err := NewMySQLSink(nil, nil, 0).Open(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "target database")
	})
	t.Run("prepare before open", func(t *testing.T) {
		s := NewMySQLSink(nil, mysqlSinkAnalyzer{}, 10)
		err := s.PrepareTables(context.Background(), map[string]string{"db": "db"}, []string{"users"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not open")
	})
	t.Run("analyzer error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()
		mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=0").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=1").WillReturnResult(sqlmock.NewResult(0, 0))
		s := NewMySQLSink(db, mysqlSinkAnalyzer{err: errors.New("metadata failed")}, 10)
		require.NoError(t, s.Open(context.Background()))
		err = s.PrepareTables(context.Background(), map[string]string{"db": "db"}, []string{"users"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "metadata failed")
		require.NoError(t, s.Close(context.Background()))
		require.NoError(t, mock.ExpectationsWereMet())
	})
}
