package writer

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"mysql-to-async/internal/metadata/domain/entity"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestNewBatchWriter(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	identity := &entity.TableIdentity{
		Strategy:     entity.PKStrategy,
		Columns:      []entity.ColumnMeta{{Name: "id"}, {Name: "name"}},
		IdentifyCols: []string{"id"},
	}

	writer := NewBatchWriter(db, identity, 100)
	assert.NotNil(t, writer)
	assert.Equal(t, 100, writer.batchSize)
	assert.Equal(t, 30*time.Second, writer.timeout)
}

func TestBatchWriter_WriteBatch(t *testing.T) {
	tests := []struct {
		name      string
		rows      []map[string]interface{}
		setupMock func(mock sqlmock.Sqlmock)
		expectErr bool
	}{
		{
			name: "成功批量写入",
			rows: []map[string]interface{}{
				{"id": 1, "name": "Alice"},
				{"id": 2, "name": "Bob"},
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("REPLACE INTO").
					WillReturnResult(sqlmock.NewResult(0, 2))
			},
			expectErr: false,
		},
		{
			name:      "空数据直接返回",
			rows:      []map[string]interface{}{},
			setupMock: func(mock sqlmock.Sqlmock) {},
			expectErr: false,
		},
		{
			name:      "nil数据直接返回",
			rows:      nil,
			setupMock: func(mock sqlmock.Sqlmock) {},
			expectErr: false,
		},
		{
			name: "数据库错误",
			rows: []map[string]interface{}{
				{"id": 1, "name": "Alice"},
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("REPLACE INTO").
					WillReturnError(sql.ErrConnDone)
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("failed to create mock db: %v", err)
			}
			defer db.Close()

			tt.setupMock(mock)

			identity := &entity.TableIdentity{
				Strategy:     entity.PKStrategy,
				Columns:      []entity.ColumnMeta{{Name: "id"}, {Name: "name"}},
				IdentifyCols: []string{"id"},
			}

			writer := NewBatchWriter(db, identity, 100)
			err = writer.WriteBatch(context.Background(), tt.rows)

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestBatchWriter_Update(t *testing.T) {
	tests := []struct {
		name      string
		row       map[string]interface{}
		setupMock func(mock sqlmock.Sqlmock)
		expectErr bool
	}{
		{
			name: "成功更新",
			row:  map[string]interface{}{"id": 1, "name": "Alice Updated"},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE").
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			expectErr: false,
		},
		{
			name: "更新无匹配行（数据漂移）",
			row:  map[string]interface{}{"id": 999, "name": "Unknown"},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE").
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			expectErr: false,
		},
		{
			name: "数据库错误",
			row:  map[string]interface{}{"id": 1, "name": "Alice"},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE").
					WillReturnError(sql.ErrConnDone)
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("failed to create mock db: %v", err)
			}
			defer db.Close()

			tt.setupMock(mock)

			identity := &entity.TableIdentity{
				Strategy:     entity.PKStrategy,
				Columns:      []entity.ColumnMeta{{Name: "id"}, {Name: "name"}},
				IdentifyCols: []string{"id"},
			}

			writer := NewBatchWriter(db, identity, 100)
			err = writer.Update(context.Background(), tt.row)

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestBatchWriter_UpdateWithBeforeImage(t *testing.T) {
	tests := []struct {
		name        string
		row         map[string]interface{}
		beforeImage map[string]interface{}
		setupMock   func(mock sqlmock.Sqlmock)
		expectErr   bool
	}{
		{
			name:        "成功使用before image更新",
			row:         map[string]interface{}{"id": 1, "name": "Alice Updated"},
			beforeImage: map[string]interface{}{"id": 1, "name": "Alice"},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE").
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			expectErr: false,
		},
		{
			name:        "无匹配行",
			row:         map[string]interface{}{"id": 999, "name": "Unknown"},
			beforeImage: map[string]interface{}{"id": 999, "name": "Old"},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE").
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			expectErr: false,
		},
		{
			name:        "数据库错误",
			row:         map[string]interface{}{"id": 1, "name": "Alice"},
			beforeImage: map[string]interface{}{"id": 1, "name": "Old"},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE").
					WillReturnError(sql.ErrConnDone)
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("failed to create mock db: %v", err)
			}
			defer db.Close()

			tt.setupMock(mock)

			identity := &entity.TableIdentity{
				Strategy:     entity.FullColumnsStrategy,
				Columns:      []entity.ColumnMeta{{Name: "id"}, {Name: "name"}},
				IdentifyCols: []string{},
			}

			writer := NewBatchWriter(db, identity, 100)
			err = writer.UpdateWithBeforeImage(context.Background(), tt.row, tt.beforeImage)

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestBatchWriter_Delete(t *testing.T) {
	tests := []struct {
		name      string
		row       map[string]interface{}
		setupMock func(mock sqlmock.Sqlmock)
		expectErr bool
	}{
		{
			name: "成功删除",
			row:  map[string]interface{}{"id": 1},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE").
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			expectErr: false,
		},
		{
			name: "删除无匹配行（数据漂移）",
			row:  map[string]interface{}{"id": 999},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE").
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			expectErr: false,
		},
		{
			name: "数据库错误",
			row:  map[string]interface{}{"id": 1},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE").
					WillReturnError(sql.ErrConnDone)
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("failed to create mock db: %v", err)
			}
			defer db.Close()

			tt.setupMock(mock)

			identity := &entity.TableIdentity{
				Strategy:     entity.PKStrategy,
				Columns:      []entity.ColumnMeta{{Name: "id"}, {Name: "name"}},
				IdentifyCols: []string{"id"},
			}

			writer := NewBatchWriter(db, identity, 100)
			err = writer.Delete(context.Background(), tt.row)

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestBatchWriter_GetSQLBuilder(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	identity := &entity.TableIdentity{
		Strategy:     entity.PKStrategy,
		Columns:      []entity.ColumnMeta{{Name: "id"}},
		IdentifyCols: []string{"id"},
	}

	writer := NewBatchWriter(db, identity, 100)
	builder := writer.GetSQLBuilder()

	assert.NotNil(t, builder)
}

func TestNewBufferedWriter(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	identity := &entity.TableIdentity{
		Strategy:     entity.PKStrategy,
		Columns:      []entity.ColumnMeta{{Name: "id"}},
		IdentifyCols: []string{"id"},
	}

	writer := NewBufferedWriter(db, identity, 100, time.Second)
	assert.NotNil(t, writer)
	assert.Equal(t, 100, writer.batchSize)
	assert.Equal(t, time.Second, writer.flushInterval)

	// 关闭以停止后台goroutine
	err = writer.Close()
	assert.NoError(t, err)
}

func TestBufferedWriter_WriteAndFlush(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	identity := &entity.TableIdentity{
		Strategy:     entity.PKStrategy,
		Columns:      []entity.ColumnMeta{{Name: "id"}, {Name: "name"}},
		IdentifyCols: []string{"id"},
	}

	// 设置批量大小为2，写入2条数据后应该自动flush
	writer := NewBufferedWriter(db, identity, 2, time.Minute)

	// 第一条写入，不应该触发flush
	err = writer.Write(map[string]interface{}{"id": 1, "name": "Alice"})
	assert.NoError(t, err)

	// 第二条写入，应该触发flush
	mock.ExpectExec("REPLACE INTO").
		WillReturnResult(sqlmock.NewResult(0, 2))
	err = writer.Write(map[string]interface{}{"id": 2, "name": "Bob"})
	assert.NoError(t, err)

	// 关闭writer（此时缓冲区应该为空，不会再有Exec调用）
	err = writer.Close()
	assert.NoError(t, err)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestBufferedWriter_FlushEmpty(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	identity := &entity.TableIdentity{
		Strategy:     entity.PKStrategy,
		Columns:      []entity.ColumnMeta{{Name: "id"}},
		IdentifyCols: []string{"id"},
	}

	writer := NewBufferedWriter(db, identity, 100, time.Minute)

	// 空缓冲区刷新应该直接返回
	err = writer.Flush()
	assert.NoError(t, err)

	// 关闭writer
	err = writer.Close()
	assert.NoError(t, err)
}

func TestBatchWriter_SetAuditLogger(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	identity := &entity.TableIdentity{
		Strategy:     entity.PKStrategy,
		Columns:      []entity.ColumnMeta{{Name: "id"}},
		IdentifyCols: []string{"id"},
	}

	writer := NewBatchWriter(db, identity, 100)

	// 设置审计日志器（可以为nil）
	writer.SetAuditLogger(nil, "task-123", "test_db", "users")

	assert.Equal(t, "task-123", writer.taskID)
	assert.Equal(t, "test_db", writer.schema)
	assert.Equal(t, "users", writer.tableName)
}

func TestBatchWriter_WriteBatchWithAuditLogger(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	mock.ExpectExec("REPLACE INTO").
		WillReturnResult(sqlmock.NewResult(0, 2))

	identity := &entity.TableIdentity{
		Strategy:     entity.PKStrategy,
		Columns:      []entity.ColumnMeta{{Name: "id"}, {Name: "name"}},
		IdentifyCols: []string{"id"},
	}

	writer := NewBatchWriter(db, identity, 100)
	// 设置审计日志器为nil，但不影响功能
	writer.SetAuditLogger(nil, "task-123", "test_db", "users")

	rows := []map[string]interface{}{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
	}

	err = writer.WriteBatch(context.Background(), rows)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestBufferedWriter_FlushWithData(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	identity := &entity.TableIdentity{
		Strategy:     entity.PKStrategy,
		Columns:      []entity.ColumnMeta{{Name: "id"}, {Name: "name"}},
		IdentifyCols: []string{"id"},
	}

	writer := NewBufferedWriter(db, identity, 100, time.Minute)

	// 手动添加数据到缓冲区
	writer.mu.Lock()
	writer.buffer = append(writer.buffer,
		map[string]interface{}{"id": 1, "name": "Alice"},
		map[string]interface{}{"id": 2, "name": "Bob"},
	)
	writer.mu.Unlock()

	// Flush应该触发写入
	mock.ExpectExec("REPLACE INTO").
		WillReturnResult(sqlmock.NewResult(0, 2))

	err = writer.Flush()
	assert.NoError(t, err)

	// 关闭writer（缓冲区已空）
	err = writer.Close()
	assert.NoError(t, err)

	assert.NoError(t, mock.ExpectationsWereMet())
}
