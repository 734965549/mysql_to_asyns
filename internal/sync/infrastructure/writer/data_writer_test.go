package writer

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"mysql-to-sync/internal/metadata/domain/entity"

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
	assert.Equal(t, 300*time.Second, writer.timeout)
}

func TestBatchWriter_WriteBatch(t *testing.T) {
	tests := []struct {
		name      string
		rows      []map[string]interface{}
		setupMock func(mock sqlmock.Sqlmock)
		expectErr bool
	}{
		{
			name: "鎴愬姛鎵归噺鍐欏叆",
			rows: []map[string]interface{}{
				{"id": 1, "name": "Alice"},
				{"id": 2, "name": "Bob"},
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT").
					WillReturnResult(sqlmock.NewResult(0, 2))
			},
			expectErr: false,
		},
		{
			name:      "Empty data returns directly",
			rows:      []map[string]interface{}{},
			setupMock: func(mock sqlmock.Sqlmock) {},
			expectErr: false,
		},
		{
			name:      "Nil data",
			rows:      nil,
			setupMock: func(mock sqlmock.Sqlmock) {},
			expectErr: false,
		},
		{
			name: "Database error",
			rows: []map[string]interface{}{
				{"id": 1, "name": "Alice"},
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT").
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
			name: "Update row",
			row:  map[string]interface{}{"id": 1, "name": "Alice Updated"},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE").
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			expectErr: false,
		},
		{
			name: "Update non-existent row",
			row:  map[string]interface{}{"id": 999, "name": "Unknown"},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE").
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			expectErr: false,
		},
		{
			name: "Database error",
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
			name:        "Update row with before image",
			row:         map[string]interface{}{"id": 1, "name": "Alice Updated"},
			beforeImage: map[string]interface{}{"id": 1, "name": "Alice"},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE").
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			expectErr: false,
		},
		{
			name:        "Update non-existent row",
			row:         map[string]interface{}{"id": 999, "name": "Unknown"},
			beforeImage: map[string]interface{}{"id": 999, "name": "Old"},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE").
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			expectErr: false,
		},
		{
			name:        "Database error",
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
			name: "鎴愬姛鍒犻櫎",
			row:  map[string]interface{}{"id": 1},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE").
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			expectErr: false,
		},
		{
			name: "鍒犻櫎鏃犲尮閰嶈锛堟暟鎹紓绉伙級",
			row:  map[string]interface{}{"id": 999},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE").
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			expectErr: false,
		},
		{
			name: "Database error",
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

	// 鍏抽棴浠ュ仠姝㈠悗鍙癵oroutine
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

	// 璁剧疆鎵归噺澶у皬涓?锛屽啓鍏?鏉℃暟鎹悗搴旇鑷姩flush
	writer := NewBufferedWriter(db, identity, 2, time.Minute)

	// 绗竴鏉″啓鍏ワ紝涓嶅簲璇ヨЕ鍙慺lush
	err = writer.Write(map[string]interface{}{"id": 1, "name": "Alice"})
	assert.NoError(t, err)

	// 绗簩鏉″啓鍏ワ紝搴旇瑙﹀彂flush
	mock.ExpectExec("INSERT").
		WillReturnResult(sqlmock.NewResult(0, 2))
	err = writer.Write(map[string]interface{}{"id": 2, "name": "Bob"})
	assert.NoError(t, err)

	// 鍏抽棴writer锛堟鏃剁紦鍐插尯搴旇涓虹┖锛屼笉浼氬啀鏈塃xec璋冪敤锛?
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

	// 绌虹紦鍐插尯鍒锋柊搴旇鐩存帴杩斿洖
	err = writer.Flush()
	assert.NoError(t, err)

	// 鍏抽棴writer
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

	// 璁剧疆瀹¤鏃ュ織鍣紙鍙互涓簄il锛?
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

	mock.ExpectExec("INSERT").
		WillReturnResult(sqlmock.NewResult(0, 2))

	identity := &entity.TableIdentity{
		Strategy:     entity.PKStrategy,
		Columns:      []entity.ColumnMeta{{Name: "id"}, {Name: "name"}},
		IdentifyCols: []string{"id"},
	}

	writer := NewBatchWriter(db, identity, 100)
	// 璁剧疆瀹¤鏃ュ織鍣ㄤ负nil锛屼絾涓嶅奖鍝嶅姛鑳?
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

	// 鎵嬪姩娣诲姞鏁版嵁鍒扮紦鍐插尯
	writer.mu.Lock()
	writer.buffer = append(writer.buffer,
		map[string]interface{}{"id": 1, "name": "Alice"},
		map[string]interface{}{"id": 2, "name": "Bob"},
	)
	writer.mu.Unlock()

	// Flush搴旇瑙﹀彂鍐欏叆
	mock.ExpectExec("INSERT").
		WillReturnResult(sqlmock.NewResult(0, 2))

	err = writer.Flush()
	assert.NoError(t, err)

	// 鍏抽棴writer锛堢紦鍐插尯宸茬┖锛?
	err = writer.Close()
	assert.NoError(t, err)

	assert.NoError(t, mock.ExpectationsWereMet())
}

// === 修复 10/11：BatchWriter.WriteBatch 在 useUpsert 模式下必须发出 ON DUPLICATE KEY UPDATE ===

// TestBatchWriter_WriteBatch_DefaultIsInsertIgnore 默认模式应保持 INSERT IGNORE（全量同步语义不变）。
func TestBatchWriter_WriteBatch_DefaultIsInsertIgnore(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	identity := &entity.TableIdentity{
		TableName:    "users",
		Strategy:     entity.PKStrategy,
		HasPK:        true,
		Columns:      []entity.ColumnMeta{{Name: "id", IsPrimaryKey: true}, {Name: "name"}},
		IdentifyCols: []string{"id"},
	}

	mock.ExpectExec("INSERT IGNORE INTO `users` (`id`, `name`) VALUES (?, ?)").
		WithArgs(1, "Alice").
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := NewBatchWriter(db, identity, 100)
	err = w.WriteBatch(context.Background(), []map[string]interface{}{{"id": 1, "name": "Alice"}})
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestBatchWriter_WriteBatch_UpsertModeEmitsOnDuplicateKey 增量模式启用 upsert 后，
// 同一 SQL 必须改为 `INSERT ... ON DUPLICATE KEY UPDATE 非键列 = VALUES(...)`。
func TestBatchWriter_WriteBatch_UpsertModeEmitsOnDuplicateKey(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	identity := &entity.TableIdentity{
		TableName:    "users",
		Strategy:     entity.PKStrategy,
		HasPK:        true,
		Columns:      []entity.ColumnMeta{{Name: "id", IsPrimaryKey: true}, {Name: "name"}},
		IdentifyCols: []string{"id"},
	}

	mock.ExpectExec("INSERT INTO `users` (`id`, `name`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)").
		WithArgs(1, "Alice").
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := NewBatchWriter(db, identity, 100)
	w.EnableUpsert()
	err = w.WriteBatch(context.Background(), []map[string]interface{}{{"id": 1, "name": "Alice"}})
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestBufferedWriter_EnableUpsert_ForwardsToBatchWriter BufferedWriter 是增量路径的入口，
// EnableUpsert 必须能正确转发到底层 BatchWriter，避免增量 INSERT 仍走 IGNORE。
func TestBufferedWriter_EnableUpsert_ForwardsToBatchWriter(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	identity := &entity.TableIdentity{
		TableName:    "users",
		Strategy:     entity.PKStrategy,
		HasPK:        true,
		Columns:      []entity.ColumnMeta{{Name: "id", IsPrimaryKey: true}, {Name: "name"}},
		IdentifyCols: []string{"id"},
	}

	bw := NewBufferedWriter(db, identity, 100, time.Hour)
	bw.EnableUpsert()

	mock.ExpectExec("INSERT INTO `users` (`id`, `name`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)").
		WithArgs(1, "Alice").
		WillReturnResult(sqlmock.NewResult(0, 1))

	bw.mu.Lock()
	bw.buffer = append(bw.buffer, map[string]interface{}{"id": 1, "name": "Alice"})
	bw.mu.Unlock()

	assert.NoError(t, bw.Flush())
	assert.NoError(t, bw.Close())
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestBatchWriter_WriteBatch_PlainInsertMode 全量同步 + enable_drop_table_before_ddl=true 时，
// 应发出普通 INSERT INTO（无 IGNORE，无 ON DUPLICATE KEY UPDATE）。
func TestBatchWriter_WriteBatch_PlainInsertMode(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	identity := &entity.TableIdentity{
		TableName:    "users",
		Strategy:     entity.PKStrategy,
		HasPK:        true,
		Columns:      []entity.ColumnMeta{{Name: "id", IsPrimaryKey: true}, {Name: "name"}},
		IdentifyCols: []string{"id"},
	}

	mock.ExpectExec("INSERT INTO `users` (`id`, `name`) VALUES (?, ?)").
		WithArgs(1, "Alice").
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := NewBatchWriter(db, identity, 100)
	w.EnablePlainInsert()
	err = w.WriteBatch(context.Background(), []map[string]interface{}{{"id": 1, "name": "Alice"}})
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestBatchWriter_WriteBatch_PlainInsertOverridesUpsert usePlainInsert 优先级高于 useUpsert。
func TestBatchWriter_WriteBatch_PlainInsertOverridesUpsert(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	identity := &entity.TableIdentity{
		TableName:    "users",
		Strategy:     entity.PKStrategy,
		HasPK:        true,
		Columns:      []entity.ColumnMeta{{Name: "id", IsPrimaryKey: true}, {Name: "name"}},
		IdentifyCols: []string{"id"},
	}

	// 即使同时启用了 upsert，plain insert 应优先生效
	mock.ExpectExec("INSERT INTO `users` (`id`, `name`) VALUES (?, ?)").
		WithArgs(1, "Alice").
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := NewBatchWriter(db, identity, 100)
	w.EnableUpsert()
	w.EnablePlainInsert()
	err = w.WriteBatch(context.Background(), []map[string]interface{}{{"id": 1, "name": "Alice"}})
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}
