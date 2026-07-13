package reader

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"mysql-to-sync/internal/metadata/domain/entity"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestNewCursorReader(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	identity := &entity.TableIdentity{
		Strategy:     entity.FullColumnsStrategy,
		Columns:      []entity.ColumnMeta{{Name: "id"}, {Name: "name"}},
		IdentifyCols: []string{},
	}

	reader := NewCursorReader(db, "test_db", "users", identity)
	assert.NotNil(t, reader)
	assert.Equal(t, "test_db", reader.schema)
	assert.Equal(t, "users", reader.table)
}

func TestCursorReader_ReadBatch(t *testing.T) {
	tests := []struct {
		name      string
		offset    int64
		limit     int64
		identity  *entity.TableIdentity
		setupMock func(mock sqlmock.Sqlmock)
		expectErr bool
		expected  int
	}{
		{
			name:   "成功读取数据",
			offset: 0,
			limit:  10,
			identity: &entity.TableIdentity{
				Strategy:     entity.FullColumnsStrategy,
				Columns:      []entity.ColumnMeta{{Name: "id"}, {Name: "name"}},
				IdentifyCols: []string{},
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "name"}).
					AddRow(1, "Alice").
					AddRow(2, "Bob")
				mock.ExpectQuery("SELECT `id`, `name` FROM `test_db`.`users`").
					WillReturnRows(rows)
			},
			expectErr: false,
			expected:  2,
		},
		{
			name:   "空结果",
			offset: 100,
			limit:  10,
			identity: &entity.TableIdentity{
				Strategy:     entity.FullColumnsStrategy,
				Columns:      []entity.ColumnMeta{{Name: "id"}},
				IdentifyCols: []string{},
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id"})
				mock.ExpectQuery("SELECT `id` FROM `test_db`.`users`").
					WillReturnRows(rows)
			},
			expectErr: false,
			expected:  0,
		},
		{
			name:   "查询错误",
			offset: 0,
			limit:  10,
			identity: &entity.TableIdentity{
				Strategy:     entity.FullColumnsStrategy,
				Columns:      []entity.ColumnMeta{{Name: "id"}},
				IdentifyCols: []string{},
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT `id` FROM `test_db`.`users`").
					WillReturnError(sql.ErrConnDone)
			},
			expectErr: true,
			expected:  0,
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
			reader := NewCursorReader(db, "test_db", "users", tt.identity)
			results, err := reader.ReadBatch(context.Background(), tt.offset, tt.limit)

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, results, tt.expected)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestNewRangeShardingReader(t *testing.T) {
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

	reader := NewRangeShardingReader(db, "test_db", "users", identity)
	assert.NotNil(t, reader)
	assert.Equal(t, "test_db", reader.schema)
	assert.Equal(t, "users", reader.table)
	assert.Equal(t, "id", reader.pkColumn)
}

func TestRangeShardingReader_ReadBatch(t *testing.T) {
	tests := []struct {
		name      string
		minID     int64
		maxID     int64
		identity  *entity.TableIdentity
		setupMock func(mock sqlmock.Sqlmock)
		expectErr bool
		expected  int
	}{
		{
			name:  "成功按范围读取",
			minID: 0,
			maxID: 100,
			identity: &entity.TableIdentity{
				Strategy:     entity.PKStrategy,
				Columns:      []entity.ColumnMeta{{Name: "id"}, {Name: "name"}},
				IdentifyCols: []string{"id"},
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "name"}).
					AddRow(1, "Alice").
					AddRow(50, "Bob")
				mock.ExpectQuery("SELECT `id`, `name` FROM `test_db`.`users` WHERE `id` >= .* AND `id` < .*").
					WithArgs(int64(0), int64(100)).
					WillReturnRows(rows)
			},
			expectErr: false,
			expected:  2,
		},
		{
			name:  "空结果",
			minID: 1000,
			maxID: 2000,
			identity: &entity.TableIdentity{
				Strategy:     entity.PKStrategy,
				Columns:      []entity.ColumnMeta{{Name: "id"}},
				IdentifyCols: []string{"id"},
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id"})
				mock.ExpectQuery("SELECT `id` FROM `test_db`.`users` WHERE `id` >= .* AND `id` < .*").
					WithArgs(int64(1000), int64(2000)).
					WillReturnRows(rows)
			},
			expectErr: false,
			expected:  0,
		},
		{
			name:  "查询错误",
			minID: 0,
			maxID: 100,
			identity: &entity.TableIdentity{
				Strategy:     entity.PKStrategy,
				Columns:      []entity.ColumnMeta{{Name: "id"}},
				IdentifyCols: []string{"id"},
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT `id` FROM `test_db`.`users` WHERE `id` >= .* AND `id` < .*").
					WithArgs(int64(0), int64(100)).
					WillReturnError(sql.ErrConnDone)
			},
			expectErr: true,
			expected:  0,
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
			reader := NewRangeShardingReader(db, "test_db", "users", tt.identity)
			results, err := reader.ReadBatch(context.Background(), tt.minID, tt.maxID)

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, results, tt.expected)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestRangeShardingReader_ReadByRange(t *testing.T) {
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

	rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "Alice")
	mock.ExpectQuery("SELECT `id`, `name` FROM `test_db`.`users` WHERE `id` >= .* AND `id` < .*").
		WithArgs(int64(0), int64(100)).
		WillReturnRows(rows)

	reader := NewRangeShardingReader(db, "test_db", "users", identity)
	results, err := reader.ReadByRange(context.Background(), 0, 100)

	assert.NoError(t, err)
	assert.Len(t, results, 1)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestNewReader(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	// 测试 FullColumnsStrategy 返回 CursorReader
	fullColsIdentity := &entity.TableIdentity{
		Strategy:     entity.FullColumnsStrategy,
		Columns:      []entity.ColumnMeta{{Name: "id"}},
		IdentifyCols: []string{},
	}
	reader := NewReader(db, "test_db", "users", fullColsIdentity)
	_, ok := reader.(*CursorReader)
	assert.True(t, ok, "Expected CursorReader for FullColumnsStrategy")

	// 测试 PKStrategy 返回 RangeShardingReader
	pkIdentity := &entity.TableIdentity{
		Strategy:     entity.PKStrategy,
		Columns:      []entity.ColumnMeta{{Name: "id"}},
		IdentifyCols: []string{"id"},
	}
	reader = NewReader(db, "test_db", "users", pkIdentity)
	_, ok = reader.(*RangeShardingReader)
	assert.True(t, ok, "Expected RangeShardingReader for PKStrategy")

	// 测试 UKStrategy 返回 RangeShardingReader
	ukIdentity := &entity.TableIdentity{
		Strategy:     entity.UKStrategy,
		Columns:      []entity.ColumnMeta{{Name: "email"}},
		IdentifyCols: []string{"email"},
	}
	reader = NewReader(db, "test_db", "users", ukIdentity)
	_, ok = reader.(*RangeShardingReader)
	assert.True(t, ok, "Expected RangeShardingReader for UKStrategy")
}

func TestSelectExprForColumn_JSON(t *testing.T) {
	col := entity.ColumnMeta{Name: "tabs", DataType: "json"}
	assert.Equal(t, "`tabs`", selectExprForColumn(col))
	assert.Equal(t, "`plain`", selectExprForColumn(entity.ColumnMeta{Name: "plain", DataType: "varchar"}))
}

func TestCursorReader_ScanRowsWithByteData(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	identity := &entity.TableIdentity{
		Strategy:     entity.FullColumnsStrategy,
		Columns:      []entity.ColumnMeta{{Name: "id"}, {Name: "data"}},
		IdentifyCols: []string{},
	}

	// 模拟返回 []byte 类型的数据
	rows := sqlmock.NewRows([]string{"id", "data"}).
		AddRow(1, []byte("binary_data"))

	mock.ExpectQuery("SELECT `id`, `data` FROM `test_db`.`users`").
		WillReturnRows(rows)

	reader := NewCursorReader(db, "test_db", "users", identity)
	results, err := reader.ReadBatch(context.Background(), 0, 10)

	assert.NoError(t, err)
	assert.Len(t, results, 1)
	// []byte 应该被转换为 string
	assert.Equal(t, "binary_data", results[0]["data"])
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestBuildCompositeKeysetWhere(t *testing.T) {
	t.Run("2列", func(t *testing.T) {
		where, args := buildCompositeKeysetWhere([]string{"id", "device_id"}, []interface{}{"8bfe", "TA00"})
		assert.Equal(t, "(`id` = ? AND `device_id` > ?) OR (`id` > ?)", where)
		assert.Equal(t, []interface{}{"8bfe", "TA00", "8bfe"}, args)
	})
	t.Run("3列", func(t *testing.T) {
		where, args := buildCompositeKeysetWhere(
			[]string{"a", "b", "c"},
			[]interface{}{1, 2, 3},
		)
		assert.Equal(t, "(`a` = ? AND `b` = ? AND `c` > ?) OR (`a` = ? AND `b` > ?) OR (`a` > ?)", where)
		assert.Equal(t, []interface{}{1, 2, 3, 1, 2, 1}, args)
	})
}

func TestRangeShardingReader_ReadBatchByKeys_CompositePK(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	identity := &entity.TableIdentity{
		Strategy:     entity.PKStrategy,
		Columns:      []entity.ColumnMeta{{Name: "id"}, {Name: "device_id"}, {Name: "payload"}},
		IdentifyCols: []string{"id", "device_id"},
		CursorCols:   []string{"id", "device_id"},
	}

	rows := sqlmock.NewRows([]string{"id", "device_id", "payload"}).
		AddRow("8bff", "TA01", "data1")
	mock.ExpectQuery("SELECT `id`, `device_id`, `payload` FROM `test_db`.`devices` WHERE .* ORDER BY `id`, `device_id` ASC LIMIT .*").
		WithArgs("8bfe", "TA00", "8bfe", int64(10)).
		WillReturnRows(rows)

	reader := NewRangeShardingReader(db, "test_db", "devices", identity)
	results, err := reader.ReadBatchByKeys(context.Background(), []interface{}{"8bfe", "TA00"}, 10)

	assert.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "TA01", results[0]["device_id"])
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRangeShardingReader_ReadBatchByKeyRange_SinglePK(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	identity := &entity.TableIdentity{
		Strategy:     entity.PKStrategy,
		Columns:      []entity.ColumnMeta{{Name: "id"}, {Name: "name"}},
		IdentifyCols: []string{"id"},
		CursorCols:   []string{"id"},
	}

	rows := sqlmock.NewRows([]string{"id", "name"}).
		AddRow(11, "alice").
		AddRow(49, "bob")
	// WHERE `id` > ? AND `id` <= ?  ->  startID=10, endID=50
	mock.ExpectQuery("SELECT `id`, `name` FROM `test_db`.`users` WHERE `id` > \\? AND `id` <= \\? ORDER BY `id` ASC LIMIT \\?").
		WithArgs(10, 50, int64(10)).
		WillReturnRows(rows)

	reader := NewRangeShardingReader(db, "test_db", "users", identity)
	results, err := reader.ReadBatchByKeyRange(context.Background(), 10, 50, 10)

	assert.NoError(t, err)
	assert.Len(t, results, 2)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRangeShardingReader_ReadBatchByKeyRange_SinglePK_FirstWorker(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	identity := &entity.TableIdentity{
		Strategy:     entity.PKStrategy,
		Columns:      []entity.ColumnMeta{{Name: "id"}, {Name: "name"}},
		IdentifyCols: []string{"id"},
		CursorCols:   []string{"id"},
	}

	rows := sqlmock.NewRows([]string{"id", "name"}).
		AddRow(1, "alice").
		AddRow(2, "bob")
	// 第一个 worker：startID=nil，只有上界 WHERE `id` <= ?
	mock.ExpectQuery("SELECT `id`, `name` FROM `test_db`.`users` WHERE `id` <= \\? ORDER BY `id` ASC LIMIT \\?").
		WithArgs(50, int64(10)).
		WillReturnRows(rows)

	reader := NewRangeShardingReader(db, "test_db", "users", identity)
	results, err := reader.ReadBatchByKeyRange(context.Background(), nil, 50, 10)

	assert.NoError(t, err)
	assert.Len(t, results, 2)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRangeShardingReader_ReadBatchByKeyRange_CompositePK(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	identity := &entity.TableIdentity{
		Strategy:     entity.PKStrategy,
		Columns:      []entity.ColumnMeta{{Name: "id"}, {Name: "device_id"}, {Name: "payload"}},
		IdentifyCols: []string{"id", "device_id"},
		CursorCols:   []string{"id", "device_id"},
	}

	rows := sqlmock.NewRows([]string{"id", "device_id", "payload"}).
		AddRow("8bff", "TA01", "data1")
	// 复合主键 2 列：下界 ("8bfe","TA00")，上界 ("9aaa","TZ99")
	// 下界 OR 分支: (id=? AND device_id>?) OR (id>?)  -> args: 8bfe, TA00, 8bfe
	// 上界 <= OR 分支: (id=? AND device_id=?) OR (id=? AND device_id<?) OR (id<?)  -> args: 9aaa, TZ99, 9aaa, TZ99, 9aaa
	mock.ExpectQuery("SELECT `id`, `device_id`, `payload` FROM `test_db`.`devices` WHERE .* AND .* ORDER BY `id`, `device_id` ASC LIMIT .*").
		WithArgs("8bfe", "TA00", "8bfe", "9aaa", "TZ99", "9aaa", "TZ99", "9aaa", int64(10)).
		WillReturnRows(rows)

	reader := NewRangeShardingReader(db, "test_db", "devices", identity)
	results, err := reader.ReadBatchByKeyRange(context.Background(), []interface{}{"8bfe", "TA00"}, []interface{}{"9aaa", "TZ99"}, 10)

	assert.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "TA01", results[0]["device_id"])
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRangeShardingReader_ReadBatchByKeyRange_NilEndDelegates(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	identity := &entity.TableIdentity{
		Strategy:     entity.PKStrategy,
		Columns:      []entity.ColumnMeta{{Name: "id"}, {Name: "name"}},
		IdentifyCols: []string{"id"},
		CursorCols:   []string{"id"},
	}

	rows := sqlmock.NewRows([]string{"id", "name"}).
		AddRow(11, "alice")
	// endID=nil → 退化为 ReadBatchByKeys，只有 WHERE `id` > ?
	mock.ExpectQuery("SELECT `id`, `name` FROM `test_db`.`users` WHERE `id` > \\? ORDER BY `id` ASC LIMIT \\?").
		WithArgs(10, int64(10)).
		WillReturnRows(rows)

	reader := NewRangeShardingReader(db, "test_db", "users", identity)
	results, err := reader.ReadBatchByKeyRange(context.Background(), 10, nil, 10)

	assert.NoError(t, err)
	assert.Len(t, results, 1)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRangeShardingReader_EmptyIdentifyCols(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	identity := &entity.TableIdentity{
		Strategy:     entity.PKStrategy,
		Columns:      []entity.ColumnMeta{{Name: "id"}},
		IdentifyCols: []string{}, // 空的 IdentifyCols
	}

	reader := NewRangeShardingReader(db, "test_db", "users", identity)
	assert.NotNil(t, reader)
	assert.Equal(t, "", reader.pkColumn) // pkColumn 应该为空
}

// TestRetryableQuery_RecoversFromCloseError 验证 scan/Close 阶段遇到 invalid connection 时能重试恢复。
// 第一次查询成功但 rows.Close() 返回 invalid connection 错误，第二次查询成功。
func TestRetryableQuery_RecoversFromCloseError(t *testing.T) {
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

	// 第一次查询：QueryContext 成功，但 rows.Close() 返回 invalid connection
	rows1 := sqlmock.NewRows([]string{"id", "name"}).
		AddRow(1, "Alice").
		CloseError(fmt.Errorf("invalid connection"))
	mock.ExpectQuery("SELECT `id`, `name` FROM `test_db`.`users` WHERE `id` >= .* AND `id` < .*").
		WithArgs(int64(0), int64(100)).
		WillReturnRows(rows1)

	// 第二次查询：完全成功
	rows2 := sqlmock.NewRows([]string{"id", "name"}).
		AddRow(1, "Alice")
	mock.ExpectQuery("SELECT `id`, `name` FROM `test_db`.`users` WHERE `id` >= .* AND `id` < .*").
		WithArgs(int64(0), int64(100)).
		WillReturnRows(rows2)

	reader := NewRangeShardingReader(db, "test_db", "users", identity)
	results, err := reader.ReadBatch(context.Background(), 0, 100)

	assert.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "Alice", results[0]["name"])
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestRetryableQuery_RecoversFromRowError 验证 rows.Scan/Next 阶段遇到 bad connection 时能重试恢复。
// 第一次查询成功但行数据返回 bad connection 错误，第二次查询成功。
func TestRetryableQuery_RecoversFromRowError(t *testing.T) {
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

	// 第一次查询：QueryContext 成功，但 RowError 返回 bad connection
	rows1 := sqlmock.NewRows([]string{"id", "name"}).
		AddRow(1, "Alice").
		RowError(0, fmt.Errorf("bad connection"))
	mock.ExpectQuery("SELECT `id`, `name` FROM `test_db`.`users` WHERE `id` >= .* AND `id` < .*").
		WithArgs(int64(0), int64(100)).
		WillReturnRows(rows1)

	// 第二次查询：完全成功
	rows2 := sqlmock.NewRows([]string{"id", "name"}).
		AddRow(1, "Alice")
	mock.ExpectQuery("SELECT `id`, `name` FROM `test_db`.`users` WHERE `id` >= .* AND `id` < .*").
		WithArgs(int64(0), int64(100)).
		WillReturnRows(rows2)

	reader := NewRangeShardingReader(db, "test_db", "users", identity)
	results, err := reader.ReadBatch(context.Background(), 0, 100)

	assert.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "Alice", results[0]["name"])
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestRetryableQuery_QueryContextRetry 验证 QueryContext 阶段的连接错误重试（原有行为保留）。
func TestRetryableQuery_QueryContextRetry(t *testing.T) {
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

	// 第一次 QueryContext 返回 invalid connection
	mock.ExpectQuery("SELECT `id`, `name` FROM `test_db`.`users` WHERE `id` >= .* AND `id` < .*").
		WithArgs(int64(0), int64(100)).
		WillReturnError(fmt.Errorf("invalid connection"))

	// 第二次 QueryContext 成功
	rows := sqlmock.NewRows([]string{"id", "name"}).
		AddRow(1, "Alice")
	mock.ExpectQuery("SELECT `id`, `name` FROM `test_db`.`users` WHERE `id` >= .* AND `id` < .*").
		WithArgs(int64(0), int64(100)).
		WillReturnRows(rows)

	reader := NewRangeShardingReader(db, "test_db", "users", identity)
	results, err := reader.ReadBatch(context.Background(), 0, 100)

	assert.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "Alice", results[0]["name"])
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestRetryableQuery_NonRetryableError 验证非连接错误不重试，直接返回。
func TestRetryableQuery_NonRetryableError(t *testing.T) {
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

	// 非连接错误（如语法错误），不应重试
	mock.ExpectQuery("SELECT `id`, `name` FROM `test_db`.`users` WHERE `id` >= .* AND `id` < .*").
		WithArgs(int64(0), int64(100)).
		WillReturnError(fmt.Errorf("syntax error"))

	reader := NewRangeShardingReader(db, "test_db", "users", identity)
	_, err = reader.ReadBatch(context.Background(), 0, 100)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "syntax error")
	assert.NoError(t, mock.ExpectationsWereMet())
}
