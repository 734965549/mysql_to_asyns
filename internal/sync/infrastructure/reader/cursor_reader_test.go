package reader

import (
	"context"
	"database/sql"
	"testing"

	"mysql-to-async/internal/metadata/domain/entity"

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
				mock.ExpectQuery("SELECT id, name FROM test_db.users").
					WithArgs(int64(0), int64(10)).
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
				mock.ExpectQuery("SELECT id FROM test_db.users").
					WithArgs(int64(100), int64(10)).
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
				mock.ExpectQuery("SELECT id FROM test_db.users").
					WithArgs(int64(0), int64(10)).
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

func TestCursorReader_GetTotalCount(t *testing.T) {
	tests := []struct {
		name      string
		setupMock func(mock sqlmock.Sqlmock)
		expectErr bool
		expected  int64
	}{
		{
			name: "成功获取总数",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT COUNT(.*) FROM test_db.users").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(100)))
			},
			expectErr: false,
			expected:  100,
		},
		{
			name: "查询错误",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT COUNT(.*) FROM test_db.users").
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
			reader := NewCursorReader(db, "test_db", "users", &entity.TableIdentity{})
			count, err := reader.GetTotalCount(context.Background())

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, count)
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
				mock.ExpectQuery("SELECT id, name FROM test_db.users WHERE id >= .* AND id < .*").
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
				mock.ExpectQuery("SELECT id FROM test_db.users WHERE id >= .* AND id < .*").
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
				mock.ExpectQuery("SELECT id FROM test_db.users WHERE id >= .* AND id < .*").
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
	mock.ExpectQuery("SELECT id, name FROM test_db.users WHERE id >= .* AND id < .*").
		WithArgs(int64(0), int64(100)).
		WillReturnRows(rows)

	reader := NewRangeShardingReader(db, "test_db", "users", identity)
	results, err := reader.ReadByRange(context.Background(), 0, 100)

	assert.NoError(t, err)
	assert.Len(t, results, 1)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRangeShardingReader_GetTotalCount(t *testing.T) {
	tests := []struct {
		name      string
		setupMock func(mock sqlmock.Sqlmock)
		expectErr bool
		expected  int64
	}{
		{
			name: "成功获取总数",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT COUNT(.*) FROM test_db.users").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(500)))
			},
			expectErr: false,
			expected:  500,
		},
		{
			name: "查询错误",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT COUNT(.*) FROM test_db.users").
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
			reader := NewRangeShardingReader(db, "test_db", "users", &entity.TableIdentity{
				IdentifyCols: []string{"id"},
			})
			count, err := reader.GetTotalCount(context.Background())

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, count)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
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

	mock.ExpectQuery("SELECT id, data FROM test_db.users").
		WithArgs(int64(0), int64(10)).
		WillReturnRows(rows)

	reader := NewCursorReader(db, "test_db", "users", identity)
	results, err := reader.ReadBatch(context.Background(), 0, 10)

	assert.NoError(t, err)
	assert.Len(t, results, 1)
	// []byte 应该被转换为 string
	assert.Equal(t, "binary_data", results[0]["data"])
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
