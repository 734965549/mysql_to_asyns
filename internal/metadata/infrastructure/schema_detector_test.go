package infrastructure

import (
	"database/sql"
	"testing"

	"mysql-to-sync/internal/metadata/domain/entity"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestNewSchemaDetector(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	detector := NewSchemaDetector(db)
	assert.NotNil(t, detector)
}

func TestSchemaDetector_GetTableColumns(t *testing.T) {
	tests := []struct {
		name      string
		schema    string
		tableName string
		setupMock func(mock sqlmock.Sqlmock)
		expectErr bool
		expected  int
	}{
		{
			name:      "成功获取表列信息",
			schema:    "test_db",
			tableName: "users",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE", "COLUMN_KEY", "COLUMN_DEFAULT", "EXTRA"}).
					AddRow("id", "int", "NO", "PRI", nil, "").
					AddRow("name", "varchar", "YES", "", nil, "").
					AddRow("email", "varchar", "NO", "UNI", nil, "")
				mock.ExpectQuery("SELECT(.+)FROM information_schema.COLUMNS").
					WithArgs("test_db", "users").
					WillReturnRows(rows)
			},
			expectErr: false,
			expected:  3,
		},
		{
			name:      "空表返回空切片",
			schema:    "test_db",
			tableName: "empty_table",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE", "COLUMN_KEY", "COLUMN_DEFAULT", "EXTRA"})
				mock.ExpectQuery("SELECT(.+)FROM information_schema.COLUMNS").
					WithArgs("test_db", "empty_table").
					WillReturnRows(rows)
			},
			expectErr: false,
			expected:  0,
		},
		{
			name:      "数据库查询错误",
			schema:    "test_db",
			tableName: "error_table",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT(.+)FROM information_schema.COLUMNS").
					WithArgs("test_db", "error_table").
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
			detector := NewSchemaDetector(db)
			columns, err := detector.GetTableColumns(tt.schema, tt.tableName)

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, columns, tt.expected)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestSchemaDetector_GetPrimaryKeyColumns(t *testing.T) {
	tests := []struct {
		name      string
		schema    string
		tableName string
		setupMock func(mock sqlmock.Sqlmock)
		expectErr bool
		expected  []string
	}{
		{
			name:      "成功获取主键列",
			schema:    "test_db",
			tableName: "users",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"COLUMN_NAME"}).
					AddRow("id")
				mock.ExpectQuery("SELECT(.+)FROM information_schema.KEY_COLUMN_USAGE").
					WithArgs("test_db", "users").
					WillReturnRows(rows)
			},
			expectErr: false,
			expected:  []string{"id"},
		},
		{
			name:      "复合主键",
			schema:    "test_db",
			tableName: "order_items",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"COLUMN_NAME"}).
					AddRow("order_id").
					AddRow("item_id")
				mock.ExpectQuery("SELECT(.+)FROM information_schema.KEY_COLUMN_USAGE").
					WithArgs("test_db", "order_items").
					WillReturnRows(rows)
			},
			expectErr: false,
			expected:  []string{"order_id", "item_id"},
		},
		{
			name:      "无主键返回空切片",
			schema:    "test_db",
			tableName: "no_pk_table",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"COLUMN_NAME"})
				mock.ExpectQuery("SELECT(.+)FROM information_schema.KEY_COLUMN_USAGE").
					WithArgs("test_db", "no_pk_table").
					WillReturnRows(rows)
			},
			expectErr: false,
			expected:  nil,
		},
		{
			name:      "数据库查询错误",
			schema:    "test_db",
			tableName: "error_table",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT(.+)FROM information_schema.KEY_COLUMN_USAGE").
					WithArgs("test_db", "error_table").
					WillReturnError(sql.ErrConnDone)
			},
			expectErr: true,
			expected:  nil,
		},
		{
			name:      "遍历中途连接断开返回错误",
			schema:    "test_db",
			tableName: "conn_drop_pk",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"COLUMN_NAME"}).
					AddRow("order_id").
					AddRow("item_id")
				rows.RowError(1, sql.ErrConnDone)
				mock.ExpectQuery("SELECT(.+)FROM information_schema.KEY_COLUMN_USAGE").
					WithArgs("test_db", "conn_drop_pk").
					WillReturnRows(rows)
			},
			expectErr: true,
			expected:  nil,
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
			detector := NewSchemaDetector(db)
			pkColumns, err := detector.GetPrimaryKeyColumns(tt.schema, tt.tableName)

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, pkColumns)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestSchemaDetector_GetUniqueKeyColumns(t *testing.T) {
	tests := []struct {
		name      string
		schema    string
		tableName string
		setupMock func(mock sqlmock.Sqlmock)
		expectErr bool
		expected  [][]string
	}{
		{
			name:      "成功获取唯一键列",
			schema:    "test_db",
			tableName: "users",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"INDEX_NAME", "COLUMN_NAME", "IS_NULLABLE"}).
					AddRow("uk_email", "email", "NO")
				mock.ExpectQuery("SELECT(.+)FROM information_schema.STATISTICS").
					WithArgs("test_db", "users").
					WillReturnRows(rows)
			},
			expectErr: false,
			expected:  [][]string{{"email"}},
		},
		{
			name:      "多个唯一索引按 INDEX_NAME 分组",
			schema:    "test_db",
			tableName: "multi_uk",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"INDEX_NAME", "COLUMN_NAME", "IS_NULLABLE"}).
					AddRow("uk_a", "col_a", "NO").
					AddRow("uk_b", "col_b", "NO").
					AddRow("uk_b", "col_c", "NO")
				mock.ExpectQuery("SELECT(.+)FROM information_schema.STATISTICS").
					WithArgs("test_db", "multi_uk").
					WillReturnRows(rows)
			},
			expectErr: false,
			expected:  [][]string{{"col_a"}, {"col_b", "col_c"}},
		},
		{
			name:      "排除含 nullable 列的唯一索引",
			schema:    "test_db",
			tableName: "nullable_uk",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"INDEX_NAME", "COLUMN_NAME", "IS_NULLABLE"}).
					AddRow("uk_nullable", "email", "YES").
					AddRow("uk_safe", "phone", "NO")
				mock.ExpectQuery("SELECT(.+)FROM information_schema.STATISTICS").
					WithArgs("test_db", "nullable_uk").
					WillReturnRows(rows)
			},
			expectErr: false,
			expected:  [][]string{{"phone"}},
		},
		{
			name:      "无唯一键返回空切片",
			schema:    "test_db",
			tableName: "no_uk_table",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"INDEX_NAME", "COLUMN_NAME", "IS_NULLABLE"})
				mock.ExpectQuery("SELECT(.+)FROM information_schema.STATISTICS").
					WithArgs("test_db", "no_uk_table").
					WillReturnRows(rows)
			},
			expectErr: false,
			expected:  nil,
		},
		{
			name:      "数据库查询错误",
			schema:    "test_db",
			tableName: "error_table",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT(.+)FROM information_schema.STATISTICS").
					WithArgs("test_db", "error_table").
					WillReturnError(sql.ErrConnDone)
			},
			expectErr: true,
			expected:  nil,
		},
		{
			name:      "遍历中途连接断开返回错误",
			schema:    "test_db",
			tableName: "conn_drop_uk",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"INDEX_NAME", "COLUMN_NAME", "IS_NULLABLE"}).
					AddRow("uk_composite", "col_a", "NO").
					AddRow("uk_composite", "col_b", "NO")
				rows.RowError(1, sql.ErrConnDone)
				mock.ExpectQuery("SELECT(.+)FROM information_schema.STATISTICS").
					WithArgs("test_db", "conn_drop_uk").
					WillReturnRows(rows)
			},
			expectErr: true,
			expected:  nil,
		},
		{
			name:      "混合函数表达式列的复合唯一索引被排除",
			schema:    "test_db",
			tableName: "func_idx_mixed",
			setupMock: func(mock sqlmock.Sqlmock) {
				// UNIQUE(col_a, (LOWER(col_b))) — col_b 是表达式，COLUMN_NAME 为 NULL
				// 同时存在一个正常的单列唯一索引，应只返回后者
				rows := sqlmock.NewRows([]string{"INDEX_NAME", "COLUMN_NAME", "IS_NULLABLE"}).
					AddRow("uk_mixed", "col_a", "NO").
					AddRow("uk_mixed", nil, nil). // 表达式列：COLUMN_NAME 和 IS_NULLABLE 均为 NULL
					AddRow("uk_safe", "col_c", "NO")
				mock.ExpectQuery("SELECT(.+)FROM information_schema.STATISTICS").
					WithArgs("test_db", "func_idx_mixed").
					WillReturnRows(rows)
			},
			expectErr: false,
			expected:  [][]string{{"col_c"}},
		},
		{
			name:      "纯函数索引被排除",
			schema:    "test_db",
			tableName: "func_idx_pure",
			setupMock: func(mock sqlmock.Sqlmock) {
				// UNIQUE((LOWER(email))) — 所有 key part 都是表达式
				rows := sqlmock.NewRows([]string{"INDEX_NAME", "COLUMN_NAME", "IS_NULLABLE"}).
					AddRow("uk_func", nil, nil)
				mock.ExpectQuery("SELECT(.+)FROM information_schema.STATISTICS").
					WithArgs("test_db", "func_idx_pure").
					WillReturnRows(rows)
			},
			expectErr: false,
			expected:  nil,
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
			detector := NewSchemaDetector(db)
			ukColumns, err := detector.GetUniqueKeyColumns(tt.schema, tt.tableName)

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, ukColumns)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestSchemaDetector_GetAllTables(t *testing.T) {
	tests := []struct {
		name      string
		schema    string
		setupMock func(mock sqlmock.Sqlmock)
		expectErr bool
		expected  []entity.TableInfo
	}{
		{
			name:   "成功获取所有表",
			schema: "test_db",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_ROWS"}).
					AddRow("users", 100).
					AddRow("orders", 500)
				mock.ExpectQuery("SELECT(.+)FROM information_schema.TABLES").
					WithArgs("test_db").
					WillReturnRows(rows)
			},
			expectErr: false,
			expected: []entity.TableInfo{
				{Schema: "test_db", TableName: "users", TableRowCount: 100},
				{Schema: "test_db", TableName: "orders", TableRowCount: 500},
			},
		},
		{
			name:   "空数据库返回空切片",
			schema: "empty_db",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_ROWS"})
				mock.ExpectQuery("SELECT(.+)FROM information_schema.TABLES").
					WithArgs("empty_db").
					WillReturnRows(rows)
			},
			expectErr: false,
			expected:  nil,
		},
		{
			name:   "数据库查询错误",
			schema: "error_db",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT(.+)FROM information_schema.TABLES").
					WithArgs("error_db").
					WillReturnError(sql.ErrConnDone)
			},
			expectErr: true,
			expected:  nil,
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
			detector := NewSchemaDetector(db)
			tables, err := detector.GetAllTables(tt.schema)

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, tables)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestSchemaDetector_CheckBinlogRowImage(t *testing.T) {
	tests := []struct {
		name      string
		setupMock func(mock sqlmock.Sqlmock)
		expectErr bool
		expected  string
	}{
		{
			name: "成功获取binlog_row_image设置",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
					AddRow("binlog_row_image", "FULL")
				mock.ExpectQuery("SHOW VARIABLES LIKE 'binlog_row_image'").
					WillReturnRows(rows)
			},
			expectErr: false,
			expected:  "FULL",
		},
		{
			name: "查询错误",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW VARIABLES LIKE 'binlog_row_image'").
					WillReturnError(sql.ErrConnDone)
			},
			expectErr: true,
			expected:  "",
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
			detector := NewSchemaDetector(db)
			value, err := detector.CheckBinlogRowImage()

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, value)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestSchemaDetector_GetAllDatabases(t *testing.T) {
	tests := []struct {
		name      string
		setupMock func(mock sqlmock.Sqlmock)
		expectErr bool
		expected  []string
	}{
		{
			name: "成功获取所有数据库",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"SCHEMA_NAME"}).
					AddRow("app_db").
					AddRow("test_db")
				mock.ExpectQuery("SELECT(.+)FROM information_schema.SCHEMATA").
					WillReturnRows(rows)
			},
			expectErr: false,
			expected:  []string{"app_db", "test_db"},
		},
		{
			name: "无数据库返回空切片",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"SCHEMA_NAME"})
				mock.ExpectQuery("SELECT(.+)FROM information_schema.SCHEMATA").
					WillReturnRows(rows)
			},
			expectErr: false,
			expected:  nil,
		},
		{
			name: "数据库查询错误",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT(.+)FROM information_schema.SCHEMATA").
					WillReturnError(sql.ErrConnDone)
			},
			expectErr: true,
			expected:  nil,
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
			detector := NewSchemaDetector(db)
			databases, err := detector.GetAllDatabases()

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, databases)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestGetDSN(t *testing.T) {
	tests := []struct {
		name     string
		host     string
		port     int
		username string
		password string
		database string
		expected string
	}{
		{
			name:     "标准DSN",
			host:     "localhost",
			port:     3306,
			username: "root",
			password: "password",
			database: "test_db",
			expected: "root:password@tcp(localhost:3306)/test_db?charset=utf8mb4&parseTime=True&loc=Local",
		},
		{
			name:     "自定义端口",
			host:     "192.168.1.100",
			port:     3307,
			username: "admin",
			password: "secret",
			database: "production",
			expected: "admin:secret@tcp(192.168.1.100:3307)/production?charset=utf8mb4&parseTime=True&loc=Local",
		},
		{
			name:     "空密码",
			host:     "127.0.0.1",
			port:     3306,
			username: "user",
			password: "",
			database: "mydb",
			expected: "user:@tcp(127.0.0.1:3306)/mydb?charset=utf8mb4&parseTime=True&loc=Local",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn := GetDSN(tt.host, tt.port, tt.username, tt.password, tt.database)
			assert.Equal(t, tt.expected, dsn)
		})
	}
}

func TestSchemaDetector_ColumnMetaFields(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	// 测试列的各种属性
	rows := sqlmock.NewRows([]string{"COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE", "COLUMN_KEY", "COLUMN_DEFAULT", "EXTRA"}).
		AddRow("id", "int", "NO", "PRI", nil, "auto_increment").
		AddRow("name", "varchar", "YES", "", "default_name", "").
		AddRow("email", "varchar", "NO", "UNI", nil, "").
		AddRow("created_at", "datetime", "YES", "", "CURRENT_TIMESTAMP", "")

	mock.ExpectQuery("SELECT(.+)FROM information_schema.COLUMNS").
		WithArgs("test_db", "test_table").
		WillReturnRows(rows)

	detector := NewSchemaDetector(db)
	columns, err := detector.GetTableColumns("test_db", "test_table")

	assert.NoError(t, err)
	assert.Len(t, columns, 4)

	// 验证第一列（主键）
	assert.Equal(t, "id", columns[0].Name)
	assert.Equal(t, "int", columns[0].DataType)
	assert.False(t, columns[0].IsNullable)
	assert.True(t, columns[0].IsPrimaryKey)
	assert.True(t, columns[0].IsAutoIncrement)
	assert.False(t, columns[0].IsUnique)

	// 验证第二列（可空，有默认值）
	assert.Equal(t, "name", columns[1].Name)
	assert.True(t, columns[1].IsNullable)
	assert.False(t, columns[1].IsPrimaryKey)
	assert.False(t, columns[1].IsUnique)
	assert.Equal(t, "default_name", columns[1].DefaultValue)

	// 验证第三列（唯一键）
	assert.Equal(t, "email", columns[2].Name)
	assert.False(t, columns[2].IsNullable)
	assert.False(t, columns[2].IsPrimaryKey)
	assert.True(t, columns[2].IsUnique)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSchemaDetector_GeneratedColumns(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE", "COLUMN_KEY", "COLUMN_DEFAULT", "EXTRA"}).
		AddRow("id", "bigint", "NO", "PRI", nil, "").
		AddRow("sign_no", "varchar", "YES", "", nil, "").
		AddRow("active_sign_no", "varchar", "YES", "", nil, "VIRTUAL GENERATED").
		AddRow("hash_col", "varchar", "YES", "", nil, "STORED GENERATED")

	mock.ExpectQuery("SELECT(.+)FROM information_schema.COLUMNS").
		WithArgs("jg_cps", "cps_quick_sign_contract").
		WillReturnRows(rows)

	detector := NewSchemaDetector(db)
	columns, err := detector.GetTableColumns("jg_cps", "cps_quick_sign_contract")

	assert.NoError(t, err)
	assert.Len(t, columns, 4)
	assert.Equal(t, entity.GeneratedNone, columns[0].GeneratedKind)
	assert.Equal(t, entity.GeneratedVirtual, columns[2].GeneratedKind)
	assert.True(t, columns[2].IsGenerated())
	assert.False(t, columns[2].IsWritable())
	assert.Equal(t, entity.GeneratedStored, columns[3].GeneratedKind)
	assert.NoError(t, mock.ExpectationsWereMet())
}
