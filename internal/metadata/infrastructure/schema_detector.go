package infrastructure // 声明当前文件属于infrastructure包，用于基础设施层

import ( // 导入外部包和标准库
	"database/sql" // 导入database/sql包，用于数据库操作
	"fmt" // 导入fmt包，用于格式化输入输出
	"mysql-to-async/internal/metadata/domain/entity" // 导入实体包
	"mysql-to-async/internal/metadata/domain/service" // 导入服务包
)

// SchemaDetector Schema探测器实现
type SchemaDetector struct { // 定义Schema探测器结构体
	db *sql.DB // 数据库连接
}

// NewSchemaDetector 创建Schema探测器函数
func NewSchemaDetector(db *sql.DB) *SchemaDetector { // 创建Schema探测器实例
	return &SchemaDetector{db: db} // 返回探测器实例
}

// GetTableColumns 获取表的列信息方法
func (d *SchemaDetector) GetTableColumns(schema, tableName string) ([]entity.ColumnMeta, error) { // 获取表的所有列信息
	query := ` // 定义SQL查询语句
		SELECT  // 查询列信息
			COLUMN_NAME, // 列名
			DATA_TYPE, // 数据类型
			IS_NULLABLE, // 是否可空
			COLUMN_KEY, // 列键类型
			COLUMN_DEFAULT // 默认值
		FROM information_schema.COLUMNS  // 从系统表查询
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? // 查询指定数据库和表的列
		ORDER BY ORDINAL_POSITION // 按列位置排序
	`

	rows, err := d.db.Query(query, schema, tableName) // 执行查询
	if err != nil { // 如果查询失败
		return nil, err // 返回错误
	}
	defer rows.Close() // 延迟关闭结果集

	var columns []entity.ColumnMeta // 定义列列表
	for rows.Next() { // 遍历结果集
		var col entity.ColumnMeta // 定义列元数据
		var isNullable, columnKey string // 定义可空性和键类型
		var defaultValue sql.NullString // 定义默认值

		err := rows.Scan(&col.Name, &col.DataType, &isNullable, &columnKey, &defaultValue) // 扫描行数据
		if err != nil { // 如果扫描失败
			return nil, err // 返回错误
		}

		col.IsNullable = isNullable == "YES" // 设置是否可空
		col.IsPrimaryKey = columnKey == "PRI" // 设置是否主键
		col.IsUnique = columnKey == "UNI" // 设置是否唯一
		if defaultValue.Valid { // 如果默认值有效
			col.DefaultValue = defaultValue.String // 设置默认值
		}

		columns = append(columns, col) // 添加到列列表
	}

	return columns, nil // 返回列列表
}

// GetPrimaryKeyColumns 获取主键列方法
func (d *SchemaDetector) GetPrimaryKeyColumns(schema, tableName string) ([]string, error) { // 获取表的主键列名
	query := ` // 定义SQL查询语句
		SELECT COLUMN_NAME // 查询列名
		FROM information_schema.KEY_COLUMN_USAGE // 从系统表查询
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' // 查询主键约束
		ORDER BY ORDINAL_POSITION // 按列位置排序
	`

	rows, err := d.db.Query(query, schema, tableName) // 执行查询
	if err != nil { // 如果查询失败
		return nil, err // 返回错误
	}
	defer rows.Close() // 延迟关闭结果集

	var pkColumns []string // 定义主键列列表
	for rows.Next() { // 遍历结果集
		var colName string // 定义列名变量
		if err := rows.Scan(&colName); err != nil { // 扫描行数据
			return nil, err // 返回错误
		}
		pkColumns = append(pkColumns, colName) // 添加到列表
	}

	return pkColumns, nil // 返回主键列列表
}

// GetUniqueKeyColumns 获取唯一键列方法
func (d *SchemaDetector) GetUniqueKeyColumns(schema, tableName string) ([]string, error) { // 获取表的唯一键列名
	query := ` // 定义SQL查询语句
		SELECT COLUMN_NAME // 查询列名
		FROM information_schema.STATISTICS // 从系统表查询
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?  // 查询指定表
		  AND NON_UNIQUE = 0 // 筛选唯一索引
		  AND INDEX_NAME != 'PRIMARY' // 排除主键索引
		ORDER BY INDEX_NAME, SEQ_IN_INDEX // 按索引名和序号排序
		LIMIT 1 // 只取第一个唯一索引
	`

	rows, err := d.db.Query(query, schema, tableName) // 执行查询
	if err != nil { // 如果查询失败
		return nil, err // 返回错误
	}
	defer rows.Close() // 延迟关闭结果集

	var ukColumns []string // 定义唯一键列列表
	for rows.Next() { // 遍历结果集
		var colName string // 定义列名变量
		if err := rows.Scan(&colName); err != nil { // 扫描行数据
			return nil, err // 返回错误
		}
		ukColumns = append(ukColumns, colName) // 添加到列表
	}

	return ukColumns, nil // 返回唯一键列列表
}

// GetAllTables 获取所有表方法
func (d *SchemaDetector) GetAllTables(schema string) ([]entity.TableInfo, error) { // 获取数据库的所有表
	query := ` // 定义SQL查询语句
		SELECT TABLE_NAME, TABLE_ROWS // 查询表名和行数
		FROM information_schema.TABLES // 从系统表查询
		WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' // 查询指定数据库的基表
	`

	rows, err := d.db.Query(query, schema) // 执行查询
	if err != nil { // 如果查询失败
		return nil, err // 返回错误
	}
	defer rows.Close() // 延迟关闭结果集

	var tables []entity.TableInfo // 定义表信息列表
	for rows.Next() { // 遍历结果集
		var table entity.TableInfo // 定义表信息
		table.Schema = schema // 设置数据库名
		if err := rows.Scan(&table.TableName, &table.TableRowCount); err != nil { // 扫描行数据
			return nil, err // 返回错误
		}
		tables = append(tables, table) // 添加到列表
	}

	return tables, nil // 返回表信息列表
}

// CheckBinlogRowImage 检查binlog_row_image设置方法
func (d *SchemaDetector) CheckBinlogRowImage() (string, error) { // 检查binlog_row_image配置
	var value string // 定义变量
	err := d.db.QueryRow("SHOW VARIABLES LIKE 'binlog_row_image'").Scan(&value, &value) // 查询binlog_row_image设置
	if err != nil { // 如果查询失败
		return "", err // 返回错误
	}
	return value, nil // 返回配置值
}

// GetAllDatabases 获取所有数据库列表方法
func (d *SchemaDetector) GetAllDatabases() ([]string, error) { // 获取所有数据库
	query := ` // 定义SQL查询语句
		SELECT SCHEMA_NAME // 查询数据库名
		FROM information_schema.SCHEMATA // 从系统表查询
		WHERE SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') // 排除系统数据库
		ORDER BY SCHEMA_NAME // 按数据库名排序
	`

	rows, err := d.db.Query(query) // 执行查询
	if err != nil { // 如果查询失败
		return nil, err // 返回错误
	}
	defer rows.Close() // 延迟关闭结果集

	var databases []string // 定义数据库列表
	for rows.Next() { // 遍历结果集
		var schemaName string // 定义数据库名变量
		if err := rows.Scan(&schemaName); err != nil { // 扫描行数据
			return nil, err // 返回错误
		}
		databases = append(databases, schemaName) // 添加到列表
	}

	return databases, nil // 返回数据库列表
}

// 确保实现了接口
var _ service.TableMetadataRepository = (*SchemaDetector)(nil) // 编译时检查接口实现

// GetDSN 构建DSN函数
func GetDSN(host string, port int, username, password, database string) string { // 构建数据库连接字符串
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local", // 格式化生成DSN
		username, password, host, port, database) // 填充参数
}
