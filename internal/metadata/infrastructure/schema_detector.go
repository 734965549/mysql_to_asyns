package infrastructure

import (
	"database/sql"
	"fmt"
	"mysql-to-async/internal/metadata/domain/entity"
	"mysql-to-async/internal/metadata/domain/service"
)

// SchemaDetector Schema探测器实现
type SchemaDetector struct {
	db *sql.DB
}

// NewSchemaDetector 创建Schema探测器
func NewSchemaDetector(db *sql.DB) *SchemaDetector {
	return &SchemaDetector{db: db}
}

// GetTableColumns 获取表的列信息
func (d *SchemaDetector) GetTableColumns(schema, tableName string) ([]entity.ColumnMeta, error) {
	query := `
		SELECT 
			COLUMN_NAME,
			DATA_TYPE,
			IS_NULLABLE,
			COLUMN_KEY,
			COLUMN_DEFAULT
		FROM information_schema.COLUMNS 
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`

	rows, err := d.db.Query(query, schema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []entity.ColumnMeta
	for rows.Next() {
		var col entity.ColumnMeta
		var isNullable, columnKey string
		var defaultValue sql.NullString

		err := rows.Scan(&col.Name, &col.DataType, &isNullable, &columnKey, &defaultValue)
		if err != nil {
			return nil, err
		}

		col.IsNullable = isNullable == "YES"
		col.IsPrimaryKey = columnKey == "PRI"
		col.IsUnique = columnKey == "UNI"
		if defaultValue.Valid {
			col.DefaultValue = defaultValue.String
		}

		columns = append(columns, col)
	}

	return columns, nil
}

// GetPrimaryKeyColumns 获取主键列
func (d *SchemaDetector) GetPrimaryKeyColumns(schema, tableName string) ([]string, error) {
	query := `
		SELECT COLUMN_NAME
		FROM information_schema.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
		ORDER BY ORDINAL_POSITION
	`

	rows, err := d.db.Query(query, schema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pkColumns []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, err
		}
		pkColumns = append(pkColumns, colName)
	}

	return pkColumns, nil
}

// GetUniqueKeyColumns 获取唯一键列
func (d *SchemaDetector) GetUniqueKeyColumns(schema, tableName string) ([]string, error) {
	query := `
		SELECT COLUMN_NAME
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? 
		  AND NON_UNIQUE = 0
		  AND INDEX_NAME != 'PRIMARY'
		ORDER BY INDEX_NAME, SEQ_IN_INDEX
		LIMIT 1
	`

	rows, err := d.db.Query(query, schema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ukColumns []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, err
		}
		ukColumns = append(ukColumns, colName)
	}

	return ukColumns, nil
}

// GetAllTables 获取所有表
func (d *SchemaDetector) GetAllTables(schema string) ([]entity.TableInfo, error) {
	query := `
		SELECT TABLE_NAME, TABLE_ROWS
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
	`

	rows, err := d.db.Query(query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []entity.TableInfo
	for rows.Next() {
		var table entity.TableInfo
		table.Schema = schema
		if err := rows.Scan(&table.TableName, &table.TableRowCount); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return tables, nil
}

// CheckBinlogRowImage 检查binlog_row_image设置
func (d *SchemaDetector) CheckBinlogRowImage() (string, error) {
	var value string
	err := d.db.QueryRow("SHOW VARIABLES LIKE 'binlog_row_image'").Scan(&value, &value)
	if err != nil {
		return "", err
	}
	return value, nil
}

// GetAllDatabases 获取所有数据库列表
func (d *SchemaDetector) GetAllDatabases() ([]string, error) {
	query := `
		SELECT SCHEMA_NAME
		FROM information_schema.SCHEMATA
		WHERE SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
		ORDER BY SCHEMA_NAME
	`

	rows, err := d.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var schemaName string
		if err := rows.Scan(&schemaName); err != nil {
			return nil, err
		}
		databases = append(databases, schemaName)
	}

	return databases, nil
}

// 确保实现了接口
var _ service.TableMetadataRepository = (*SchemaDetector)(nil)

// GetDSN 构建DSN
func GetDSN(host string, port int, username, password, database string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		username, password, host, port, database)
}