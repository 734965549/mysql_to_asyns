package writer

import (
	"fmt"
	"mysql-to-async/internal/metadata/domain/entity"
	"mysql-to-async/internal/sync/domain/strategy"
	"strings"
)

// SQLBuilder SQL构建器
type SQLBuilder struct {
	identity      *entity.TableIdentity
	matchStrategy strategy.MatchStrategy
	schema        string
}

// NewSQLBuilder 创建SQL构建器
func NewSQLBuilder(identity *entity.TableIdentity) *SQLBuilder {
	return &SQLBuilder{
		identity:      identity,
		matchStrategy: strategy.NewMatchStrategy(identity),
	}
}

// NewSQLBuilderWithSchema 创建带schema的SQL构建器（推荐，确保数据写入正确的schema）
func NewSQLBuilderWithSchema(identity *entity.TableIdentity, schema string) *SQLBuilder {
	return &SQLBuilder{
		identity:      identity,
		matchStrategy: strategy.NewMatchStrategy(identity),
		schema:        schema,
	}
}

// tableRef 返回带schema前缀的表名引用
func (b *SQLBuilder) tableRef() string {
	if b.schema != "" {
		return fmt.Sprintf("`%s`.`%s`", b.schema, b.identity.TableName)
	}
	return "`" + b.identity.TableName + "`"
}

// BuildInsert 构建INSERT语句（使用INSERT ON DUPLICATE KEY UPDATE实现幂等，不删除数据）
func (b *SQLBuilder) BuildInsert(row map[string]interface{}) (string, []interface{}) {
	columns := make([]string, 0, len(row))
	placeholders := make([]string, 0, len(row))
	values := make([]interface{}, 0, len(row))
	updates := make([]string, 0, len(row))

	for _, col := range b.identity.Columns {
		if val, ok := row[col.Name]; ok {
			columns = append(columns, "`"+col.Name+"`")
			placeholders = append(placeholders, "?")
			values = append(values, val)
			// 非主键列才需要更新
			if !col.IsPrimaryKey {
				updates = append(updates, fmt.Sprintf("`%s` = VALUES(`%s`)", col.Name, col.Name))
			}
		}
	}

	var query string
	if len(updates) == 0 {
		query = fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES (%s)",
			b.tableRef(),
			strings.Join(columns, ", "),
			strings.Join(placeholders, ", "))
	} else {
		query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
			b.tableRef(),
			strings.Join(columns, ", "),
			strings.Join(placeholders, ", "),
			strings.Join(updates, ", "))
	}

	return query, values
}

// BuildInsertOnDuplicate 构建INSERT ON DUPLICATE KEY UPDATE语句
func (b *SQLBuilder) BuildInsertOnDuplicate(row map[string]interface{}) (string, []interface{}) {
	return b.BuildInsert(row)
}

// BuildUpdate 构建UPDATE语句
func (b *SQLBuilder) BuildUpdate(row map[string]interface{}) (string, []interface{}) {
	sets := make([]string, 0, len(row))
	values := make([]interface{}, 0, len(row))

	for _, col := range b.identity.Columns {
		if val, ok := row[col.Name]; ok && !col.IsPrimaryKey {
			sets = append(sets, fmt.Sprintf("`%s` = ?", col.Name))
			values = append(values, val)
		}
	}

	whereClause := b.matchStrategy.BuildWhereClause(row)
	whereArgs := b.matchStrategy.GetWhereArgs(row)

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		b.tableRef(),
		strings.Join(sets, ", "),
		whereClause)

	values = append(values, whereArgs...)
	return query, values
}

// BuildUpdateWithBeforeImage 构建UPDATE语句（使用 before image 作为 WHERE 条件）
func (b *SQLBuilder) BuildUpdateWithBeforeImage(row, beforeImage map[string]interface{}) (string, []interface{}) {
	sets := make([]string, 0, len(row))
	values := make([]interface{}, 0, len(row))

	for _, col := range b.identity.Columns {
		if val, ok := row[col.Name]; ok && !col.IsPrimaryKey {
			sets = append(sets, fmt.Sprintf("`%s` = ?", col.Name))
			values = append(values, val)
		}
	}

	// 使用 before image 构建 WHERE 子句
	whereClause := b.matchStrategy.BuildWhereClause(beforeImage)
	whereArgs := b.matchStrategy.GetWhereArgs(beforeImage)

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		b.tableRef(),
		strings.Join(sets, ", "),
		whereClause)

	values = append(values, whereArgs...)
	return query, values
}

// BuildDelete 构建DELETE语句
func (b *SQLBuilder) BuildDelete(row map[string]interface{}) (string, []interface{}) {
	whereClause := b.matchStrategy.BuildWhereClause(row)
	whereArgs := b.matchStrategy.GetWhereArgs(row)

	query := fmt.Sprintf("DELETE FROM %s WHERE %s", b.tableRef(), whereClause)
	return query, whereArgs
}

// BuildBatchInsert 构建批量INSERT语句（使用INSERT ON DUPLICATE KEY UPDATE，不删除数据）
func (b *SQLBuilder) BuildBatchInsert(rows []map[string]interface{}) (string, []interface{}) {
	if len(rows) == 0 {
		return "", nil
	}

	// 获取列名（加 backtick 防止关键字冲突）
	columns := make([]string, 0, len(b.identity.Columns))
	columnNames := make([]string, 0, len(b.identity.Columns))
	for _, col := range b.identity.Columns {
		columns = append(columns, "`"+col.Name+"`")
		columnNames = append(columnNames, col.Name)
	}

	// 构建值占位符
	var values []interface{}
	var rowPlaceholders []string

	for _, row := range rows {
		placeholders := make([]string, 0, len(columnNames))
		for _, colName := range columnNames {
			placeholders = append(placeholders, "?")
			values = append(values, row[colName])
		}
		rowPlaceholders = append(rowPlaceholders, "("+strings.Join(placeholders, ", ")+")")
	}

	// 构建 ON DUPLICATE KEY UPDATE 子句（只更新非主键列）
	var updates []string
	for _, col := range b.identity.Columns {
		if !col.IsPrimaryKey {
			updates = append(updates, fmt.Sprintf("`%s` = VALUES(`%s`)", col.Name, col.Name))
		}
	}

	var query string
	if len(updates) == 0 {
		// 全列均为主键时，重复插入直接忽略（不更新任何字段）
		query = fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES %s",
			b.tableRef(),
			strings.Join(columns, ", "),
			strings.Join(rowPlaceholders, ", "))
	} else {
		query = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
			b.tableRef(),
			strings.Join(columns, ", "),
			strings.Join(rowPlaceholders, ", "),
			strings.Join(updates, ", "))
	}

	return query, values
}

// GetStrategyName 获取当前策略名称
func (b *SQLBuilder) GetStrategyName() string {
	return b.matchStrategy.GetStrategyName()
}
