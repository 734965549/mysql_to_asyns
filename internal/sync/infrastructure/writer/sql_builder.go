package writer // 声明当前文件属于writer包，用于数据写入

import ( // 导入外部包和标准库
	"fmt" // 导入fmt包，用于格式化输入输出
	"mysql-to-async/internal/metadata/domain/entity" // 导入实体包
	"mysql-to-async/internal/sync/domain/strategy" // 导入策略包
	"strings" // 导入strings包，用于字符串操作
)

// SQLBuilder SQL构建器
type SQLBuilder struct { // 定义SQL构建器结构体
	identity      *entity.TableIdentity // 表标识信息
	matchStrategy strategy.MatchStrategy // 匹配策略
	schema        string // 数据库schema
}

// NewSQLBuilder 创建SQL构建器函数
func NewSQLBuilder(identity *entity.TableIdentity) *SQLBuilder { // 创建SQL构建器实例
	return &SQLBuilder{ // 返回构建器实例
		identity:      identity, // 设置表标识
		matchStrategy: strategy.NewMatchStrategy(identity), // 创建匹配策略
	}
}

// NewSQLBuilderWithSchema 创建带schema的SQL构建器函数（推荐，确保数据写入正确的schema）
func NewSQLBuilderWithSchema(identity *entity.TableIdentity, schema string) *SQLBuilder { // 创建带schema的SQL构建器实例
	return &SQLBuilder{ // 返回构建器实例
		identity:      identity, // 设置表标识
		matchStrategy: strategy.NewMatchStrategy(identity), // 创建匹配策略
		schema:        schema, // 设置schema
	}
}

// tableRef 返回带schema前缀的表名引用方法
func (b *SQLBuilder) tableRef() string { // 生成表引用字符串
	if b.schema != "" { // 如果设置了schema
		return fmt.Sprintf("`%s`.`%s`", b.schema, b.identity.TableName) // 返回带schema的表引用
	}
	return "`" + b.identity.TableName + "`" // 返回简单表引用
}

// BuildInsert 构建INSERT语句方法（使用INSERT ON DUPLICATE KEY UPDATE实现幂等，不删除数据）
func (b *SQLBuilder) BuildInsert(row map[string]interface{}) (string, []interface{}) { // 构建INSERT语句
	columns := make([]string, 0, len(row)) // 创建列名列表
	placeholders := make([]string, 0, len(row)) // 创建占位符列表
	values := make([]interface{}, 0, len(row)) // 创建值列表
	updates := make([]string, 0, len(row)) // 创建更新列表

	for _, col := range b.identity.Columns { // 遍历所有列
		if val, ok := row[col.Name]; ok { // 如果列存在值
			columns = append(columns, "`"+col.Name+"`") // 添加列名
			placeholders = append(placeholders, "?") // 添加占位符
			values = append(values, val) // 添加值
			// 非主键列才需要更新
			if !col.IsPrimaryKey { // 如果不是主键列
				updates = append(updates, fmt.Sprintf("`%s` = VALUES(`%s`)", col.Name, col.Name)) // 添加更新语句
			}
		}
	}

	var query string // 定义查询语句
	if len(updates) == 0 { // 如果没有需要更新的列（全是主键）
		query = fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES (%s)", // 使用INSERT IGNORE
			b.tableRef(), // 表引用
			strings.Join(columns, ", "), // 列名
			strings.Join(placeholders, ", ")) // 占位符
	} else { // 否则
		query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s", // 使用INSERT ON DUPLICATE KEY UPDATE
			b.tableRef(), // 表引用
			strings.Join(columns, ", "), // 列名
			strings.Join(placeholders, ", "), // 占位符
			strings.Join(updates, ", ")) // 更新子句
	}

	return query, values // 返回查询语句和参数
}

// BuildInsertOnDuplicate 构建INSERT ON DUPLICATE KEY UPDATE语句方法
func (b *SQLBuilder) BuildInsertOnDuplicate(row map[string]interface{}) (string, []interface{}) { // 构建INSERT ON DUPLICATE KEY UPDATE语句
	return b.BuildInsert(row) // 调用BuildInsert方法
}

// BuildUpdate 构建UPDATE语句方法
func (b *SQLBuilder) BuildUpdate(row map[string]interface{}) (string, []interface{}) { // 构建UPDATE语句
	sets := make([]string, 0, len(row)) // 创建SET子句列表
	values := make([]interface{}, 0, len(row)) // 创建值列表

	for _, col := range b.identity.Columns { // 遍历所有列
		if val, ok := row[col.Name]; ok && !col.IsPrimaryKey { // 如果列存在值且不是主键
			sets = append(sets, fmt.Sprintf("`%s` = ?", col.Name)) // 添加SET子句
			values = append(values, val) // 添加值
		}
	}

	whereClause := b.matchStrategy.BuildWhereClause(row) // 构建WHERE子句
	whereArgs := b.matchStrategy.GetWhereArgs(row) // 获取WHERE参数

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", // 构建UPDATE语句
		b.tableRef(), // 表引用
		strings.Join(sets, ", "), // SET子句
		whereClause) // WHERE子句

	values = append(values, whereArgs...) // 添加WHERE参数
	return query, values // 返回查询语句和参数
}

// BuildUpdateWithBeforeImage 构建UPDATE语句方法（使用 before image 作为 WHERE 条件）
func (b *SQLBuilder) BuildUpdateWithBeforeImage(row, beforeImage map[string]interface{}) (string, []interface{}) { // 构建使用before image的UPDATE语句
	sets := make([]string, 0, len(row)) // 创建SET子句列表
	values := make([]interface{}, 0, len(row)) // 创建值列表

	for _, col := range b.identity.Columns { // 遍历所有列
		if val, ok := row[col.Name]; ok && !col.IsPrimaryKey { // 如果列存在值且不是主键
			sets = append(sets, fmt.Sprintf("`%s` = ?", col.Name)) // 添加SET子句
			values = append(values, val) // 添加值
		}
	}

	// 使用 before image 构建 WHERE 子句
	whereClause := b.matchStrategy.BuildWhereClause(beforeImage) // 使用before image构建WHERE子句
	whereArgs := b.matchStrategy.GetWhereArgs(beforeImage) // 获取WHERE参数

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", // 构建UPDATE语句
		b.tableRef(), // 表引用
		strings.Join(sets, ", "), // SET子句
		whereClause) // WHERE子句

	values = append(values, whereArgs...) // 添加WHERE参数
	return query, values // 返回查询语句和参数
}

// BuildDelete 构建DELETE语句方法
func (b *SQLBuilder) BuildDelete(row map[string]interface{}) (string, []interface{}) { // 构建DELETE语句
	whereClause := b.matchStrategy.BuildWhereClause(row) // 构建WHERE子句
	whereArgs := b.matchStrategy.GetWhereArgs(row) // 获取WHERE参数

	query := fmt.Sprintf("DELETE FROM %s WHERE %s", b.tableRef(), whereClause) // 构建DELETE语句
	return query, whereArgs // 返回查询语句和参数
}

// BuildBatchInsert 构建批量INSERT语句方法（使用INSERT ON DUPLICATE KEY UPDATE，不删除数据）
func (b *SQLBuilder) BuildBatchInsert(rows []map[string]interface{}) (string, []interface{}) { // 构建批量INSERT语句
	if len(rows) == 0 { // 如果没有行数据
		return "", nil // 返回空
	}

	// 获取列名（加 backtick 防止关键字冲突）
	columns := make([]string, 0, len(b.identity.Columns)) // 创建列引用列表
	columnNames := make([]string, 0, len(b.identity.Columns)) // 创建列名列表
	for _, col := range b.identity.Columns { // 遍历所有列
		columns = append(columns, "`"+col.Name+"`") // 添加列引用
		columnNames = append(columnNames, col.Name) // 添加列名
	}

	// 构建值占位符
	var values []interface{} // 定义值列表
	var rowPlaceholders []string // 定义行占位符列表

	for _, row := range rows { // 遍历所有行
		placeholders := make([]string, 0, len(columnNames)) // 创建占位符列表
		for _, colName := range columnNames { // 遍历所有列名
			placeholders = append(placeholders, "?") // 添加占位符
			values = append(values, row[colName]) // 添加值
		}
		rowPlaceholders = append(rowPlaceholders, "("+strings.Join(placeholders, ", ")+")") // 添加行占位符
	}

	// 构建 ON DUPLICATE KEY UPDATE 子句（只更新非主键列）
	var updates []string // 定义更新列表
	for _, col := range b.identity.Columns { // 遍历所有列
		if !col.IsPrimaryKey { // 如果不是主键列
			updates = append(updates, fmt.Sprintf("`%s` = VALUES(`%s`)", col.Name, col.Name)) // 添加更新语句
		}
	}

	// 全量同步场景：目标表通常为空，ON DUPLICATE KEY UPDATE 的 UPDATE 步骤纯属开销。
	// 使用 INSERT IGNORE 可跳过重复键检查的写入代价，对空表与 ON DUPLICATE 行为一致。
	query := fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES %s", // 使用INSERT IGNORE
		b.tableRef(), // 表引用
		strings.Join(columns, ", "), // 列名
		strings.Join(rowPlaceholders, ", ")) // 行占位符

	return query, values // 返回查询语句和参数
}

// GetStrategyName 获取当前策略名称方法
func (b *SQLBuilder) GetStrategyName() string { // 获取当前使用的匹配策略名称
	return b.matchStrategy.GetStrategyName() // 返回策略名称
}
