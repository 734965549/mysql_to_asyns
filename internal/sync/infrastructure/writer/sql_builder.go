package writer // 声明当前文件属于writer包，用于数据写入

import ( // 导入外部包和标准库
	"fmt"                                           // 导入fmt包，用于格式化输入输出
	"mysql-to-sync/internal/metadata/domain/entity" // 导入实体包
	"mysql-to-sync/internal/sync/domain/strategy"   // 导入策略包
	"strings"                                       // 导入strings包，用于字符串操作
)

// SQLBuilder SQL构建器
type SQLBuilder struct { // 定义SQL构建器结构体
	identity      *entity.TableIdentity  // 表标识信息
	matchStrategy strategy.MatchStrategy // 匹配策略
	schema        string                 // 数据库schema
}

// NewSQLBuilder 创建SQL构建器函数
func NewSQLBuilder(identity *entity.TableIdentity) *SQLBuilder { // 创建SQL构建器实例
	return &SQLBuilder{ // 返回构建器实例
		identity:      identity,                            // 设置表标识
		matchStrategy: strategy.NewMatchStrategy(identity), // 创建匹配策略
	}
}

// NewSQLBuilderWithSchema 创建带schema的SQL构建器函数（推荐，确保数据写入正确的schema）
func NewSQLBuilderWithSchema(identity *entity.TableIdentity, schema string) *SQLBuilder { // 创建带schema的SQL构建器实例
	return &SQLBuilder{ // 返回构建器实例
		identity:      identity,                            // 设置表标识
		matchStrategy: strategy.NewMatchStrategy(identity), // 创建匹配策略
		schema:        schema,                              // 设置schema
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
	columns := make([]string, 0, len(row))      // 创建列名列表
	placeholders := make([]string, 0, len(row)) // 创建占位符列表
	values := make([]interface{}, 0, len(row))  // 创建值列表
	updates := make([]string, 0, len(row))      // 创建更新列表

	for _, col := range b.identity.Columns { // 遍历所有列
		if val, ok := row[col.Name]; ok { // 如果列存在值
			columns = append(columns, "`"+col.Name+"`") // 添加列名
			placeholders = append(placeholders, "?")    // 添加占位符
			values = append(values, val)                // 添加值
			// 非主键列才需要更新
			if !col.IsPrimaryKey { // 如果不是主键列
				updates = append(updates, fmt.Sprintf("`%s` = VALUES(`%s`)", col.Name, col.Name)) // 添加更新语句
			}
		}
	}

	var query string       // 定义查询语句
	if len(updates) == 0 { // 如果没有需要更新的列（全是主键）
		query = fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES (%s)", // 使用INSERT IGNORE
			b.tableRef(),                     // 表引用
			strings.Join(columns, ", "),      // 列名
			strings.Join(placeholders, ", ")) // 占位符
	} else { // 否则
		query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s", // 使用INSERT ON DUPLICATE KEY UPDATE
			b.tableRef(),                     // 表引用
			strings.Join(columns, ", "),      // 列名
			strings.Join(placeholders, ", "), // 占位符
			strings.Join(updates, ", "))      // 更新子句
	}

	return query, values // 返回查询语句和参数
}

// BuildInsertOnDuplicate 构建INSERT ON DUPLICATE KEY UPDATE语句方法
func (b *SQLBuilder) BuildInsertOnDuplicate(row map[string]interface{}) (string, []interface{}) { // 构建INSERT ON DUPLICATE KEY UPDATE语句
	return b.BuildInsert(row) // 调用BuildInsert方法
}

// BuildUpdate 构建UPDATE语句方法
func (b *SQLBuilder) BuildUpdate(row map[string]interface{}) (string, []interface{}) { // 构建UPDATE语句
	sets := make([]string, 0, len(row))        // 创建SET子句列表
	values := make([]interface{}, 0, len(row)) // 创建值列表

	for _, col := range b.identity.Columns { // 遍历所有列
		if val, ok := row[col.Name]; ok && !col.IsPrimaryKey { // 如果列存在值且不是主键
			sets = append(sets, fmt.Sprintf("`%s` = ?", col.Name)) // 添加SET子句
			values = append(values, val)                           // 添加值
		}
	}

	whereClause := b.matchStrategy.BuildWhereClause(row) // 构建WHERE子句
	whereArgs := b.matchStrategy.GetWhereArgs(row)       // 获取WHERE参数

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", // 构建UPDATE语句
		b.tableRef(),             // 表引用
		strings.Join(sets, ", "), // SET子句
		whereClause)              // WHERE子句

	values = append(values, whereArgs...) // 添加WHERE参数
	return query, values                  // 返回查询语句和参数
}

// BuildUpdateWithBeforeImage 构建UPDATE语句方法（使用 before image 作为 WHERE 条件）
func (b *SQLBuilder) BuildUpdateWithBeforeImage(row, beforeImage map[string]interface{}) (string, []interface{}) { // 构建使用before image的UPDATE语句
	sets := make([]string, 0, len(row))        // 创建SET子句列表
	values := make([]interface{}, 0, len(row)) // 创建值列表

	for _, col := range b.identity.Columns { // 遍历所有列
		if val, ok := row[col.Name]; ok && !col.IsPrimaryKey { // 如果列存在值且不是主键
			sets = append(sets, fmt.Sprintf("`%s` = ?", col.Name)) // 添加SET子句
			values = append(values, val)                           // 添加值
		}
	}

	// 使用 before image 构建 WHERE 子句
	whereClause := b.matchStrategy.BuildWhereClause(beforeImage) // 使用before image构建WHERE子句
	whereArgs := b.matchStrategy.GetWhereArgs(beforeImage)       // 获取WHERE参数

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", // 构建UPDATE语句
		b.tableRef(),             // 表引用
		strings.Join(sets, ", "), // SET子句
		whereClause)              // WHERE子句

	values = append(values, whereArgs...) // 添加WHERE参数
	return query, values                  // 返回查询语句和参数
}

// BuildDelete 构建DELETE语句方法
func (b *SQLBuilder) BuildDelete(row map[string]interface{}) (string, []interface{}) { // 构建DELETE语句
	whereClause := b.matchStrategy.BuildWhereClause(row) // 构建WHERE子句
	whereArgs := b.matchStrategy.GetWhereArgs(row)       // 获取WHERE参数

	query := fmt.Sprintf("DELETE FROM %s WHERE %s", b.tableRef(), whereClause) // 构建DELETE语句
	return query, whereArgs                                                    // 返回查询语句和参数
}

// BuildBatchInsert 构建批量 INSERT IGNORE 语句方法。
//
// 用途：未显式选择 plain INSERT 或 upsert 的兼容路径。对于增量重放可能反复到达
// 同一 INSERT 的场景，IGNORE 不会更新已有行，**不具有 upsert 语义**，请增量路径使用
// BuildBatchUpsert（按 strategy 分流，详见函数注释）。
func (b *SQLBuilder) BuildBatchInsert(rows []map[string]interface{}) (string, []interface{}) { // 构建批量INSERT语句
	if len(rows) == 0 { // 如果没有行数据
		return "", nil // 返回空
	}

	columns, _, values, rowPlaceholders := b.collectBatchValues(rows)

	// 默认兼容路径保留 INSERT IGNORE；全量同步路径由服务层改用 BuildBatchInsertPlain。
	query := fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES %s",
		b.tableRef(),
		strings.Join(columns, ", "),
		strings.Join(rowPlaceholders, ", "))

	return query, values
}

// BuildBatchInsertPlain 构建普通批量 INSERT 语句（无 IGNORE，无 ON DUPLICATE KEY UPDATE）。
//
// 全量同步路径统一使用：目标表必须由用户保证为空，或在同步前已被 DROP+CREATE 重建为空。
// 增量同步路径绝对不能使用此方法（幂等性依赖 upsert/IGNORE）。
func (b *SQLBuilder) BuildBatchInsertPlain(rows []map[string]interface{}) (string, []interface{}) {
	if len(rows) == 0 {
		return "", nil
	}

	columns, _, values, rowPlaceholders := b.collectBatchValues(rows)

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		b.tableRef(),
		strings.Join(columns, ", "),
		strings.Join(rowPlaceholders, ", "))

	return query, values
}

// BuildBatchUpsert 构建批量 upsert 语句方法。
//
// 修复 10/11：增量阶段的 INSERT 事件必须能在"重复到达"时仍把目标表收敛到与源库一致的状态。
//   - 有主键或唯一键的表（PKStrategy / UKStrategy）：使用 `INSERT ... ON DUPLICATE KEY UPDATE 非键列 = VALUES(...)`，
//     保证后到的 INSERT/UPDATE 事件会覆盖之前的旧值；
//   - 无主键、无唯一键的表（FullColumnsStrategy）：MySQL 没有可识别的"冲突键"，
//     ON DUPLICATE KEY UPDATE 退化为普通 INSERT，会插入重复行；此时回退到 `INSERT IGNORE`
//     与 BuildBatchInsert 行为一致，并由 sync 服务侧在事件入口打出 [NoPK] 告警。
func (b *SQLBuilder) BuildBatchUpsert(rows []map[string]interface{}) (string, []interface{}) {
	if len(rows) == 0 {
		return "", nil
	}

	columns, _, values, rowPlaceholders := b.collectBatchValues(rows)

	// 无主键 / 无唯一键：没有可识别的冲突键，ON DUPLICATE 没有意义，退化为 INSERT IGNORE
	if b.identity == nil || !b.identity.HasPK && !b.identity.HasUK {
		query := fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES %s",
			b.tableRef(),
			strings.Join(columns, ", "),
			strings.Join(rowPlaceholders, ", "))
		return query, values
	}

	// 有 PK/UK：拼 ON DUPLICATE KEY UPDATE 子句，只更新非主键列（避免 PK 列自我覆盖）
	updates := make([]string, 0, len(b.identity.Columns))
	for _, col := range b.identity.Columns {
		if col.IsPrimaryKey {
			continue
		}
		updates = append(updates, fmt.Sprintf("`%s` = VALUES(`%s`)", col.Name, col.Name))
	}

	if len(updates) == 0 {
		// 全部列都是主键（极少见，例如关联表）：等价于 INSERT IGNORE
		query := fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES %s",
			b.tableRef(),
			strings.Join(columns, ", "),
			strings.Join(rowPlaceholders, ", "))
		return query, values
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
		b.tableRef(),
		strings.Join(columns, ", "),
		strings.Join(rowPlaceholders, ", "),
		strings.Join(updates, ", "))

	return query, values
}

// collectBatchValues 抽取 BuildBatchInsert / BuildBatchUpsert 公共的列名与值占位符构造逻辑。
// 返回：带 backtick 的列引用、裸列名、扁平化的参数列表、每行的"(?, ?, ...)" 占位符。
func (b *SQLBuilder) collectBatchValues(rows []map[string]interface{}) ([]string, []string, []interface{}, []string) {
	columns := make([]string, 0, len(b.identity.Columns))
	columnNames := make([]string, 0, len(b.identity.Columns))
	for _, col := range b.identity.Columns {
		columns = append(columns, "`"+col.Name+"`")
		columnNames = append(columnNames, col.Name)
	}

	var values []interface{}
	rowPlaceholders := make([]string, 0, len(rows))

	for _, row := range rows {
		placeholders := make([]string, 0, len(columnNames))
		for _, colName := range columnNames {
			placeholders = append(placeholders, "?")
			values = append(values, row[colName])
		}
		rowPlaceholders = append(rowPlaceholders, "("+strings.Join(placeholders, ", ")+")")
	}

	return columns, columnNames, values, rowPlaceholders
}

// IdentityChanged 比较 before/after image 中标识列（PK/UK）的值是否发生变化。
// 返回 true 表示至少有一个标识列值不同，调用方应在同一事务内先 DELETE 旧行再 UPSERT 新行。
func (b *SQLBuilder) IdentityChanged(before, after map[string]interface{}) bool {
	for _, col := range b.identity.IdentifyCols {
		beforeVal, beforeOk := before[col]
		afterVal, afterOk := after[col]
		if !beforeOk || !afterOk {
			if beforeOk != afterOk {
				return true
			}
			continue
		}
		if fmt.Sprintf("%v", beforeVal) != fmt.Sprintf("%v", afterVal) {
			return true
		}
	}
	return false
}

// GetStrategyName 获取当前策略名称方法
func (b *SQLBuilder) GetStrategyName() string { // 获取当前使用的匹配策略名称
	return b.matchStrategy.GetStrategyName() // 返回策略名称
}
