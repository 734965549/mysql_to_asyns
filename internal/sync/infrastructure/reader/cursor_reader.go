package reader // 声明当前文件属于reader包，用于数据读取

import ( // 导入外部包和标准库
	"context"                                       // 导入context包，用于上下文管理
	"database/sql"                                  // 导入database/sql包，用于数据库操作
	"database/sql/driver"                           // 导入driver包，用于驱动接口
	"errors"                                        // 导入errors包，用于错误处理
	"fmt"                                           // 导入fmt包，用于格式化输入输出
	"mysql-to-sync/internal/metadata/domain/entity" // 导入实体包
	"strings"                                       // 导入strings包，用于字符串操作
	"time"                                          // 导入time包，用于时间处理
)

type queryExecutor interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// defaultSelectLimit 当调用方传入 limit<=0 时的兜底，避免生成无 LIMIT 的查询一次拉全表。
const defaultSelectLimit int64 = 1000 // 默认查询限制，防止一次性加载全表数据

func normalizeSelectLimit(limit int64) int64 { // 规范化查询限制函数
	if limit <= 0 { // 如果限制值小于等于0
		return defaultSelectLimit // 返回默认限制值
	}
	return limit // 否则返回传入的限制值
}

func isConnRetryable(err error) bool { // 判断连接错误是否可重试函数
	if err == nil { // 如果没有错误
		return false // 不可重试
	}
	if errors.Is(err, driver.ErrBadConn) { // 如果是错误连接
		return true // 可重试
	}
	s := strings.ToLower(err.Error())                   // 转换为小写
	return strings.Contains(s, "invalid connection") || // 检查是否包含无效连接
		strings.Contains(s, "bad connection") || // 检查是否包含错误连接
		strings.Contains(s, "unexpected packet") || // 检查是否包含意外包
		strings.Contains(s, "connection was bad") // 检查是否包含连接错误
}

// retryableQuery 先直接查询；在 QueryContext 或 scan/Close 阶段遇到连接失效等可重试错误时进入重试循环。
// 正常路径无闭包分配和循环开销。
func retryableQuery(ctx context.Context, db queryExecutor, query string, scan func(*sql.Rows) ([]map[string]interface{}, error), args ...interface{}) ([]map[string]interface{}, error) {
	const maxRetries = 3

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(25+attempt*25) * time.Millisecond):
			}
		}

		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			if isConnRetryable(err) && attempt < maxRetries {
				continue
			}
			return nil, err
		}

		results, sErr := scan(rows)
		if cErr := rows.Close(); cErr != nil && sErr == nil {
			sErr = cErr
		}

		if sErr != nil && isConnRetryable(sErr) && attempt < maxRetries {
			continue
		}

		return results, sErr
	}

	return nil, fmt.Errorf("retryableQuery: max retries exceeded")
}

// selectExprForColumn 构建 SELECT 列表项。JSON/BLOB/TEXT 等原样读取，避免 CAST 在服务端物化整段大字段拖慢 IO；Scan 后经 normalizeScannedValue 处理 []byte。
func selectExprForColumn(col entity.ColumnMeta) string { // 构建SELECT列表达式函数
	q := "`" + strings.ReplaceAll(col.Name, "`", "``") + "`" // 处理列名中的反引号
	return q                                                 // 返回列表达式
}

// normalizeScannedValue 将 Scan 结果放入 map：对 []byte 先拷贝再转为 string，避免 database/sql 与驱动复用底层缓冲区导致跨行串数据，并与历史写入行为一致。
func normalizeScannedValue(val interface{}) interface{} { // 规范化扫描值函数
	if val == nil { // 如果值为空
		return nil // 返回空
	}
	if b, ok := val.([]byte); ok { // 如果是字节数组
		// string([]byte) 会分配新内存并拷贝，生成不可变 string，不受 driver 底层缓冲复用影响。
		// 不需要先 make+copy 再 string，避免重复拷贝。
		return string(b)
	}
	return val // 返回原值
}

// DataReader 数据读取器接口
// DataReader is the full-sync source read contract.
//
// Implementations return source rows as column-name maps. They do not advance
// task resume cursors; TaskService records cursors only after the corresponding
// target write transaction commits.
type DataReader interface { // 定义数据读取器接口
	// ReadBatch 批量读取数据方法
	ReadBatch(ctx context.Context, offset, limit int64) ([]map[string]interface{}, error) // 批量读取数据
	// ReadBatchByKeys 批量读取数据（基于主键范围，优化深分页）方法
	ReadBatchByKeys(ctx context.Context, lastID interface{}, limit int64) ([]map[string]interface{}, error) // 基于主键批量读取
	// GetTotalCount 获取总行数方法（精确，可能慢）
	GetTotalCount(ctx context.Context) (int64, error) // 获取总行数
	// GetEstimatedCount 通过 information_schema 快速获取估算行数（毫秒级，有误差）
	GetEstimatedCount(ctx context.Context) (int64, error) // 获取估算行数
}

// CursorReader 无主键表流式读取器（单次全表扫描，避免 OFFSET 深翻页）
type CursorReader struct { // 定义无主键表流式读取器结构体
	db       queryExecutor         // 数据库连接
	schema   string                // 数据库schema
	table    string                // 表名
	identity *entity.TableIdentity // 表标识信息
	rows     *sql.Rows             // 流式游标，第一次 ReadBatch 时打开
	colNames []string              // 列名缓存
	done     bool                  // 标记游标已耗尽，防止重复打开
}

// NewCursorReader 创建流式读取器函数
func NewCursorReader(db queryExecutor, schema, table string, identity *entity.TableIdentity) *CursorReader { // 创建流式读取器实例
	return &CursorReader{ // 返回读取器实例
		db:       db,       // 设置数据库连接
		schema:   schema,   // 设置schema
		table:    table,    // 设置表名
		identity: identity, // 设置表标识
	}
}

// ReadBatch 批量读取数据方法（流式：第一次调用打开游标，后续调用继续从游标读取）
// offset 参数在流式模式下忽略（已由游标位置隐含）
func (r *CursorReader) ReadBatch(ctx context.Context, _ /* offset */, limit int64) ([]map[string]interface{}, error) { // 批量读取数据
	if r.done { // 游标已耗尽，直接返回空结果
		return nil, nil
	}
	limit = normalizeSelectLimit(limit) // 规范化限制值
	if r.rows == nil {                  // 如果游标未打开
		var colParts []string                    // 创建列部分列表
		for _, col := range r.identity.Columns { // 遍历所有列
			colParts = append(colParts, selectExprForColumn(col)) // 添加列表达式
		}
		// 不加 LIMIT：游标通过 Next() 逐行消费，批量大小由外层 limit 参数控制
		query := fmt.Sprintf("SELECT %s FROM `%s`.`%s`", strings.Join(colParts, ", "), r.schema, r.table) // 构建全表查询
		rows, err := r.db.QueryContext(ctx, query)                                                        // 执行查询
		if err != nil {                                                                                   // 如果查询失败
			return nil, fmt.Errorf("打开流式游标失败: %v, SQL: %s", err, query) // 返回错误
		}
		r.rows = rows                    // 保存游标
		r.colNames, err = rows.Columns() // 获取列名
		if err != nil {                  // 如果获取失败
			return nil, err // 返回错误
		}
	}

	var results []map[string]interface{} // 创建结果列表
	for i := int64(0); i < limit; i++ {  // 循环读取指定数量
		if !r.rows.Next() { // 如果没有更多行
			if err := r.rows.Err(); err != nil { // 如果有错误
				return nil, err // 返回错误
			}
			_ = r.rows.Close() // 关闭游标
			r.rows = nil       // 清空游标
			r.done = true      // 标记已完成，防止重新打开游标
			break              // 退出循环
		}
		values := make([]interface{}, len(r.colNames))    // 创建值列表
		valuePtrs := make([]interface{}, len(r.colNames)) // 创建值指针列表
		for j := range values {                           // 遍历所有值
			valuePtrs[j] = &values[j] // 设置指针
		}
		if err := r.rows.Scan(valuePtrs...); err != nil { // 扫描行数据
			return nil, err // 返回错误
		}
		row := make(map[string]interface{}) // 创建行映射
		for j, col := range r.colNames {    // 遍历所有列名
			row[col] = normalizeScannedValue(values[j]) // 规范化值并添加到映射
		}
		results = append(results, row) // 添加到结果列表
	}
	return results, nil // 返回结果
}

// ReadBatchByKeys 批量读取数据方法（无主键表不支持，回退到 ReadBatch）
func (r *CursorReader) ReadBatchByKeys(ctx context.Context, lastID interface{}, limit int64) ([]map[string]interface{}, error) { // 基于主键批量读取
	return nil, fmt.Errorf("ReadBatchByKeys not supported for no-PK tables") // 返回不支持错误
}

// GetTotalCount 获取总行数方法
func (r *CursorReader) GetTotalCount(ctx context.Context) (int64, error) { // 获取表的总行数
	var count int64                                                           // 定义计数变量
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", r.schema, r.table) // 构建查询
	err := r.db.QueryRowContext(ctx, query).Scan(&count)                      // 执行查询
	return count, err                                                         // 返回计数和错误
}

// GetEstimatedCount 通过 information_schema 快速获取估算行数
func (r *CursorReader) GetEstimatedCount(ctx context.Context) (int64, error) {
	var count int64
	query := "SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	err := r.db.QueryRowContext(ctx, query, r.schema, r.table).Scan(&count)
	return count, err
}

// scanRows 扫描行数据方法
func (r *CursorReader) scanRows(rows *sql.Rows) ([]map[string]interface{}, error) { // 扫描行数据
	return scanRowsToMaps(rows) // 调用扫描函数
}

// RangeShardingReader 有主键/唯一键表分片读取器（支持单列和复合主键）
type RangeShardingReader struct { // 定义范围分片读取器结构体
	db       queryExecutor         // 数据库连接
	schema   string                // 数据库schema
	table    string                // 表名
	identity *entity.TableIdentity // 表标识信息
	pkColumn string                // 兼容保留，单列时使用
}

// NewRangeShardingReader 创建分片读取器函数
func NewRangeShardingReader(db queryExecutor, schema, table string, identity *entity.TableIdentity) *RangeShardingReader { // 创建分片读取器实例
	pkColumn := "" // 初始化主键列
	cursorCols := identity.EffectiveCursorCols()
	if len(cursorCols) > 0 { // 如果有游标列
		pkColumn = cursorCols[0] // 使用第一个游标列
	}
	return &RangeShardingReader{ // 返回读取器实例
		db:       db,       // 设置数据库连接
		schema:   schema,   // 设置schema
		table:    table,    // 设置表名
		identity: identity, // 设置表标识
		pkColumn: pkColumn, // 设置主键列
	}
}

// ReadBatch 批量读取数据方法（按范围）
func (r *RangeShardingReader) ReadBatch(ctx context.Context, minID, maxID int64) ([]map[string]interface{}, error) { // 按范围批量读取
	var colParts []string                    // 创建列部分列表
	for _, col := range r.identity.Columns { // 遍历所有列
		colParts = append(colParts, selectExprForColumn(col)) // 添加列表达式
	}
	columns := strings.Join(colParts, ", ") // 连接列名

	query := fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE `%s` >= ? AND `%s` < ?", // 构建查询
		columns, r.schema, r.table, r.pkColumn, r.pkColumn) // 查询指定范围
	return retryableQuery(ctx, r.db, query, r.scanRows, minID, maxID)
}

// ReadBatchByKeys 批量读取数据方法（Keyset Pagination，支持单列和复合主键）
func (r *RangeShardingReader) ReadBatchByKeys(ctx context.Context, lastID interface{}, limit int64) ([]map[string]interface{}, error) { // 基于主键批量读取
	limit = normalizeSelectLimit(limit)      // 规范化限制值
	var colParts []string                    // 创建列部分列表
	for _, col := range r.identity.Columns { // 遍历所有列
		colParts = append(colParts, selectExprForColumn(col)) // 添加列表达式
	}
	columns := strings.Join(colParts, ", ") // 连接列名

	pkCols := r.identity.EffectiveCursorCols() // 获取游标/分片列

	var query string       // 定义查询语句
	var args []interface{} // 定义参数列表

	if len(pkCols) == 1 { // 如果是单列主键
		// 单列主键：WHERE pk > ? ORDER BY pk LIMIT ?
		if lastID == nil { // 如果没有上次ID
			query = fmt.Sprintf("SELECT %s FROM `%s`.`%s` ORDER BY `%s` ASC LIMIT ?", // 构建查询
				columns, r.schema, r.table, pkCols[0]) // 排序并限制
			args = []interface{}{limit} // 设置参数
		} else { // 如果有上次ID
			query = fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE `%s` > ? ORDER BY `%s` ASC LIMIT ?", // 构建查询
				columns, r.schema, r.table, pkCols[0], pkCols[0]) // 筛选大于上次ID的记录
			args = []interface{}{lastID, limit} // 设置参数
		}
	} else { // 否则是复合主键
		// 复合主键：将 (pk1,...,pkN) > (v1,...,vN) 展开为 OR 表达式，
		// 便于 MySQL 优化器按联合主键各列走索引（CDC 常用写法）。
		var bkCols []string          // 创建备份列列表
		for _, col := range pkCols { // 遍历主键列
			bkCols = append(bkCols, "`"+col+"`") // 添加列引用
		}
		colList := strings.Join(bkCols, ", ") // 连接列名

		if lastID == nil { // 如果没有上次ID
			query = fmt.Sprintf("SELECT %s FROM `%s`.`%s` ORDER BY %s ASC LIMIT ?", // 构建查询
				columns, r.schema, r.table, colList) // 排序并限制
			args = []interface{}{limit} // 设置参数
		} else if lastIDs, ok := lastID.([]interface{}); ok && len(lastIDs) == len(pkCols) { // 完整复合主键值
			whereClause, whereArgs := buildCompositeKeysetWhere(pkCols, lastIDs)
			query = fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE %s ORDER BY %s ASC LIMIT ?", // 构建查询
				columns, r.schema, r.table, whereClause, colList)
			args = append(whereArgs, limit)
		} else { // 单值：仅按第一主键列过滤（并行采样边界起始定位）
			query = fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE `%s` > ? ORDER BY %s ASC LIMIT ?", // 构建查询
				columns, r.schema, r.table, pkCols[0], colList) // 按首列定位
			args = []interface{}{lastID, limit} // 设置参数
		}
	}

	return retryableQuery(ctx, r.db, query, r.scanRows, args...)
}

// ReadBatchInRange 批量读取数据方法（指定范围内，且带 LIMIT）
func (r *RangeShardingReader) ReadBatchInRange(ctx context.Context, startID, endID, limit int64) ([]map[string]interface{}, error) { // 在指定范围内批量读取
	// 必须带 LIMIT：limit 由任务 batch_size 驱动；<=0 时兜底，避免 WHERE 区间内一次扫出全部行。
	limit = normalizeSelectLimit(limit)      // 规范化限制值
	var colParts []string                    // 创建列部分列表
	for _, col := range r.identity.Columns { // 遍历所有列
		colParts = append(colParts, selectExprForColumn(col)) // 添加列表达式
	}
	columns := strings.Join(colParts, ", ") // 连接列名

	query := fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE `%s` >= ? AND `%s` < ? ORDER BY `%s` ASC LIMIT ?", // 构建查询
		columns, r.schema, r.table, r.pkColumn, r.pkColumn, r.pkColumn) // 查询指定范围
	return retryableQuery(ctx, r.db, query, r.scanRows, startID, endID, limit)
}

// ReadByRange 按范围读取方法
func (r *RangeShardingReader) ReadByRange(ctx context.Context, startID, endID int64) ([]map[string]interface{}, error) { // 按范围读取
	return r.ReadBatch(ctx, startID, endID) // 调用批量读取方法
}

// OpenRangeStream 在指定连接上打开一次覆盖整个范围的流式查询。
// 调用方负责调用 rows.Close()。
// 使用独立连接而非连接池，避免多 worker 并发时连接池竞争和源库 I/O 抖动。
func (r *RangeShardingReader) OpenRangeStream(conn *sql.Conn, ctx context.Context, minID, maxID int64) (*sql.Rows, []string, error) { // 打开范围流式查询
	var colParts []string                    // 创建列部分列表
	for _, col := range r.identity.Columns { // 遍历所有列
		colParts = append(colParts, selectExprForColumn(col)) // 添加列表达式
	}
	columns := strings.Join(colParts, ", ") // 连接列名
	query := fmt.Sprintf(                   // 构建查询
		"SELECT %s FROM `%s`.`%s` WHERE `%s` >= ? AND `%s` < ? ORDER BY `%s` ASC", // 流式查询指定范围
		columns, r.schema, r.table, r.pkColumn, r.pkColumn, r.pkColumn, // 查询参数
	)
	rows, err := conn.QueryContext(ctx, query, minID, maxID) // 执行查询
	if err != nil {                                          // 如果查询失败
		return nil, nil, err // 返回错误
	}
	cols, err := rows.Columns() // 获取列名
	if err != nil {             // 如果获取失败
		rows.Close()         // 关闭结果集
		return nil, nil, err // 返回错误
	}
	return rows, cols, nil // 返回结果集和列名
}

// GetTotalCount 获取总行数方法
func (r *RangeShardingReader) GetTotalCount(ctx context.Context) (int64, error) { // 获取表的总行数
	var count int64                                                           // 定义计数变量
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", r.schema, r.table) // 构建查询
	err := r.db.QueryRowContext(ctx, query).Scan(&count)                      // 执行查询
	return count, err                                                         // 返回计数和错误
}

// GetEstimatedCount 通过 information_schema 快速获取估算行数
func (r *RangeShardingReader) GetEstimatedCount(ctx context.Context) (int64, error) {
	var count int64
	query := "SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	err := r.db.QueryRowContext(ctx, query, r.schema, r.table).Scan(&count)
	return count, err
}

// scanRows 扫描行数据方法
func (r *RangeShardingReader) scanRows(rows *sql.Rows) ([]map[string]interface{}, error) { // 扫描行数据
	return scanRowsToMaps(rows) // 调用扫描函数
}

func scanRowsToMaps(rows *sql.Rows) ([]map[string]interface{}, error) { // 扫描行到映射函数
	columns, err := rows.Columns() // 获取列名
	if err != nil {                // 如果获取失败
		return nil, err // 返回错误
	}
	var results []map[string]interface{} // 创建结果列表
	for rows.Next() {                    // 遍历所有行
		values := make([]interface{}, len(columns))    // 创建值列表
		valuePtrs := make([]interface{}, len(columns)) // 创建值指针列表
		for i := range values {                        // 遍历所有值
			valuePtrs[i] = &values[i] // 设置指针
		}
		if err := rows.Scan(valuePtrs...); err != nil { // 扫描行数据
			return nil, err // 返回错误
		}
		row := make(map[string]interface{}) // 创建行映射
		for i, col := range columns {       // 遍历所有列名
			row[col] = normalizeScannedValue(values[i]) // 规范化值并添加到映射
		}
		results = append(results, row) // 添加到结果列表
	}
	if err := rows.Err(); err != nil { // 检查错误
		return nil, err // 返回错误
	}
	return results, nil // 返回结果
}

// buildCompositeKeysetWhere 将复合主键元组比较 (pk1,...,pkN) > (v1,...,vN) 展开为 OR 表达式。
// 2 列示例：(pk1 = ? AND pk2 > ?) OR (pk1 > ?)
// N 列通用：(pk1=v1 AND ... AND pk_{k-1}=v_{k-1} AND pk_k > vk) OR ... OR (pk1 > v1)
func buildCompositeKeysetWhere(pkCols []string, lastIDs []interface{}) (string, []interface{}) {
	n := len(pkCols)
	var branches []string
	var args []interface{}
	for k := n; k >= 1; k-- {
		var conds []string
		for j := 0; j < k-1; j++ {
			conds = append(conds, fmt.Sprintf("`%s` = ?", pkCols[j]))
			args = append(args, lastIDs[j])
		}
		conds = append(conds, fmt.Sprintf("`%s` > ?", pkCols[k-1]))
		args = append(args, lastIDs[k-1])
		branches = append(branches, "("+strings.Join(conds, " AND ")+")")
	}
	return strings.Join(branches, " OR "), args
}

// NewReader 根据表标识创建合适的读取器函数
func NewReader(db queryExecutor, schema, table string, identity *entity.TableIdentity) DataReader { // 根据表标识创建读取器
	if identity.Strategy == entity.FullColumnsStrategy { // 如果是全列匹配策略
		return NewCursorReader(db, schema, table, identity) // 返回流式读取器
	}
	return NewRangeShardingReader(db, schema, table, identity) // 返回范围分片读取器
}
