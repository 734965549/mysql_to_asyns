package fullload

import "strings"

// RowBatch 是全量快速路径的固定结构批次，替代增量路径的 map 行模型。
//
// 列顺序在分析表结构后固定（Columns），Rows 按同一顺序存放值，写入器不再为每行
// 创建列名 map，也不再从 map 二次查找主键。增量同步仍使用现有事件 map 与
// before-image，不与全量快速路径混用。
type RowBatch struct {
	Schema       string   // 源库名（进度标记用）
	Table        string   // 源表名
	TargetSchema string   // 目标库名（写入用）
	TargetTable  string   // 目标表名
	Columns      []string // 固定列顺序（带写入的列名，无反引号）
	Rows         [][]any  // 行数据，每行长度等于 len(Columns)
	StartKey     []any    // 本批起始游标值（排查用，可为空）
	EndKey       []any    // 本批结束游标值（游标推进用）
	ApproxBytes  int64    // 估算字节数（背压与提交频率用）
	ChunkID      string   // 所属 chunk 标识
	AttemptID    int      // 表级重试序号（P2.2）；0 表示不启用重试校验
	StagingTable string   // P2.3: staging 表名（非空时 writer 写此表而非 TargetTable）；为空表示直写最终表
}

// logicalWindowRows 源端单次查询的逻辑行窗口（SQL LIMIT），固定为 batch_size。
func logicalWindowRows(opt Options) int {
	if opt.BatchRows < 1 {
		return defaultBatchRows
	}
	return opt.BatchRows
}

// tableQueueKey 用于公平写队列的表标识（源库.源表）。
func tableQueueKey(schema, table string) string {
	return schema + "." + table
}

// estimateRowBytes 估算单行字节数。
func estimateRowBytes(row []any) int64 {
	var n int64
	for _, v := range row {
		n += estimateValueBytes(v)
	}
	return n
}

// estimateRowsBytes 估算多行总字节数。
func estimateRowsBytes(rows [][]any) int64 {
	var n int64
	for _, row := range rows {
		n += estimateRowBytes(row)
	}
	return n
}

// estimateValueBytes 估算单个值的传输字节数。
func estimateValueBytes(v any) int64 {
	switch x := v.(type) {
	case nil:
		return 1
	case []byte:
		return int64(len(x)) + 2
	case string:
		return int64(len(x)) + 2
	default:
		// 数值/时间/布尔等定长类型统一按 8 字节估算。
		return 8
	}
}

// normalizeColumnDataType 去掉括号参数、unsigned 等后缀，便于宽列判定。
func normalizeColumnDataType(dataType string) string {
	dt := strings.ToLower(strings.TrimSpace(dataType))
	if i := strings.IndexByte(dt, '('); i >= 0 {
		dt = dt[:i]
	}
	dt = strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(dt, " unsigned"), " zerofill"))
	return dt
}

// isLargeColumnType 判定是否为 JSON/BLOB/TEXT 等大列类型。
func isLargeColumnType(dataType string) bool {
	switch normalizeColumnDataType(dataType) {
	case "json", "text", "mediumtext", "longtext",
		"blob", "tinyblob", "mediumblob", "longblob":
		return true
	default:
		return false
	}
}

// hasLargeColumnTypes 判定表是否含大列（用于宽表两阶段读自动启用）。
func hasLargeColumnTypes(spec *TableSpec) bool {
	if spec == nil || spec.Identity == nil {
		return false
	}
	for _, col := range spec.Identity.Columns {
		if isLargeColumnType(col.DataType) {
			return true
		}
	}
	return false
}
