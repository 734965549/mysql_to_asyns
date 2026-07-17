package fullload

// RowBatch 是全量快速路径的固定结构批次，替代增量路径的 map 行模型。
//
// 列顺序在分析表结构后固定（Columns），Rows 按同一顺序存放值，写入器不再为每行
// 创建列名 map，也不再从 map 二次查找主键。增量同步仍使用现有事件 map 与
// before-image，不与全量快速路径混用。
type RowBatch struct {
	Schema       string  // 源库名（进度标记用）
	Table        string  // 源表名
	TargetSchema string  // 目标库名（写入用）
	TargetTable  string  // 目标表名
	Columns      []string // 固定列顺序（带写入的列名，无反引号）
	Rows         [][]any  // 行数据，每行长度等于 len(Columns)
	StartKey     []any    // 本批起始游标值（排查用，可为空）
	EndKey       []any    // 本批结束游标值（游标推进用）
	ApproxBytes  int64    // 估算字节数（背压与提交阈值用）
	ChunkID      string   // 所属 chunk 标识
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
