package entity // 声明当前文件属于entity包，用于定义数据实体

import "strings"

// IdentityStrategy 标识策略类型
type IdentityStrategy string // 定义标识策略为字符串类型

const ( // 定义常量
	PKStrategy          IdentityStrategy = "PK_STRATEGY"           // 主键策略：使用主键作为标识
	UKStrategy          IdentityStrategy = "UK_STRATEGY"           // 唯一键策略：使用唯一键作为标识
	FullColumnsStrategy IdentityStrategy = "FULL_COLUMNS_STRATEGY" // 全列匹配策略：使用所有列作为标识
)

// TableIdentity 表标识信息 - 值对象
// TableIdentity is the metadata decision shared by readers and writers.
//
// IdentifyCols are used to match target rows for UPDATE/DELETE. CursorCols are
// used by full-sync readers for keyset/range pagination and can differ from
// IdentifyCols when a composite key contains an auto-increment column.
type TableIdentity struct { // 定义表标识结构体，用于唯一标识一条记录
	TableName    string           // 表名
	Strategy     IdentityStrategy // 标识策略：使用何种方式标识记录
	IdentifyCols []string         // 标识列集合：用于标识记录的列名（UPDATE/DELETE 匹配）
	CursorCols   []string         // 游标/分片列：全量读取分页用；复合主键含自增列时仅含该自增列
	Columns      []ColumnMeta     // 所有列信息：表的所有列的元数据
	HasPK        bool             // 是否有主键：表是否存在主键
	HasUK        bool             // 是否有唯一键：表是否存在唯一键
}

// EffectiveCursorCols 返回用于分页/分片的游标列。
// 复合主键含自增列时仅返回该自增列；否则与 IdentifyCols 一致。
func (t *TableIdentity) EffectiveCursorCols() []string {
	if t == nil {
		return nil
	}
	if len(t.CursorCols) > 0 {
		return t.CursorCols
	}
	return t.IdentifyCols
}

// GeneratedKind MySQL 生成列类型（information_schema.COLUMNS.EXTRA）。
type GeneratedKind string

const (
	GeneratedNone    GeneratedKind = ""
	GeneratedVirtual GeneratedKind = "VIRTUAL"
	GeneratedStored  GeneratedKind = "STORED"
)

// ParseGeneratedKindFromExtra 从 COLUMNS.EXTRA 解析生成列类型。
func ParseGeneratedKindFromExtra(extra string) GeneratedKind {
	lower := strings.ToLower(extra)
	switch {
	case strings.Contains(lower, "virtual generated"):
		return GeneratedVirtual
	case strings.Contains(lower, "stored generated"):
		return GeneratedStored
	default:
		return GeneratedNone
	}
}

// ColumnMeta 列元数据
type ColumnMeta struct { // 定义列元数据结构体，存储列的详细信息
	Name            string        // 列名：列的名称
	DataType        string        // 数据类型：列的数据类型
	IsNullable      bool          // 是否可空：列是否允许为空值
	IsPrimaryKey    bool          // 是否主键：列是否为主键
	IsUnique        bool          // 是否唯一：列是否有唯一约束
	IsAutoIncrement bool          // 是否自增：EXTRA 含 auto_increment
	GeneratedKind   GeneratedKind // 生成列类型；非生成列为空
	DefaultValue    string        // 默认值：列的默认值
}

// IsGenerated 是否为 MySQL 生成列（VIRTUAL / STORED）。
func (c ColumnMeta) IsGenerated() bool {
	return c.GeneratedKind != GeneratedNone
}

// IsWritable 是否可在 INSERT/UPDATE SET 中显式赋值（生成列只能由目标库表达式计算）。
func (c ColumnMeta) IsWritable() bool {
	return !c.IsGenerated()
}

// WritableColumns 返回可写入列子集，保留 TableIdentity.Columns 完整元数据供匹配与 binlog 映射。
func WritableColumns(columns []ColumnMeta) []ColumnMeta {
	if len(columns) == 0 {
		return nil
	}
	out := make([]ColumnMeta, 0, len(columns))
	for _, col := range columns {
		if col.IsWritable() {
			out = append(out, col)
		}
	}
	return out
}

// WritableColumnCount 返回可写入列数量。
func (t *TableIdentity) WritableColumnCount() int {
	if t == nil {
		return 0
	}
	n := 0
	for _, col := range t.Columns {
		if col.IsWritable() {
			n++
		}
	}
	return n
}

// TableInfo 表信息
type TableInfo struct { // 定义表信息结构体，存储表的基本信息
	Schema        string `json:"schema"`          // 数据库名：表所属的数据库名
	TableName     string `json:"table_name"`      // 表名：表的名称
	TableRowCount int64  `json:"table_row_count"` // 行数：表中的记录总数
}
