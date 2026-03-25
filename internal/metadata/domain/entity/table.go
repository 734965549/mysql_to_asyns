package entity

// IdentityStrategy 标识策略类型
type IdentityStrategy string

const (
	PKStrategy          IdentityStrategy = "PK_STRATEGY"          // 主键策略
	UKStrategy          IdentityStrategy = "UK_STRATEGY"          // 唯一键策略
	FullColumnsStrategy IdentityStrategy = "FULL_COLUMNS_STRATEGY" // 全列匹配策略
)

// TableIdentity 表标识信息 - 值对象
type TableIdentity struct {
	TableName    string           // 表名
	Strategy     IdentityStrategy // 标识策略
	IdentifyCols []string         // 标识列集合
	Columns      []ColumnMeta     // 所有列信息
	HasPK        bool             // 是否有主键
	HasUK        bool             // 是否有唯一键
}

// ColumnMeta 列元数据
type ColumnMeta struct {
	Name         string // 列名
	DataType     string // 数据类型
	IsNullable   bool   // 是否可空
	IsPrimaryKey bool   // 是否主键
	IsUnique     bool   // 是否唯一
	DefaultValue string // 默认值
}

// TableInfo 表信息
type TableInfo struct {
	Schema        string `json:"schema"`          // 数据库名
	TableName     string `json:"table_name"`      // 表名
	TableRowCount int64  `json:"table_row_count"` // 行数
}
