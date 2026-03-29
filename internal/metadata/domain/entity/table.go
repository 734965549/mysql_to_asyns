package entity // 声明当前文件属于entity包，用于定义数据实体

// IdentityStrategy 标识策略类型
type IdentityStrategy string // 定义标识策略为字符串类型

const ( // 定义常量
	PKStrategy          IdentityStrategy = "PK_STRATEGY" // 主键策略：使用主键作为标识
	UKStrategy          IdentityStrategy = "UK_STRATEGY" // 唯一键策略：使用唯一键作为标识
	FullColumnsStrategy IdentityStrategy = "FULL_COLUMNS_STRATEGY" // 全列匹配策略：使用所有列作为标识
)

// TableIdentity 表标识信息 - 值对象
type TableIdentity struct { // 定义表标识结构体，用于唯一标识一条记录
	TableName    string           // 表名
	Strategy     IdentityStrategy // 标识策略：使用何种方式标识记录
	IdentifyCols []string         // 标识列集合：用于标识记录的列名
	Columns      []ColumnMeta     // 所有列信息：表的所有列的元数据
	HasPK        bool             // 是否有主键：表是否存在主键
	HasUK        bool             // 是否有唯一键：表是否存在唯一键
}

// ColumnMeta 列元数据
type ColumnMeta struct { // 定义列元数据结构体，存储列的详细信息
	Name         string // 列名：列的名称
	DataType     string // 数据类型：列的数据类型
	IsNullable   bool   // 是否可空：列是否允许为空值
	IsPrimaryKey bool   // 是否主键：列是否为主键
	IsUnique     bool   // 是否唯一：列是否有唯一约束
	DefaultValue string // 默认值：列的默认值
}

// TableInfo 表信息
type TableInfo struct { // 定义表信息结构体，存储表的基本信息
	Schema        string `json:"schema"`          // 数据库名：表所属的数据库名
	TableName     string `json:"table_name"`      // 表名：表的名称
	TableRowCount int64  `json:"table_row_count"` // 行数：表中的记录总数
}
