package service // 声明当前文件属于service包，用于业务服务层

import ( // 导入外部包和标准库
	"mysql-to-sync/internal/metadata/domain/entity" // 导入实体包
)

// IdentityAnalyzer 标识分析器接口
type IdentityAnalyzer interface { // 定义标识分析器接口
	// AnalyzeTable 分析表结构，确定标识策略方法
	AnalyzeTable(schema, tableName string) (*entity.TableIdentity, error) // 分析表结构，确定如何标识记录
	// GetAllTables 获取所有表方法
	GetAllTables(schema string) ([]entity.TableInfo, error) // 获取指定数据库的所有表信息
	// GetAllDatabases 获取所有数据库列表方法
	GetAllDatabases() ([]string, error) // 获取所有数据库列表
}

// IdentityAnalyzerService 标识分析器服务实现
type IdentityAnalyzerService struct { // 定义标识分析器服务结构体
	repo TableMetadataRepository // 表元数据仓库接口
}

// NewIdentityAnalyzerService 创建标识分析器服务函数
func NewIdentityAnalyzerService(repo TableMetadataRepository) *IdentityAnalyzerService { // 创建标识分析器服务实例
	return &IdentityAnalyzerService{repo: repo} // 返回服务实例
}

// AnalyzeTable 分析表结构，确定标识策略方法
func (s *IdentityAnalyzerService) AnalyzeTable(schema, tableName string) (*entity.TableIdentity, error) { // 分析表结构，确定如何标识记录
	// 获取表的列信息
	columns, err := s.repo.GetTableColumns(schema, tableName) // 从仓库获取表的所有列信息
	if err != nil { // 如果获取失败
		return nil, err // 返回错误
	}

	// 获取主键列
	pkColumns, err := s.repo.GetPrimaryKeyColumns(schema, tableName) // 从仓库获取主键列
	if err != nil { // 如果获取失败
		return nil, err // 返回错误
	}

	// 获取唯一键列
	ukColumns, err := s.repo.GetUniqueKeyColumns(schema, tableName) // 从仓库获取唯一键列
	if err != nil { // 如果获取失败
		return nil, err // 返回错误
	}

	identity := &entity.TableIdentity{ // 创建表标识对象
		TableName: tableName, // 设置表名
		Columns:   columns, // 设置列信息
	}

	// 确定标识策略
	if len(pkColumns) > 0 { // 如果存在主键
		identity.Strategy = entity.PKStrategy // 使用主键策略
		identity.IdentifyCols = pkColumns // 设置标识列为主键列
		identity.HasPK = true // 设置有主键标志
	} else if len(ukColumns) > 0 { // 如果存在唯一键
		identity.Strategy = entity.UKStrategy // 使用唯一键策略
		identity.IdentifyCols = ukColumns // 设置标识列为唯一键列
		identity.HasUK = true // 设置有唯一键标志
	} else { // 如果既没有主键也没有唯一键
		// 无主键，使用全列匹配
		identity.Strategy = entity.FullColumnsStrategy // 使用全列匹配策略
		identity.HasPK = false // 设置无主键标志
		identity.HasUK = false // 设置无唯一键标志
		// 所有列作为标识列
		for _, col := range columns { // 遍历所有列
			identity.IdentifyCols = append(identity.IdentifyCols, col.Name) // 将列名添加到标识列
		}
	}

	// 游标/分片列：复合主键含自增列时仅用自增列分页，否则与 IdentifyCols 一致
	identity.CursorCols = resolveCursorCols(identity.IdentifyCols, columns)

	return identity, nil // 返回表标识
}

// resolveCursorCols 根据主键/标识列与列元数据决定全量同步的分页游标键。
// 复合主键中存在 auto_increment 列时，仅使用该列作为游标；否则保留完整标识列（含元组比较）。
func resolveCursorCols(identifyCols []string, columns []entity.ColumnMeta) []string {
	if len(identifyCols) <= 1 {
		return append([]string(nil), identifyCols...)
	}
	colByName := make(map[string]entity.ColumnMeta, len(columns))
	for _, col := range columns {
		colByName[col.Name] = col
	}
	for _, name := range identifyCols {
		if col, ok := colByName[name]; ok && col.IsAutoIncrement {
			return []string{name}
		}
	}
	return append([]string(nil), identifyCols...)
}

// GetAllTables 获取所有表方法
func (s *IdentityAnalyzerService) GetAllTables(schema string) ([]entity.TableInfo, error) { // 获取指定数据库的所有表
	return s.repo.GetAllTables(schema) // 调用仓库方法获取所有表
}

// GetAllDatabases 获取所有数据库列表方法
func (s *IdentityAnalyzerService) GetAllDatabases() ([]string, error) { // 获取所有数据库
	return s.repo.GetAllDatabases() // 调用仓库方法获取所有数据库
}

// TableMetadataRepository 表元数据仓库接口
type TableMetadataRepository interface { // 定义表元数据仓库接口
	GetTableColumns(schema, tableName string) ([]entity.ColumnMeta, error) // 获取表的所有列
	GetPrimaryKeyColumns(schema, tableName string) ([]string, error) // 获取主键列名
	GetUniqueKeyColumns(schema, tableName string) ([]string, error) // 获取唯一键列名
	GetAllTables(schema string) ([]entity.TableInfo, error) // 获取所有表信息
	GetAllDatabases() ([]string, error) // 获取所有数据库名
}
