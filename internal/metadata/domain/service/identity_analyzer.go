package service

import (
	"mysql-to-async/internal/metadata/domain/entity"
)

// IdentityAnalyzer 标识分析器接口
type IdentityAnalyzer interface {
	// AnalyzeTable 分析表结构，确定标识策略
	AnalyzeTable(schema, tableName string) (*entity.TableIdentity, error)
	// GetAllTables 获取所有表
	GetAllTables(schema string) ([]entity.TableInfo, error)
	// GetAllDatabases 获取所有数据库列表
	GetAllDatabases() ([]string, error)
}

// IdentityAnalyzerService 标识分析器服务实现
type IdentityAnalyzerService struct {
	repo TableMetadataRepository
}

// NewIdentityAnalyzerService 创建标识分析器服务
func NewIdentityAnalyzerService(repo TableMetadataRepository) *IdentityAnalyzerService {
	return &IdentityAnalyzerService{repo: repo}
}

// AnalyzeTable 分析表结构，确定标识策略
func (s *IdentityAnalyzerService) AnalyzeTable(schema, tableName string) (*entity.TableIdentity, error) {
	// 获取表的列信息
	columns, err := s.repo.GetTableColumns(schema, tableName)
	if err != nil {
		return nil, err
	}

	// 获取主键列
	pkColumns, err := s.repo.GetPrimaryKeyColumns(schema, tableName)
	if err != nil {
		return nil, err
	}

	// 获取唯一键列
	ukColumns, err := s.repo.GetUniqueKeyColumns(schema, tableName)
	if err != nil {
		return nil, err
	}

	identity := &entity.TableIdentity{
		TableName: tableName,
		Columns:   columns,
	}

	// 确定标识策略
	if len(pkColumns) > 0 {
		identity.Strategy = entity.PKStrategy
		identity.IdentifyCols = pkColumns
		identity.HasPK = true
	} else if len(ukColumns) > 0 {
		identity.Strategy = entity.UKStrategy
		identity.IdentifyCols = ukColumns
		identity.HasUK = true
	} else {
		// 无主键，使用全列匹配
		identity.Strategy = entity.FullColumnsStrategy
		identity.HasPK = false
		identity.HasUK = false
		// 所有列作为标识列
		for _, col := range columns {
			identity.IdentifyCols = append(identity.IdentifyCols, col.Name)
		}
	}

	return identity, nil
}

// GetAllTables 获取所有表
func (s *IdentityAnalyzerService) GetAllTables(schema string) ([]entity.TableInfo, error) {
	return s.repo.GetAllTables(schema)
}

// GetAllDatabases 获取所有数据库列表
func (s *IdentityAnalyzerService) GetAllDatabases() ([]string, error) {
	return s.repo.GetAllDatabases()
}

// TableMetadataRepository 表元数据仓库接口
type TableMetadataRepository interface {
	GetTableColumns(schema, tableName string) ([]entity.ColumnMeta, error)
	GetPrimaryKeyColumns(schema, tableName string) ([]string, error)
	GetUniqueKeyColumns(schema, tableName string) ([]string, error)
	GetAllTables(schema string) ([]entity.TableInfo, error)
	GetAllDatabases() ([]string, error)
}