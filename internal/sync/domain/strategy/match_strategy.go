package strategy

import (
	"mysql-to-async/internal/metadata/domain/entity"
)

// MatchStrategy 匹配策略接口
type MatchStrategy interface {
	// BuildWhereClause 构建WHERE条件
	BuildWhereClause(row map[string]interface{}) string
	// GetWhereArgs 获取WHERE条件参数
	GetWhereArgs(row map[string]interface{}) []interface{}
	// GetStrategyName 获取策略名称
	GetStrategyName() string
}

// PKMatchStrategy 主键匹配策略
type PKMatchStrategy struct {
	identity *entity.TableIdentity
}

// NewPKMatchStrategy 创建主键匹配策略
func NewPKMatchStrategy(identity *entity.TableIdentity) *PKMatchStrategy {
	return &PKMatchStrategy{identity: identity}
}

// BuildWhereClause 构建WHERE条件
func (s *PKMatchStrategy) BuildWhereClause(row map[string]interface{}) string {
	whereClause := ""
	for i, col := range s.identity.IdentifyCols {
		if i > 0 {
			whereClause += " AND "
		}
		whereClause += col + " = ?"
	}
	return whereClause
}

// GetWhereArgs 获取WHERE条件参数
func (s *PKMatchStrategy) GetWhereArgs(row map[string]interface{}) []interface{} {
	args := make([]interface{}, len(s.identity.IdentifyCols))
	for i, col := range s.identity.IdentifyCols {
		args[i] = row[col]
	}
	return args
}

// GetStrategyName 获取策略名称
func (s *PKMatchStrategy) GetStrategyName() string {
	return "PK_STRATEGY"
}

// FullColumnMatchStrategy 全列匹配策略（无主键表）
type FullColumnMatchStrategy struct {
	identity *entity.TableIdentity
}

// NewFullColumnMatchStrategy 创建全列匹配策略
func NewFullColumnMatchStrategy(identity *entity.TableIdentity) *FullColumnMatchStrategy {
	return &FullColumnMatchStrategy{identity: identity}
}

// BuildWhereClause 构建WHERE条件（全列匹配）
func (s *FullColumnMatchStrategy) BuildWhereClause(row map[string]interface{}) string {
	whereClause := ""
	for i, col := range s.identity.IdentifyCols {
		if i > 0 {
			whereClause += " AND "
		}
		whereClause += col + " = ?"
	}
	// 无主键表添加 LIMIT 1 保护
	return whereClause + " LIMIT 1"
}

// GetWhereArgs 获取WHERE条件参数
func (s *FullColumnMatchStrategy) GetWhereArgs(row map[string]interface{}) []interface{} {
	args := make([]interface{}, len(s.identity.IdentifyCols))
	for i, col := range s.identity.IdentifyCols {
		args[i] = row[col]
	}
	return args
}

// GetStrategyName 获取策略名称
func (s *FullColumnMatchStrategy) GetStrategyName() string {
	return "FULL_COLUMNS_STRATEGY"
}

// UKMatchStrategy 唯一键匹配策略
type UKMatchStrategy struct {
	identity *entity.TableIdentity
}

// NewUKMatchStrategy 创建唯一键匹配策略
func NewUKMatchStrategy(identity *entity.TableIdentity) *UKMatchStrategy {
	return &UKMatchStrategy{identity: identity}
}

// BuildWhereClause 构建WHERE条件
func (s *UKMatchStrategy) BuildWhereClause(row map[string]interface{}) string {
	whereClause := ""
	for i, col := range s.identity.IdentifyCols {
		if i > 0 {
			whereClause += " AND "
		}
		whereClause += col + " = ?"
	}
	return whereClause
}

// GetWhereArgs 获取WHERE条件参数
func (s *UKMatchStrategy) GetWhereArgs(row map[string]interface{}) []interface{} {
	args := make([]interface{}, len(s.identity.IdentifyCols))
	for i, col := range s.identity.IdentifyCols {
		args[i] = row[col]
	}
	return args
}

// GetStrategyName 获取策略名称
func (s *UKMatchStrategy) GetStrategyName() string {
	return "UK_STRATEGY"
}

// NewMatchStrategy 根据标识策略创建匹配策略
func NewMatchStrategy(identity *entity.TableIdentity) MatchStrategy {
	switch identity.Strategy {
	case entity.PKStrategy:
		return NewPKMatchStrategy(identity)
	case entity.UKStrategy:
		return NewUKMatchStrategy(identity)
	case entity.FullColumnsStrategy:
		return NewFullColumnMatchStrategy(identity)
	default:
		return NewFullColumnMatchStrategy(identity)
	}
}