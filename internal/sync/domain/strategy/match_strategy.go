package strategy // 声明当前文件属于strategy包，用于匹配策略

import ( // 导入外部包和标准库
	"mysql-to-async/internal/metadata/domain/entity" // 导入实体包
)

// MatchStrategy 匹配策略接口
type MatchStrategy interface { // 定义匹配策略接口
	// BuildWhereClause 构建WHERE条件方法
	BuildWhereClause(row map[string]interface{}) string // 构建SQL WHERE条件
	// GetWhereArgs 获取WHERE条件参数方法
	GetWhereArgs(row map[string]interface{}) []interface{} // 获取WHERE条件的参数值
	// GetStrategyName 获取策略名称方法
	GetStrategyName() string // 获取策略名称
}

// PKMatchStrategy 主键匹配策略
type PKMatchStrategy struct { // 定义主键匹配策略结构体
	identity *entity.TableIdentity // 表标识信息
}

// NewPKMatchStrategy 创建主键匹配策略函数
func NewPKMatchStrategy(identity *entity.TableIdentity) *PKMatchStrategy { // 创建主键匹配策略实例
	return &PKMatchStrategy{identity: identity} // 返回策略实例
}

// BuildWhereClause 构建WHERE条件方法
func (s *PKMatchStrategy) BuildWhereClause(row map[string]interface{}) string { // 构建主键WHERE条件
	whereClause := "" // 初始化WHERE条件字符串
	for i, col := range s.identity.IdentifyCols { // 遍历标识列
		if i > 0 { // 如果不是第一列
			whereClause += " AND " // 添加AND连接符
		}
		whereClause += col + " = ?" // 添加列条件
	}
	return whereClause // 返回WHERE条件
}

// GetWhereArgs 获取WHERE条件参数方法
func (s *PKMatchStrategy) GetWhereArgs(row map[string]interface{}) []interface{} { // 获取WHERE条件的参数值
	args := make([]interface{}, len(s.identity.IdentifyCols)) // 创建参数切片
	for i, col := range s.identity.IdentifyCols { // 遍历标识列
		args[i] = row[col] // 获取列的值
	}
	return args // 返回参数列表
}

// GetStrategyName 获取策略名称方法
func (s *PKMatchStrategy) GetStrategyName() string { // 获取策略名称
	return "PK_STRATEGY" // 返回主键策略名称
}

// FullColumnMatchStrategy 全列匹配策略（无主键表）
type FullColumnMatchStrategy struct { // 定义全列匹配策略结构体
	identity *entity.TableIdentity // 表标识信息
}

// NewFullColumnMatchStrategy 创建全列匹配策略函数
func NewFullColumnMatchStrategy(identity *entity.TableIdentity) *FullColumnMatchStrategy { // 创建全列匹配策略实例
	return &FullColumnMatchStrategy{identity: identity} // 返回策略实例
}

// BuildWhereClause 构建WHERE条件（全列匹配）方法
func (s *FullColumnMatchStrategy) BuildWhereClause(row map[string]interface{}) string { // 构建全列WHERE条件
	whereClause := "" // 初始化WHERE条件字符串
	for i, col := range s.identity.IdentifyCols { // 遍历标识列
		if i > 0 { // 如果不是第一列
			whereClause += " AND " // 添加AND连接符
		}
		whereClause += col + " = ?" // 添加列条件
	}
	// 无主键表添加 LIMIT 1 保护
	return whereClause + " LIMIT 1" // 添加LIMIT限制，避免更新多条记录
}

// GetWhereArgs 获取WHERE条件参数方法
func (s *FullColumnMatchStrategy) GetWhereArgs(row map[string]interface{}) []interface{} { // 获取WHERE条件的参数值
	args := make([]interface{}, len(s.identity.IdentifyCols)) // 创建参数切片
	for i, col := range s.identity.IdentifyCols { // 遍历标识列
		args[i] = row[col] // 获取列的值
	}
	return args // 返回参数列表
}

// GetStrategyName 获取策略名称方法
func (s *FullColumnMatchStrategy) GetStrategyName() string { // 获取策略名称
	return "FULL_COLUMNS_STRATEGY" // 返回全列匹配策略名称
}

// UKMatchStrategy 唯一键匹配策略
type UKMatchStrategy struct { // 定义唯一键匹配策略结构体
	identity *entity.TableIdentity // 表标识信息
}

// NewUKMatchStrategy 创建唯一键匹配策略函数
func NewUKMatchStrategy(identity *entity.TableIdentity) *UKMatchStrategy { // 创建唯一键匹配策略实例
	return &UKMatchStrategy{identity: identity} // 返回策略实例
}

// BuildWhereClause 构建WHERE条件方法
func (s *UKMatchStrategy) BuildWhereClause(row map[string]interface{}) string { // 构建唯一键WHERE条件
	whereClause := "" // 初始化WHERE条件字符串
	for i, col := range s.identity.IdentifyCols { // 遍历标识列
		if i > 0 { // 如果不是第一列
			whereClause += " AND " // 添加AND连接符
		}
		whereClause += col + " = ?" // 添加列条件
	}
	return whereClause // 返回WHERE条件
}

// GetWhereArgs 获取WHERE条件参数方法
func (s *UKMatchStrategy) GetWhereArgs(row map[string]interface{}) []interface{} { // 获取WHERE条件的参数值
	args := make([]interface{}, len(s.identity.IdentifyCols)) // 创建参数切片
	for i, col := range s.identity.IdentifyCols { // 遍历标识列
		args[i] = row[col] // 获取列的值
	}
	return args // 返回参数列表
}

// GetStrategyName 获取策略名称方法
func (s *UKMatchStrategy) GetStrategyName() string { // 获取策略名称
	return "UK_STRATEGY" // 返回唯一键策略名称
}

// NewMatchStrategy 根据标识策略创建匹配策略函数
func NewMatchStrategy(identity *entity.TableIdentity) MatchStrategy { // 根据表标识创建匹配策略
	switch identity.Strategy { // 根据标识策略选择匹配策略
	case entity.PKStrategy: // 如果是主键策略
		return NewPKMatchStrategy(identity) // 返回主键匹配策略
	case entity.UKStrategy: // 如果是唯一键策略
		return NewUKMatchStrategy(identity) // 返回唯一键匹配策略
	case entity.FullColumnsStrategy: // 如果是全列匹配策略
		return NewFullColumnMatchStrategy(identity) // 返回全列匹配策略
	default: // 默认情况
		return NewFullColumnMatchStrategy(identity) // 返回全列匹配策略
	}
}
