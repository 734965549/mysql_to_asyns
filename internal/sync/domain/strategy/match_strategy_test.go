package strategy

import (
	"testing"

	"mysql-to-sync/internal/metadata/domain/entity"
)

func TestPKMatchStrategy_BuildWhereClause(t *testing.T) {
	tests := []struct {
		name           string
		identity       *entity.TableIdentity
		expectedClause string
	}{
		{
			name: "single primary key",
			identity: &entity.TableIdentity{
				Strategy:     entity.PKStrategy,
				IdentifyCols: []string{"id"},
			},
			expectedClause: "id = ?",
		},
		{
			name: "composite primary key",
			identity: &entity.TableIdentity{
				Strategy:     entity.PKStrategy,
				IdentifyCols: []string{"user_id", "role_id"},
			},
			expectedClause: "user_id = ? AND role_id = ?",
		},
		{
			name: "three column primary key",
			identity: &entity.TableIdentity{
				Strategy:     entity.PKStrategy,
				IdentifyCols: []string{"col1", "col2", "col3"},
			},
			expectedClause: "col1 = ? AND col2 = ? AND col3 = ?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := NewPKMatchStrategy(tt.identity)
			row := map[string]interface{}{} // row content doesn't matter for WHERE clause building
			clause := strategy.BuildWhereClause(row)
			if clause != tt.expectedClause {
				t.Errorf("expected clause %q, got %q", tt.expectedClause, clause)
			}
		})
	}
}

func TestPKMatchStrategy_GetWhereArgs(t *testing.T) {
	tests := []struct {
		name         string
		identity     *entity.TableIdentity
		row          map[string]interface{}
		expectedArgs []interface{}
	}{
		{
			name: "single primary key",
			identity: &entity.TableIdentity{
				Strategy:     entity.PKStrategy,
				IdentifyCols: []string{"id"},
			},
			row: map[string]interface{}{
				"id":   123,
				"name": "test",
			},
			expectedArgs: []interface{}{123},
		},
		{
			name: "composite primary key",
			identity: &entity.TableIdentity{
				Strategy:     entity.PKStrategy,
				IdentifyCols: []string{"user_id", "role_id"},
			},
			row: map[string]interface{}{
				"user_id": 1,
				"role_id": 2,
				"name":    "admin",
			},
			expectedArgs: []interface{}{1, 2},
		},
		{
			name: "string primary key",
			identity: &entity.TableIdentity{
				Strategy:     entity.PKStrategy,
				IdentifyCols: []string{"uuid"},
			},
			row: map[string]interface{}{
				"uuid": "abc-123-def",
			},
			expectedArgs: []interface{}{"abc-123-def"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := NewPKMatchStrategy(tt.identity)
			args := strategy.GetWhereArgs(tt.row)
			if len(args) != len(tt.expectedArgs) {
				t.Errorf("expected %d args, got %d", len(tt.expectedArgs), len(args))
				return
			}
			for i, arg := range args {
				if arg != tt.expectedArgs[i] {
					t.Errorf("arg[%d]: expected %v, got %v", i, tt.expectedArgs[i], arg)
				}
			}
		})
	}
}

func TestPKMatchStrategy_GetStrategyName(t *testing.T) {
	identity := &entity.TableIdentity{
		Strategy:     entity.PKStrategy,
		IdentifyCols: []string{"id"},
	}
	strategy := NewPKMatchStrategy(identity)
	if strategy.GetStrategyName() != "PK_STRATEGY" {
		t.Errorf("expected strategy name 'PK_STRATEGY', got %s", strategy.GetStrategyName())
	}
}

func TestUKMatchStrategy_BuildWhereClause(t *testing.T) {
	tests := []struct {
		name           string
		identity       *entity.TableIdentity
		expectedClause string
	}{
		{
			name: "single unique key",
			identity: &entity.TableIdentity{
				Strategy:     entity.UKStrategy,
				IdentifyCols: []string{"email"},
			},
			expectedClause: "email = ?",
		},
		{
			name: "composite unique key",
			identity: &entity.TableIdentity{
				Strategy:     entity.UKStrategy,
				IdentifyCols: []string{"order_id", "product_id"},
			},
			expectedClause: "order_id = ? AND product_id = ?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := NewUKMatchStrategy(tt.identity)
			row := map[string]interface{}{}
			clause := strategy.BuildWhereClause(row)
			if clause != tt.expectedClause {
				t.Errorf("expected clause %q, got %q", tt.expectedClause, clause)
			}
		})
	}
}

func TestUKMatchStrategy_GetWhereArgs(t *testing.T) {
	tests := []struct {
		name         string
		identity     *entity.TableIdentity
		row          map[string]interface{}
		expectedArgs []interface{}
	}{
		{
			name: "single unique key",
			identity: &entity.TableIdentity{
				Strategy:     entity.UKStrategy,
				IdentifyCols: []string{"email"},
			},
			row: map[string]interface{}{
				"email": "test@example.com",
				"name":  "test",
			},
			expectedArgs: []interface{}{"test@example.com"},
		},
		{
			name: "composite unique key",
			identity: &entity.TableIdentity{
				Strategy:     entity.UKStrategy,
				IdentifyCols: []string{"order_id", "product_id"},
			},
			row: map[string]interface{}{
				"order_id":   "ORD001",
				"product_id": "PROD001",
			},
			expectedArgs: []interface{}{"ORD001", "PROD001"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := NewUKMatchStrategy(tt.identity)
			args := strategy.GetWhereArgs(tt.row)
			if len(args) != len(tt.expectedArgs) {
				t.Errorf("expected %d args, got %d", len(tt.expectedArgs), len(args))
				return
			}
			for i, arg := range args {
				if arg != tt.expectedArgs[i] {
					t.Errorf("arg[%d]: expected %v, got %v", i, tt.expectedArgs[i], arg)
				}
			}
		})
	}
}

func TestUKMatchStrategy_GetStrategyName(t *testing.T) {
	identity := &entity.TableIdentity{
		Strategy:     entity.UKStrategy,
		IdentifyCols: []string{"email"},
	}
	strategy := NewUKMatchStrategy(identity)
	if strategy.GetStrategyName() != "UK_STRATEGY" {
		t.Errorf("expected strategy name 'UK_STRATEGY', got %s", strategy.GetStrategyName())
	}
}

func TestFullColumnMatchStrategy_BuildWhereClause(t *testing.T) {
	tests := []struct {
		name           string
		identity       *entity.TableIdentity
		expectedClause string
	}{
		{
			name: "two columns",
			identity: &entity.TableIdentity{
				Strategy:     entity.FullColumnsStrategy,
				IdentifyCols: []string{"col1", "col2"},
			},
			expectedClause: "col1 = ? AND col2 = ? LIMIT 1",
		},
		{
			name: "three columns",
			identity: &entity.TableIdentity{
				Strategy:     entity.FullColumnsStrategy,
				IdentifyCols: []string{"a", "b", "c"},
			},
			expectedClause: "a = ? AND b = ? AND c = ? LIMIT 1",
		},
		{
			name: "single column",
			identity: &entity.TableIdentity{
				Strategy:     entity.FullColumnsStrategy,
				IdentifyCols: []string{"status"},
			},
			expectedClause: "status = ? LIMIT 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := NewFullColumnMatchStrategy(tt.identity)
			row := map[string]interface{}{}
			clause := strategy.BuildWhereClause(row)
			if clause != tt.expectedClause {
				t.Errorf("expected clause %q, got %q", tt.expectedClause, clause)
			}
		})
	}
}

func TestFullColumnMatchStrategy_GetWhereArgs(t *testing.T) {
	tests := []struct {
		name         string
		identity     *entity.TableIdentity
		row          map[string]interface{}
		expectedArgs []interface{}
	}{
		{
			name: "all columns",
			identity: &entity.TableIdentity{
				Strategy:     entity.FullColumnsStrategy,
				IdentifyCols: []string{"message", "level", "created_at"},
			},
			row: map[string]interface{}{
				"message":    "test log",
				"level":      "INFO",
				"created_at": "2024-01-01",
			},
			expectedArgs: []interface{}{"test log", "INFO", "2024-01-01"},
		},
		{
			name: "mixed types",
			identity: &entity.TableIdentity{
				Strategy:     entity.FullColumnsStrategy,
				IdentifyCols: []string{"id", "name", "active"},
			},
			row: map[string]interface{}{
				"id":     1,
				"name":   "test",
				"active": true,
			},
			expectedArgs: []interface{}{1, "test", true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := NewFullColumnMatchStrategy(tt.identity)
			args := strategy.GetWhereArgs(tt.row)
			if len(args) != len(tt.expectedArgs) {
				t.Errorf("expected %d args, got %d", len(tt.expectedArgs), len(args))
				return
			}
			for i, arg := range args {
				if arg != tt.expectedArgs[i] {
					t.Errorf("arg[%d]: expected %v, got %v", i, tt.expectedArgs[i], arg)
				}
			}
		})
	}
}

func TestFullColumnMatchStrategy_GetStrategyName(t *testing.T) {
	identity := &entity.TableIdentity{
		Strategy:     entity.FullColumnsStrategy,
		IdentifyCols: []string{"col1", "col2"},
	}
	strategy := NewFullColumnMatchStrategy(identity)
	if strategy.GetStrategyName() != "FULL_COLUMNS_STRATEGY" {
		t.Errorf("expected strategy name 'FULL_COLUMNS_STRATEGY', got %s", strategy.GetStrategyName())
	}
}

func TestNewMatchStrategy_Factory(t *testing.T) {
	tests := []struct {
		name             string
		identity         *entity.TableIdentity
		expectedStrategy string
	}{
		{
			name: "PK strategy",
			identity: &entity.TableIdentity{
				Strategy:     entity.PKStrategy,
				IdentifyCols: []string{"id"},
			},
			expectedStrategy: "PK_STRATEGY",
		},
		{
			name: "UK strategy",
			identity: &entity.TableIdentity{
				Strategy:     entity.UKStrategy,
				IdentifyCols: []string{"email"},
			},
			expectedStrategy: "UK_STRATEGY",
		},
		{
			name: "Full columns strategy",
			identity: &entity.TableIdentity{
				Strategy:     entity.FullColumnsStrategy,
				IdentifyCols: []string{"col1", "col2"},
			},
			expectedStrategy: "FULL_COLUMNS_STRATEGY",
		},
		{
			name: "Unknown strategy defaults to full columns",
			identity: &entity.TableIdentity{
				Strategy:     "UNKNOWN",
				IdentifyCols: []string{"col1"},
			},
			expectedStrategy: "FULL_COLUMNS_STRATEGY",
		},
		{
			name: "Empty strategy defaults to full columns",
			identity: &entity.TableIdentity{
				Strategy:     "",
				IdentifyCols: []string{"col1"},
			},
			expectedStrategy: "FULL_COLUMNS_STRATEGY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := NewMatchStrategy(tt.identity)
			if strategy.GetStrategyName() != tt.expectedStrategy {
				t.Errorf("expected strategy %s, got %s", tt.expectedStrategy, strategy.GetStrategyName())
			}
		})
	}
}

func TestMatchStrategy_Interface(t *testing.T) {
	// 确保所有策略都实现了 MatchStrategy 接口
	var _ MatchStrategy = (*PKMatchStrategy)(nil)
	var _ MatchStrategy = (*UKMatchStrategy)(nil)
	var _ MatchStrategy = (*FullColumnMatchStrategy)(nil)
}

func TestFullColumnMatchStrategy_LimitOneProtection(t *testing.T) {
	// 测试无主键表的 LIMIT 1 保护
	identity := &entity.TableIdentity{
		Strategy:     entity.FullColumnsStrategy,
		IdentifyCols: []string{"message"},
	}
	strategy := NewFullColumnMatchStrategy(identity)
	clause := strategy.BuildWhereClause(map[string]interface{}{})

	// 确保包含 LIMIT 1
	if clause != "message = ? LIMIT 1" {
		t.Errorf("expected LIMIT 1 protection, got %s", clause)
	}
}

func TestPKMatchStrategy_NoLimitOne(t *testing.T) {
	// 测试有主键表不包含 LIMIT 1
	identity := &entity.TableIdentity{
		Strategy:     entity.PKStrategy,
		IdentifyCols: []string{"id"},
	}
	strategy := NewPKMatchStrategy(identity)
	clause := strategy.BuildWhereClause(map[string]interface{}{})

	// 确保不包含 LIMIT 1
	if clause == "id = ? LIMIT 1" {
		t.Error("PK strategy should not include LIMIT 1")
	}
	if clause != "id = ?" {
		t.Errorf("expected 'id = ?', got %s", clause)
	}
}

func TestUKMatchStrategy_NoLimitOne(t *testing.T) {
	// 测试唯一键表不包含 LIMIT 1
	identity := &entity.TableIdentity{
		Strategy:     entity.UKStrategy,
		IdentifyCols: []string{"email"},
	}
	strategy := NewUKMatchStrategy(identity)
	clause := strategy.BuildWhereClause(map[string]interface{}{})

	// 确保不包含 LIMIT 1
	if clause == "email = ? LIMIT 1" {
		t.Error("UK strategy should not include LIMIT 1")
	}
	if clause != "email = ?" {
		t.Errorf("expected 'email = ?', got %s", clause)
	}
}

func TestGetWhereArgs_NilValue(t *testing.T) {
	// 测试 nil 值处理
	identity := &entity.TableIdentity{
		Strategy:     entity.PKStrategy,
		IdentifyCols: []string{"id"},
	}
	strategy := NewPKMatchStrategy(identity)
	row := map[string]interface{}{
		"id": nil,
	}
	args := strategy.GetWhereArgs(row)
	if len(args) != 1 {
		t.Errorf("expected 1 arg, got %d", len(args))
	}
	if args[0] != nil {
		t.Errorf("expected nil arg, got %v", args[0])
	}
}

func TestGetWhereArgs_MissingColumn(t *testing.T) {
	// 测试缺失列的处理
	identity := &entity.TableIdentity{
		Strategy:     entity.PKStrategy,
		IdentifyCols: []string{"id", "name"},
	}
	strategy := NewPKMatchStrategy(identity)
	row := map[string]interface{}{
		"id": 1,
		// name is missing
	}
	args := strategy.GetWhereArgs(row)
	if len(args) != 2 {
		t.Errorf("expected 2 args, got %d", len(args))
	}
	if args[0] != 1 {
		t.Errorf("expected first arg 1, got %v", args[0])
	}
	if args[1] != nil {
		t.Errorf("expected second arg nil, got %v", args[1])
	}
}
