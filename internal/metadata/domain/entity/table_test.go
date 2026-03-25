package entity

import (
	"encoding/json"
	"testing"
)

func TestIdentityStrategy_Constants(t *testing.T) {
	tests := []struct {
		name     string
		strategy IdentityStrategy
		expected string
	}{
		{"PKStrategy", PKStrategy, "PK_STRATEGY"},
		{"UKStrategy", UKStrategy, "UK_STRATEGY"},
		{"FullColumnsStrategy", FullColumnsStrategy, "FULL_COLUMNS_STRATEGY"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.strategy) != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, tt.strategy)
			}
		})
	}
}

func TestTableIdentity_Fields(t *testing.T) {
	identity := TableIdentity{
		TableName:    "users",
		Strategy:     PKStrategy,
		IdentifyCols: []string{"id"},
		Columns: []ColumnMeta{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
			{Name: "name", DataType: "varchar", IsPrimaryKey: false},
		},
		HasPK: true,
		HasUK: false,
	}

	if identity.TableName != "users" {
		t.Errorf("expected TableName 'users', got %s", identity.TableName)
	}

	if identity.Strategy != PKStrategy {
		t.Errorf("expected Strategy PKStrategy, got %s", identity.Strategy)
	}

	if len(identity.IdentifyCols) != 1 {
		t.Errorf("expected 1 IdentifyCol, got %d", len(identity.IdentifyCols))
	}

	if len(identity.Columns) != 2 {
		t.Errorf("expected 2 Columns, got %d", len(identity.Columns))
	}

	if !identity.HasPK {
		t.Error("expected HasPK to be true")
	}

	if identity.HasUK {
		t.Error("expected HasUK to be false")
	}
}

func TestColumnMeta_Fields(t *testing.T) {
	col := ColumnMeta{
		Name:         "id",
		DataType:     "int",
		IsNullable:   false,
		IsPrimaryKey: true,
		IsUnique:     true,
		DefaultValue: "",
	}

	if col.Name != "id" {
		t.Errorf("expected Name 'id', got %s", col.Name)
	}

	if col.DataType != "int" {
		t.Errorf("expected DataType 'int', got %s", col.DataType)
	}

	if col.IsNullable {
		t.Error("expected IsNullable to be false")
	}

	if !col.IsPrimaryKey {
		t.Error("expected IsPrimaryKey to be true")
	}

	if !col.IsUnique {
		t.Error("expected IsUnique to be true")
	}

	if col.DefaultValue != "" {
		t.Errorf("expected DefaultValue '', got %s", col.DefaultValue)
	}
}

func TestColumnMeta_AllFields(t *testing.T) {
	col := ColumnMeta{
		Name:         "email",
		DataType:     "varchar(255)",
		IsNullable:   true,
		IsPrimaryKey: false,
		IsUnique:     true,
		DefaultValue: "NULL",
	}

	if col.Name != "email" {
		t.Errorf("expected Name 'email', got %s", col.Name)
	}

	if col.DataType != "varchar(255)" {
		t.Errorf("expected DataType 'varchar(255)', got %s", col.DataType)
	}

	if !col.IsNullable {
		t.Error("expected IsNullable to be true")
	}

	if col.IsPrimaryKey {
		t.Error("expected IsPrimaryKey to be false")
	}

	if !col.IsUnique {
		t.Error("expected IsUnique to be true")
	}

	if col.DefaultValue != "NULL" {
		t.Errorf("expected DefaultValue 'NULL', got %s", col.DefaultValue)
	}
}

func TestTableInfo_Fields(t *testing.T) {
	info := TableInfo{
		Schema:        "test_db",
		TableName:     "users",
		TableRowCount: 1000,
	}

	if info.Schema != "test_db" {
		t.Errorf("expected Schema 'test_db', got %s", info.Schema)
	}

	if info.TableName != "users" {
		t.Errorf("expected TableName 'users', got %s", info.TableName)
	}

	if info.TableRowCount != 1000 {
		t.Errorf("expected TableRowCount 1000, got %d", info.TableRowCount)
	}
}

func TestTableInfo_JSON_Serialization(t *testing.T) {
	original := TableInfo{
		Schema:        "test_db",
		TableName:     "users",
		TableRowCount: 1000,
	}

	// 序列化
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("failed to marshal TableInfo: %v", err)
	}

	// 反序列化
	var decoded TableInfo
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal TableInfo: %v", err)
	}

	if decoded.Schema != original.Schema {
		t.Errorf("expected Schema %s, got %s", original.Schema, decoded.Schema)
	}

	if decoded.TableName != original.TableName {
		t.Errorf("expected TableName %s, got %s", original.TableName, decoded.TableName)
	}

	if decoded.TableRowCount != original.TableRowCount {
		t.Errorf("expected TableRowCount %d, got %d", original.TableRowCount, decoded.TableRowCount)
	}
}

func TestTableIdentity_WithMultipleColumns(t *testing.T) {
	identity := TableIdentity{
		TableName:    "order_items",
		Strategy:     UKStrategy,
		IdentifyCols: []string{"order_id", "product_id"},
		Columns: []ColumnMeta{
			{Name: "order_id", DataType: "int", IsPrimaryKey: false, IsUnique: true},
			{Name: "product_id", DataType: "int", IsPrimaryKey: false, IsUnique: true},
			{Name: "quantity", DataType: "int", IsPrimaryKey: false, IsUnique: false},
			{Name: "price", DataType: "decimal", IsPrimaryKey: false, IsUnique: false},
		},
		HasPK: false,
		HasUK: true,
	}

	if len(identity.IdentifyCols) != 2 {
		t.Errorf("expected 2 IdentifyCols, got %d", len(identity.IdentifyCols))
	}

	if identity.IdentifyCols[0] != "order_id" {
		t.Errorf("expected first IdentifyCol 'order_id', got %s", identity.IdentifyCols[0])
	}

	if identity.IdentifyCols[1] != "product_id" {
		t.Errorf("expected second IdentifyCol 'product_id', got %s", identity.IdentifyCols[1])
	}

	if len(identity.Columns) != 4 {
		t.Errorf("expected 4 Columns, got %d", len(identity.Columns))
	}
}

func TestTableIdentity_FullColumnsStrategy(t *testing.T) {
	// 测试无主键、无唯一键的表使用全列匹配策略
	identity := TableIdentity{
		TableName:    "logs",
		Strategy:     FullColumnsStrategy,
		IdentifyCols: []string{"id", "message", "created_at", "level"},
		Columns: []ColumnMeta{
			{Name: "id", DataType: "int", IsPrimaryKey: false},
			{Name: "message", DataType: "text", IsPrimaryKey: false},
			{Name: "created_at", DataType: "datetime", IsPrimaryKey: false},
			{Name: "level", DataType: "varchar", IsPrimaryKey: false},
		},
		HasPK: false,
		HasUK: false,
	}

	if identity.Strategy != FullColumnsStrategy {
		t.Errorf("expected FullColumnsStrategy, got %s", identity.Strategy)
	}

	if identity.HasPK {
		t.Error("expected HasPK to be false")
	}

	if identity.HasUK {
		t.Error("expected HasUK to be false")
	}

	// 全列匹配时，标识列应该包含所有列
	if len(identity.IdentifyCols) != len(identity.Columns) {
		t.Errorf("expected IdentifyCols length %d to match Columns length %d",
			len(identity.IdentifyCols), len(identity.Columns))
	}
}

func TestColumnMeta_CompositePrimaryKey(t *testing.T) {
	// 测试复合主键场景
	columns := []ColumnMeta{
		{Name: "user_id", DataType: "int", IsPrimaryKey: true, IsUnique: false},
		{Name: "role_id", DataType: "int", IsPrimaryKey: true, IsUnique: false},
		{Name: "assigned_at", DataType: "datetime", IsPrimaryKey: false, IsUnique: false},
	}

	pkCount := 0
	for _, col := range columns {
		if col.IsPrimaryKey {
			pkCount++
		}
	}

	if pkCount != 2 {
		t.Errorf("expected 2 primary key columns, got %d", pkCount)
	}
}

func TestTableIdentity_EmptyColumns(t *testing.T) {
	// 测试空列场景（边界条件）
	identity := TableIdentity{
		TableName:    "empty_table",
		Strategy:     FullColumnsStrategy,
		IdentifyCols: []string{},
		Columns:      []ColumnMeta{},
		HasPK:        false,
		HasUK:        false,
	}

	if len(identity.Columns) != 0 {
		t.Errorf("expected 0 columns, got %d", len(identity.Columns))
	}

	if len(identity.IdentifyCols) != 0 {
		t.Errorf("expected 0 identify columns, got %d", len(identity.IdentifyCols))
	}
}

func TestTableInfo_ZeroRowCount(t *testing.T) {
	// 测试空表场景
	info := TableInfo{
		Schema:        "test_db",
		TableName:     "empty_table",
		TableRowCount: 0,
	}

	if info.TableRowCount != 0 {
		t.Errorf("expected TableRowCount 0, got %d", info.TableRowCount)
	}
}

func TestTableInfo_LargeRowCount(t *testing.T) {
	// 测试大表场景
	info := TableInfo{
		Schema:        "production_db",
		TableName:     "audit_logs",
		TableRowCount: 9223372036854775807, // max int64
	}

	if info.TableRowCount != 9223372036854775807 {
		t.Errorf("expected TableRowCount 9223372036854775807, got %d", info.TableRowCount)
	}
}

func TestColumnMeta_VariousDataTypes(t *testing.T) {
	tests := []struct {
		name     string
		dataType string
	}{
		{"TinyInt", "tinyint"},
		{"SmallInt", "smallint"},
		{"MediumInt", "mediumint"},
		{"Int", "int"},
		{"BigInt", "bigint"},
		{"Float", "float"},
		{"Double", "double"},
		{"Decimal", "decimal(10,2)"},
		{"Char", "char(10)"},
		{"Varchar", "varchar(255)"},
		{"Text", "text"},
		{"MediumText", "mediumtext"},
		{"LongText", "longtext"},
		{"Date", "date"},
		{"DateTime", "datetime"},
		{"TimeStamp", "timestamp"},
		{"Blob", "blob"},
		{"JSON", "json"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := ColumnMeta{
				Name:     "test_col",
				DataType: tt.dataType,
			}

			if col.DataType != tt.dataType {
				t.Errorf("expected DataType %s, got %s", tt.dataType, col.DataType)
			}
		})
	}
}
