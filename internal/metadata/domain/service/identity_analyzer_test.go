package service

import (
	"errors"
	"testing"

	"mysql-to-async/internal/metadata/domain/entity"
)

// MockTableMetadataRepository 模拟表元数据仓库
type MockTableMetadataRepository struct {
	columns      []entity.ColumnMeta
	pkColumns    []string
	ukColumns    []string
	tables       []entity.TableInfo
	databases    []string
	columnsErr   error
	pkColumnsErr error
	ukColumnsErr error
	tablesErr    error
	databasesErr error
}

func (m *MockTableMetadataRepository) GetTableColumns(schema, tableName string) ([]entity.ColumnMeta, error) {
	return m.columns, m.columnsErr
}

func (m *MockTableMetadataRepository) GetPrimaryKeyColumns(schema, tableName string) ([]string, error) {
	return m.pkColumns, m.pkColumnsErr
}

func (m *MockTableMetadataRepository) GetUniqueKeyColumns(schema, tableName string) ([]string, error) {
	return m.ukColumns, m.ukColumnsErr
}

func (m *MockTableMetadataRepository) GetAllTables(schema string) ([]entity.TableInfo, error) {
	return m.tables, m.tablesErr
}

func (m *MockTableMetadataRepository) GetAllDatabases() ([]string, error) {
	return m.databases, m.databasesErr
}

func TestIdentityAnalyzerService_AnalyzeTable_PK(t *testing.T) {
	mockRepo := &MockTableMetadataRepository{
		columns: []entity.ColumnMeta{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
			{Name: "name", DataType: "varchar", IsPrimaryKey: false},
		},
		pkColumns: []string{"id"},
		ukColumns: []string{},
	}

	service := NewIdentityAnalyzerService(mockRepo)
	identity, err := service.AnalyzeTable("test_db", "users")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if identity.Strategy != entity.PKStrategy {
		t.Errorf("expected PKStrategy, got %s", identity.Strategy)
	}

	if !identity.HasPK {
		t.Error("expected HasPK to be true")
	}

	if len(identity.IdentifyCols) != 1 {
		t.Errorf("expected 1 identify column, got %d", len(identity.IdentifyCols))
	}

	if identity.IdentifyCols[0] != "id" {
		t.Errorf("expected identify column 'id', got %s", identity.IdentifyCols[0])
	}
}

func TestIdentityAnalyzerService_AnalyzeTable_UK(t *testing.T) {
	mockRepo := &MockTableMetadataRepository{
		columns: []entity.ColumnMeta{
			{Name: "id", DataType: "int", IsPrimaryKey: false},
			{Name: "email", DataType: "varchar", IsPrimaryKey: false, IsUnique: true},
		},
		pkColumns: []string{},
		ukColumns: []string{"email"},
	}

	service := NewIdentityAnalyzerService(mockRepo)
	identity, err := service.AnalyzeTable("test_db", "users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if identity.Strategy != entity.UKStrategy {
		t.Errorf("expected UKStrategy, got %s", identity.Strategy)
	}

	if !identity.HasUK {
		t.Error("expected HasUK to be true")
	}

	if identity.HasPK {
		t.Error("expected HasPK to be false")
	}

	if len(identity.IdentifyCols) != 1 {
		t.Errorf("expected 1 identify column, got %d", len(identity.IdentifyCols))
	}

	if identity.IdentifyCols[0] != "email" {
		t.Errorf("expected identify column 'email', got %s", identity.IdentifyCols[0])
	}
}

func TestIdentityAnalyzerService_AnalyzeTable_FullColumns(t *testing.T) {
	mockRepo := &MockTableMetadataRepository{
		columns: []entity.ColumnMeta{
			{Name: "message", DataType: "text", IsPrimaryKey: false},
			{Name: "level", DataType: "varchar", IsPrimaryKey: false},
			{Name: "created_at", DataType: "datetime", IsPrimaryKey: false},
		},
		pkColumns: []string{},
		ukColumns: []string{},
	}

	service := NewIdentityAnalyzerService(mockRepo)
	identity, err := service.AnalyzeTable("test_db", "logs")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if identity.Strategy != entity.FullColumnsStrategy {
		t.Errorf("expected FullColumnsStrategy, got %s", identity.Strategy)
	}

	if identity.HasPK {
		t.Error("expected HasPK to be false")
	}

	if identity.HasUK {
		t.Error("expected HasUK to be false")
	}

	// 全列匹配时，所有列作为标识列
	if len(identity.IdentifyCols) != 3 {
		t.Errorf("expected 3 identify columns for full columns strategy, got %d", len(identity.IdentifyCols))
	}

	// 风险名称验证
	expectedCols := map[string]bool{"message": true, "level": true, "created_at": true}
	for _, col := range identity.Columns {
		if !expectedCols[col.Name] {
			t.Errorf("missing column %s in identify columns", col.Name)
		}
	}
}

func TestIdentityAnalyzerService_AnalyzeTable_CompositePK(t *testing.T) {
	mockRepo := &MockTableMetadataRepository{
		columns: []entity.ColumnMeta{
			{Name: "user_id", DataType: "int", IsPrimaryKey: true},
			{Name: "role_id", DataType: "int", IsPrimaryKey: true},
			{Name: "assigned_at", DataType: "datetime", IsPrimaryKey: false},
		},
		pkColumns: []string{"user_id", "role_id"},
		ukColumns: []string{},
	}

	service := NewIdentityAnalyzerService(mockRepo)
	identity, err := service.AnalyzeTable("test_db", "user_roles")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if identity.Strategy != entity.PKStrategy {
		t.Errorf("expected PKStrategy, got %s", identity.Strategy)
	}

	if len(identity.IdentifyCols) != 2 {
		t.Errorf("expected 2 identify columns for composite PK, got %d", len(identity.IdentifyCols))
	}

	if identity.IdentifyCols[0] != "user_id" {
		t.Errorf("expected first identify column 'user_id', got %s", identity.IdentifyCols[0])
	}

	if identity.IdentifyCols[1] != "role_id" {
		t.Errorf("expected second identify column 'role_id', got %s", identity.IdentifyCols[1])
	}
}

func TestIdentityAnalyzerService_AnalyzeTable_ColumnsError(t *testing.T) {
	testError := errors.New("columns query error")
	mockRepo := &MockTableMetadataRepository{
		columnsErr: testError,
	}

	service := NewIdentityAnalyzerService(mockRepo)
	_, err := service.AnalyzeTable("test_db", "users")
	if err == nil {
		t.Error("expected error, got nil")
	}
	if err != testError {
		t.Errorf("expected testError, got %v", err)
	}
}

func TestIdentityAnalyzerService_AnalyzeTable_PKError(t *testing.T) {
	testError := errors.New("pk columns query error")
	mockRepo := &MockTableMetadataRepository{
		columns: []entity.ColumnMeta{
			{Name: "id", DataType: "int"},
		},
		pkColumnsErr: testError,
	}

	service := NewIdentityAnalyzerService(mockRepo)
	_, err := service.AnalyzeTable("test_db", "users")
	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestIdentityAnalyzerService_AnalyzeTable_UKError(t *testing.T) {
	testError := errors.New("uk columns query error")
	mockRepo := &MockTableMetadataRepository{
		columns: []entity.ColumnMeta{
			{Name: "id", DataType: "int"},
		},
		pkColumns:    []string{},
		ukColumnsErr: testError,
	}

	service := NewIdentityAnalyzerService(mockRepo)
	_, err := service.AnalyzeTable("test_db", "users")
	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestIdentityAnalyzerService_GetAllTables(t *testing.T) {
	mockRepo := &MockTableMetadataRepository{
		tables: []entity.TableInfo{
			{Schema: "test_db", TableName: "users", TableRowCount: 100},
			{Schema: "test_db", TableName: "orders", TableRowCount: 500},
			{Schema: "test_db", TableName: "products", TableRowCount: 200},
		},
	}

	service := NewIdentityAnalyzerService(mockRepo)
	tables, err := service.GetAllTables("test_db")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(tables) != 3 {
		t.Errorf("expected 3 tables, got %d", len(tables))
	}

	// 风险名称验证
	expectedNames := map[string]bool{"users": true, "orders": true, "products": true}
	for _, table := range tables {
		if !expectedNames[table.TableName] {
			t.Errorf("unexpected table %s", table.TableName)
		}
	}
}

func TestIdentityAnalyzerService_GetAllTables_Error(t *testing.T) {
	testError := errors.New("tables query error")
	mockRepo := &MockTableMetadataRepository{
		tablesErr: testError,
	}

	service := NewIdentityAnalyzerService(mockRepo)
	_, err := service.GetAllTables("test_db")
	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestIdentityAnalyzerService_GetAllDatabases(t *testing.T) {
	mockRepo := &MockTableMetadataRepository{
		databases: []string{"db1", "db2", "db3"},
	}

	service := NewIdentityAnalyzerService(mockRepo)
	databases, err := service.GetAllDatabases()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(databases) != 3 {
		t.Errorf("expected 3 databases, got %d", len(databases))
	}

	expectedDBs := map[string]bool{"db1": true, "db2": true, "db3": true}
	for _, db := range databases {
		if !expectedDBs[db] {
			t.Errorf("unexpected database %s", db)
		}
	}
}

func TestIdentityAnalyzerService_GetAllDatabases_Error(t *testing.T) {
	testError := errors.New("databases query error")
	mockRepo := &MockTableMetadataRepository{
		databasesErr: testError,
	}

	service := NewIdentityAnalyzerService(mockRepo)
	_, err := service.GetAllDatabases()
	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestNewIdentityAnalyzerService(t *testing.T) {
	mockRepo := &MockTableMetadataRepository{}
	service := NewIdentityAnalyzerService(mockRepo)
	if service == nil {
		t.Error("service should not be nil")
	}
}

func TestIdentityAnalyzerService_EmptyColumns(t *testing.T) {
	mockRepo := &MockTableMetadataRepository{
		columns:   []entity.ColumnMeta{},
		pkColumns: []string{},
		ukColumns: []string{},
	}

	service := NewIdentityAnalyzerService(mockRepo)
	identity, err := service.AnalyzeTable("test_db", "empty_table")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if identity.Strategy != entity.FullColumnsStrategy {
		t.Errorf("expected FullColumnsStrategy for empty table, got %s", identity.Strategy)
	}

	if len(identity.IdentifyCols) != 0 {
		t.Errorf("expected 0 identify columns for empty table, got %d", len(identity.IdentifyCols))
	}
}
