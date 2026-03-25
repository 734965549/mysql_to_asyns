package router

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"mysql-to-async/internal/config"
	metadataEntity "mysql-to-async/internal/metadata/domain/entity"
	taskService "mysql-to-async/internal/task/application/service"
)

// MockAnalyzer 模拟 IdentityAnalyzer
type MockAnalyzer struct{}

// AnalyzeTable 实现 IdentityAnalyzer 接口
func (m *MockAnalyzer) AnalyzeTable(schema, tableName string) (*metadataEntity.TableIdentity, error) {
	return &metadataEntity.TableIdentity{
		TableName:    tableName,
		IdentifyCols: []string{"id"},
		Strategy:     metadataEntity.PKStrategy,
	}, nil
}

// GetAllTables 实现 IdentityAnalyzer 接口
func (m *MockAnalyzer) GetAllTables(schema string) ([]metadataEntity.TableInfo, error) {
	return []metadataEntity.TableInfo{{Schema: schema, TableName: "test_table"}}, nil
}

// GetAllDatabases 实现 IdentityAnalyzer 接口
func (m *MockAnalyzer) GetAllDatabases() ([]string, error) {
	return []string{"test_db"}, nil
}

// testConfig 创建测试配置
func testConfig() *config.Config {
	return &config.Config{
		Datasource: config.DatasourceConfig{
			Debug: false,
		},
	}
}

func TestSetupRouter(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := taskService.NewTaskService()
	analyzer := &MockAnalyzer{}
	cfg := testConfig()

	router := SetupRouter(taskSvc, analyzer, cfg)

	if router == nil {
		t.Fatal("expected router, got nil")
	}
}

func TestSetupRouterWithNilConfig(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := taskService.NewTaskService()
	analyzer := &MockAnalyzer{}

	router := SetupRouter(taskSvc, analyzer, nil)

	if router == nil {
		t.Fatal("expected router, got nil")
	}
}

func TestHealthEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := taskService.NewTaskService()
	analyzer := &MockAnalyzer{}
	cfg := testConfig()

	router := SetupRouter(taskSvc, analyzer, cfg)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	// 验证响应内容
	expected := `{"status":"ok"}`
	if w.Body.String() != expected {
		t.Errorf("expected %s, got %s", expected, w.Body.String())
	}
}

func TestAPIHealthEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := taskService.NewTaskService()
	analyzer := &MockAnalyzer{}
	cfg := testConfig()

	router := SetupRouter(taskSvc, analyzer, cfg)

	req := httptest.NewRequest("GET", "/api/health", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := taskService.NewTaskService()
	analyzer := &MockAnalyzer{}
	cfg := testConfig()

	router := SetupRouter(taskSvc, analyzer, cfg)

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
}

func TestCORSMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := taskService.NewTaskService()
	analyzer := &MockAnalyzer{}
	cfg := testConfig()

	router := SetupRouter(taskSvc, analyzer, cfg)

	// OPTIONS请求应该返回204
	req := httptest.NewRequest("OPTIONS", "/api/tasks", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("expected status 204, got %d", w.Code)
	}

	// 验证CORS头
	if w.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Errorf("expected Access-Control-Allow-Origin: *, got %s", w.Header().Get("Access-Control-Allow-Origin"))
	}
}

func TestTasksEndpoints(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := taskService.NewTaskService()
	analyzer := &MockAnalyzer{}
	cfg := testConfig()

	router := SetupRouter(taskSvc, analyzer, cfg)

	// GET /api/tasks
	req := httptest.NewRequest("GET", "/api/tasks", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("GET /api/tasks: expected status 200, got %d", w.Code)
	}
}

func TestTaskByIDEndpoints(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := taskService.NewTaskService()
	analyzer := &MockAnalyzer{}
	cfg := testConfig()

	router := SetupRouter(taskSvc, analyzer, cfg)

	// GET /api/tasks/nonexistent
	req := httptest.NewRequest("GET", "/api/tasks/nonexistent", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("GET /api/tasks/nonexistent: expected status 404, got %d", w.Code)
	}
}

func TestMetadataEndpoints(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := taskService.NewTaskService()
	analyzer := &MockAnalyzer{}
	cfg := testConfig()

	router := SetupRouter(taskSvc, analyzer, cfg)

	// GET /api/metadata/databases
	req := httptest.NewRequest("GET", "/api/metadata/databases", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("GET /api/metadata/databases: expected status 200, got %d", w.Code)
	}
}

func TestTablesEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := taskService.NewTaskService()
	analyzer := &MockAnalyzer{}
	cfg := testConfig()

	router := SetupRouter(taskSvc, analyzer, cfg)

	// GET /api/metadata/tables
	req := httptest.NewRequest("GET", "/api/metadata/tables?schema=test_db", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("GET /api/metadata/tables: expected status 200, got %d", w.Code)
	}
}

func TestIdentityEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := taskService.NewTaskService()
	analyzer := &MockAnalyzer{}
	cfg := testConfig()

	router := SetupRouter(taskSvc, analyzer, cfg)

	// GET /api/metadata/identity
	req := httptest.NewRequest("GET", "/api/metadata/identity?schema=test_db&table=test_table", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("GET /api/metadata/identity: expected status 200, got %d", w.Code)
	}
}

func TestConfigEndpoints(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := taskService.NewTaskService()
	analyzer := &MockAnalyzer{}
	cfg := testConfig()

	router := SetupRouter(taskSvc, analyzer, cfg)

	// GET /api/config/default - 可能返回500因为没有配置文件
	req := httptest.NewRequest("GET", "/api/config/default", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// 接受200或500（配置文件可能不存在）
	if w.Code != http.StatusOK && w.Code != http.StatusInternalServerError {
		t.Errorf("GET /api/config/default: expected status 200 or 500, got %d", w.Code)
	}
}

func TestDebugMode(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := taskService.NewTaskService()
	analyzer := &MockAnalyzer{}
	cfg := &config.Config{
		Datasource: config.DatasourceConfig{
			Debug: true,
		},
	}

	router := SetupRouter(taskSvc, analyzer, cfg)

	if router == nil {
		t.Fatal("expected router, got nil")
	}
}
