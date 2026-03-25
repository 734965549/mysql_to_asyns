package handler

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"

	"mysql-to-async/internal/config"
	"mysql-to-async/internal/metadata/domain/service"
	metadataService "mysql-to-async/internal/metadata/domain/service"
	"mysql-to-async/internal/metadata/infrastructure"
	taskService "mysql-to-async/internal/task/application/service"
	taskEntity "mysql-to-async/internal/task/domain/entity"
)

// TaskHandler 任务处理器
type TaskHandler struct {
	taskService *taskService.TaskService
	analyzer    metadataService.IdentityAnalyzer
}

// NewTaskHandler 创建任务处理器
func NewTaskHandler(taskService *taskService.TaskService, analyzer metadataService.IdentityAnalyzer) *TaskHandler {
	return &TaskHandler{
		taskService: taskService,
		analyzer:    analyzer,
	}
}

// DatabaseConfigRequest 数据库配置请求
type DatabaseConfigRequest struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
}

// CreateTaskRequest 创建任务请求
type CreateTaskRequest struct {
	Name            string                 `json:"name" binding:"required"`
	SyncLevel       string                 `json:"sync_level"` // 同步级别: DATABASE 或 TABLE
	SourceSchema    string                 `json:"source_schema"`
	TargetSchema    string                 `json:"target_schema"`
	SourceDatabases []string               `json:"source_databases"` // 源数据库列表（库级别同步时使用）
	TargetDatabase  string                 `json:"target_database"`  // 目标数据库（库级别同步时，所有库同步到此库）
	TargetDatabases []string               `json:"target_databases"` // 目标数据库列表（与 SourceDatabases 一一对应）
	Tables          []string               `json:"tables"`
	Mode            string                 `json:"mode" binding:"required"`
	BatchSize       int                    `json:"batch_size"`
	WorkerCount     int                    `json:"worker_count"`
	EnableLimitOne  bool                   `json:"enable_limit_one"`
	SourceDB        *DatabaseConfigRequest `json:"source_db,omitempty"` // 源数据库配置（可选）
	TargetDB        *DatabaseConfigRequest `json:"target_db,omitempty"` // 目标数据库配置（可选）
}

// CreateTask 创建任务
func (h *TaskHandler) CreateTask(c *gin.Context) {
	var req CreateTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 设置默认值
	if req.BatchSize == 0 {
		req.BatchSize = 1000
	}
	if req.WorkerCount == 0 {
		req.WorkerCount = 4
	}

	// 转换数据库配置
	var sourceDB, targetDB *taskEntity.DatabaseConfig
	if req.SourceDB != nil {
		sourceDB = &taskEntity.DatabaseConfig{
			Host:     req.SourceDB.Host,
			Port:     req.SourceDB.Port,
			Database: req.SourceDB.Database,
			Username: req.SourceDB.Username,
			Password: req.SourceDB.Password,
		}
	}
	if req.TargetDB != nil {
		targetDB = &taskEntity.DatabaseConfig{
			Host:     req.TargetDB.Host,
			Port:     req.TargetDB.Port,
			Database: req.TargetDB.Database,
			Username: req.TargetDB.Username,
			Password: req.TargetDB.Password,
		}
	}

	// 设置同步级别（支持大小写不敏感）
	syncLevel := taskEntity.SyncLevelTable
	if strings.ToUpper(req.SyncLevel) == "DATABASE" {
		syncLevel = taskEntity.SyncLevelDatabase
	} else {
		syncLevel = taskEntity.SyncLevelTable
	}

	taskCfg := taskEntity.TaskConfig{
		ID:              generateID(),
		Name:            req.Name,
		SyncLevel:       syncLevel,
		SourceSchema:    req.SourceSchema,
		TargetSchema:    req.TargetSchema,
		SourceDatabases: req.SourceDatabases,
		TargetDatabase:  req.TargetDatabase,
		TargetDatabases: req.TargetDatabases,
		Tables:          req.Tables,
		Mode:            taskEntity.SyncMode(req.Mode),
		BatchSize:       req.BatchSize,
		WorkerCount:     req.WorkerCount,
		EnableLimitOne:  req.EnableLimitOne,
		SourceDB:        sourceDB,
		TargetDB:        targetDB,
	}
	task, err := h.taskService.CreateTask(taskCfg)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, task)
}

// StartTask 启动任务
func (h *TaskHandler) StartTask(c *gin.Context) {
	taskID := c.Param("id")

	if err := h.taskService.StartTask(c.Request.Context(), taskID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Task started"})
}

// PauseTask 暂停任务
func (h *TaskHandler) PauseTask(c *gin.Context) {
	taskID := c.Param("id")

	if err := h.taskService.PauseTask(taskID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Task paused"})
}

// GetTask 获取任务详情
func (h *TaskHandler) GetTask(c *gin.Context) {
	taskID := c.Param("id")

	task, exists := h.taskService.GetTask(taskID)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "Task not found"})
		return
	}

	c.JSON(http.StatusOK, task)
}

// GetAllTasks 获取所有任务
func (h *TaskHandler) GetAllTasks(c *gin.Context) {
	tasks := h.taskService.GetAllTasks()
	c.JSON(http.StatusOK, tasks)
}

// GetTaskMetrics 获取任务指标
func (h *TaskHandler) GetTaskMetrics(c *gin.Context) {
	taskID := c.Param("id")

	metrics, err := h.taskService.GetTaskMetrics(taskID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, metrics)
}

// SkipError 跳过错误
func (h *TaskHandler) SkipError(c *gin.Context) {
	taskID := c.Param("id")

	if err := h.taskService.SkipError(taskID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Error skipped"})
}

// GetDefaultConfig 获取当前全局配置
func (h *TaskHandler) GetDefaultConfig(c *gin.Context) {
	cfg := config.GlobalConfig
	if cfg == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Config not loaded"})
		return
	}

	c.JSON(http.StatusOK, cfg)
}

// UpdateGlobalConfig 更新全局配置
func (h *TaskHandler) UpdateGlobalConfig(c *gin.Context) {
	var req config.Config
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 更新内存中的配置
	*config.GlobalConfig = req

	// 保存到文件
	if err := config.SaveConfig("etc/application.toml", config.GlobalConfig); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save config: " + err.Error()})
		return
	}

	// 动态重初始化存储后端（如从 file 切换到 mysql 时会自动建表）
	storageMsg := ""
	if err := h.taskService.ReinitStorage(config.GlobalConfig); err != nil {
		storageMsg = fmt.Sprintf("Warning: storage reinitialization failed: %v", err)
	} else if req.Storage.Mode == "mysql" {
		storageMsg = "MySQL storage initialized, sys_sync_tasks table ensured"
	}

	resp := gin.H{"message": "Configuration updated successfully"}
	if storageMsg != "" {
		resp["storage"] = storageMsg
	}
	c.JSON(http.StatusOK, resp)
}

// MetadataHandler 元数据处理器
type MetadataHandler struct {
	analyzer metadataService.IdentityAnalyzer
}

// NewMetadataHandler 创建元数据处理器
func NewMetadataHandler(analyzer metadataService.IdentityAnalyzer) *MetadataHandler {
	return &MetadataHandler{analyzer: analyzer}
}

// GetDatabases 获取数据库列表
func (h *MetadataHandler) GetDatabases(c *gin.Context) {
	if h.analyzer == nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Database not connected. Please create a task with database configuration first, or configure the datasource in config file.",
		})
		return
	}

	databases, err := h.analyzer.GetAllDatabases()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, databases)
}

// GetTables 获取表列表
func (h *MetadataHandler) GetTables(c *gin.Context) {
	if h.analyzer == nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Database not connected. Please create a task with database configuration first, or configure the datasource in config file.",
		})
		return
	}

	schema := c.Query("schema")
	if schema == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "schema is required"})
		return
	}

	tables, err := h.analyzer.GetAllTables(schema)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, tables)
}

// RefreshMetadata 刷新元数据（重新从数据库加载）
func (h *MetadataHandler) RefreshMetadata(c *gin.Context) {
	// 元数据是从 information_schema 实时查询的
	// 这里只需要返回成功，前端会重新调用获取接口
	c.JSON(http.StatusOK, gin.H{
		"message": "Metadata refresh triggered",
		"success": true,
	})
}

// GetTableIdentity 获取表标识信息
func (h *MetadataHandler) GetTableIdentity(c *gin.Context) {
	if h.analyzer == nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Database not connected. Please create a task with database configuration first, or configure the datasource in config file.",
		})
		return
	}

	schema := c.Query("schema")
	tableName := c.Query("table")

	if schema == "" || tableName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "schema and table are required"})
		return
	}

	identity, err := h.analyzer.AnalyzeTable(schema, tableName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 构建响应，包含风险提示
	response := gin.H{
		"table_name":    identity.TableName,
		"strategy":      identity.Strategy,
		"identify_cols": identity.IdentifyCols,
		"has_pk":        identity.HasPK,
		"has_uk":        identity.HasUK,
		"columns":       identity.Columns,
	}

	// 无主键表风险提示
	if identity.Strategy == "FULL_COLUMNS_STRATEGY" {
		response["warning"] = "该表将采用全列匹配模式，同步性能可能受限"
	}

	c.JSON(http.StatusOK, response)
}

// UpdateTask 更新任务
func (h *TaskHandler) UpdateTask(c *gin.Context) {
	taskID := c.Param("id")
	var req UpdateTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	task, exists := h.taskService.GetTask(taskID)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "Task not found"})
		return
	}

	// 只允许更新非运行状态的任务
	if task.Context.Status == taskEntity.TaskStatusRunning {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Cannot update running task"})
		return
	}

	// 更新任务配置
	if req.Name != "" {
		task.Config.Name = req.Name
	}
	// 更新同步级别（支持大小写不敏感）
	if req.SyncLevel != "" {
		if strings.ToUpper(req.SyncLevel) == "DATABASE" {
			task.Config.SyncLevel = taskEntity.SyncLevelDatabase
		} else {
			task.Config.SyncLevel = taskEntity.SyncLevelTable
		}
	}
	if req.SourceSchema != "" {
		task.Config.SourceSchema = req.SourceSchema
	}
	if req.TargetSchema != "" {
		task.Config.TargetSchema = req.TargetSchema
	}
	// 表列表：允许清空（库级别同步时 tables 为空）
	task.Config.Tables = req.Tables
	if req.Mode != "" {
		task.Config.Mode = taskEntity.SyncMode(req.Mode)
	}
	if req.BatchSize > 0 {
		task.Config.BatchSize = req.BatchSize
	}
	if req.WorkerCount > 0 {
		task.Config.WorkerCount = req.WorkerCount
	}
	// enable_limit_one 是 bool 类型，直接赋值
	task.Config.EnableLimitOne = req.EnableLimitOne

	// 更新数据库配置
	if req.SourceDB != nil {
		task.Config.SourceDB = &taskEntity.DatabaseConfig{
			Host:     req.SourceDB.Host,
			Port:     req.SourceDB.Port,
			Database: req.SourceDB.Database,
			Username: req.SourceDB.Username,
			Password: req.SourceDB.Password,
		}
	}
	if req.TargetDB != nil {
		task.Config.TargetDB = &taskEntity.DatabaseConfig{
			Host:     req.TargetDB.Host,
			Port:     req.TargetDB.Port,
			Database: req.TargetDB.Database,
			Username: req.TargetDB.Username,
			Password: req.TargetDB.Password,
		}
	}

	if err := h.taskService.UpdateTask(task); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, task)
}

// DeleteTask 删除任务
func (h *TaskHandler) DeleteTask(c *gin.Context) {
	taskID := c.Param("id")

	task, exists := h.taskService.GetTask(taskID)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "Task not found"})
		return
	}

	// 只允许删除非运行状态的任务
	if task.Context.Status == taskEntity.TaskStatusRunning {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Cannot delete running task"})
		return
	}

	if err := h.taskService.DeleteTask(taskID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Task deleted"})
}

// UpdateTaskRequest 更新任务请求
type UpdateTaskRequest struct {
	Name           string                 `json:"name"`
	SyncLevel      string                 `json:"sync_level"`
	SourceSchema   string                 `json:"source_schema"`
	TargetSchema   string                 `json:"target_schema"`
	Tables         []string               `json:"tables"`
	Mode           string                 `json:"mode"`
	BatchSize      int                    `json:"batch_size"`
	WorkerCount    int                    `json:"worker_count"`
	EnableLimitOne bool                   `json:"enable_limit_one"`
	SourceDB       *DatabaseConfigRequest `json:"source_db,omitempty"`
	TargetDB       *DatabaseConfigRequest `json:"target_db,omitempty"`
}

// generateID 生成唯一ID
func generateID() string {
	return "task_" + randomString(8)
}

// randomString 生成随机字符串
func randomString(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// 如果加密随机数生成失败，使用时间戳作为备用方案
		return hex.EncodeToString([]byte(time.Now().Format("20060102150405")))[:n]
	}
	return hex.EncodeToString(b)[:n]
}

// TestConnectionRequest 测试连接请求
type TestConnectionRequest struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"`
}

// TestConnection 测试数据库连接
func (h *TaskHandler) TestConnection(c *gin.Context) {
	var req TestConnectionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 构建DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&timeout=5s",
		req.Username,
		req.Password,
		req.Host,
		req.Port,
		req.Database,
	)

	// 尝试连接
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"success": false,
			"message": fmt.Sprintf("连接失败: %v", err),
		})
		return
	}
	defer db.Close()

	// 设置超时
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 测试连接
	if err := db.PingContext(ctx); err != nil {
		c.JSON(http.StatusOK, gin.H{
			"success": false,
			"message": fmt.Sprintf("连接失败: %v", err),
		})
		return
	}

	// 获取版本信息
	var version string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		version = "unknown"
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": fmt.Sprintf("连接成功 (MySQL %s)", version),
		"version": version,
	})
}

// GetDatabasesWithConfig 使用自定义配置获取数据库列表
func (h *MetadataHandler) GetDatabasesWithConfig(c *gin.Context) {
	var req TestConnectionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 构建DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		req.Username,
		req.Password,
		req.Host,
		req.Port,
		req.Database,
	)

	// 连接数据库
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("连接失败: %v", err)})
		return
	}
	defer db.Close()

	// 创建分析器
	schemaDetector := infrastructure.NewSchemaDetector(db)
	analyzer := service.NewIdentityAnalyzerService(schemaDetector)

	// 获取数据库列表
	databases, err := analyzer.GetAllDatabases()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, databases)
}

// GetTablesWithConfig 使用自定义配置获取表列表
func (h *MetadataHandler) GetTablesWithConfig(c *gin.Context) {
	var req struct {
		TestConnectionRequest
		Schema string `json:"schema"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.Schema == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "schema is required"})
		return
	}

	// 构建DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		req.Username,
		req.Password,
		req.Host,
		req.Port,
		req.Database,
	)

	// 连接数据库
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("连接失败: %v", err)})
		return
	}
	defer db.Close()

	// 创建分析器
	schemaDetector := infrastructure.NewSchemaDetector(db)
	analyzer := service.NewIdentityAnalyzerService(schemaDetector)

	// 获取表列表
	tables, err := analyzer.GetAllTables(req.Schema)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, tables)
}
