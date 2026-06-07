package handler // 声明当前文件属于handler包，用于处理HTTP请求

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
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
	"mysql-to-async/pkg/logger"
)

// TaskHandler 任务处理器结构体
type TaskHandler struct { // 定义任务处理器结构体
	taskService *taskService.TaskService         // 任务服务实例
	analyzer    metadataService.IdentityAnalyzer // 元数据分析器实例
}

// NewTaskHandler 创建任务处理器函数
func NewTaskHandler(taskService *taskService.TaskService, analyzer metadataService.IdentityAnalyzer) *TaskHandler { // 创建任务处理器实例
	return &TaskHandler{ // 返回任务处理器实例
		taskService: taskService, // 设置任务服务
		analyzer:    analyzer,    // 设置元数据服务分析器
	}
}

// DatabaseConfigRequest 数据库配置请求结构体
type DatabaseConfigRequest struct { // 定义数据库配置请求结构体
	Host     string `json:"host"`     // 数据库主机地址
	Port     int    `json:"port"`     // 数据库端口
	Database string `json:"database"` // 数据库名称
	Username string `json:"username"` // 用户名
	Password string `json:"password"` // 密码
}

// CreateTaskRequest 创建任务请求结构体
type CreateTaskRequest struct { // 定义创建任务请求结构体
	Name                     string                 `json:"name" binding:"required"`      // 任务名称，必填
	SyncLevel                string                 `json:"sync_level"`                   // 同步级别: DATABASE 或 TABLE
	SourceSchema             string                 `json:"source_schema"`                // 源模式名
	TargetSchema             string                 `json:"target_schema"`                // 目标模式名
	SourceDatabases          []string               `json:"source_databases"`             // 源数据库列表（库级别同步时使用）
	TargetDatabase           string                 `json:"target_database"`              // 目标数据库（库级别同步时，所有库同步到此库）
	TargetDatabases          []string               `json:"target_databases"`             // 目标数据库列表（与 SourceDatabases 一一对应）
	Tables                   []string               `json:"tables"`                       // 源表列表
	TargetTables             []string               `json:"target_tables"`                // 目标表列表（与 Tables 一一对应；空则沿用源表名）
	Mode                     string                 `json:"mode" binding:"required"`      // 同步模式，必填
	BatchSize                int                    `json:"batch_size"`                   // 批处理大小
	WorkerCount              int                    `json:"worker_count"`                 // 工作线程数
	IntraTableWorkerCount    int                    `json:"intra_table_worker_count"`     // 表内工作线程数
	EnableLimitOne           bool                   `json:"enable_limit_one"`             // 是否启用LIMIT 1优化
	OptimizeIndex            bool                   `json:"optimize_index"`               // 索引优化：先删后建
	EnableReadOnly           bool                   `json:"enable_read_only"`             // 同步前关闭目标只读，同步后恢复
	EnableDropTableBeforeDDL bool                   `json:"enable_drop_table_before_ddl"` // 同步DDL前先执行 DROP TABLE IF EXISTS
	SourceDB                 *DatabaseConfigRequest `json:"source_db,omitempty"`          // 源数据库配置（可选）
	TargetDB                 *DatabaseConfigRequest `json:"target_db,omitempty"`          // 目标数据库配置（可选）
}

// CreateTask 创建任务方法
func (h *TaskHandler) CreateTask(c *gin.Context) { // 创建新任务
	var req CreateTaskRequest                      // 定义请求变量
	if err := c.ShouldBindJSON(&req); err != nil { // 绑定JSON请求
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()}) // 返回错误
		return
	}

	// 设置默认值（可被 application.toml [sync] 覆盖）
	gc := config.GlobalConfig // 获取全局配置
	if req.BatchSize == 0 {   // 如果批处理大小为0
		if gc != nil && gc.Sync.APIDefaultBatchSize > 0 { // 如果配置中有默认值
			req.BatchSize = gc.Sync.APIDefaultBatchSize // 使用配置的默认值
		} else { // 否则
			req.BatchSize = 1000 // 使用硬编码默认值
		}
	}
	if req.WorkerCount == 0 { // 如果工作线程数为0
		if gc != nil && gc.Sync.APIDefaultWorkerCount > 0 { // 如果配置中有默认值
			req.WorkerCount = gc.Sync.APIDefaultWorkerCount // 使用配置的默认值
		} else { // 否则
			req.WorkerCount = 4 // 使用硬编码默认值
		}
	}

	// 转换数据库配置
	var sourceDB, targetDB *taskEntity.DatabaseConfig // 定义数据库配置变量
	if req.SourceDB != nil {                          // 如果提供了源数据库配置
		sourceDB = &taskEntity.DatabaseConfig{ // 创建源数据库配置
			Host:     req.SourceDB.Host,     // 设置主机
			Port:     req.SourceDB.Port,     // 设置端口
			Database: req.SourceDB.Database, // 设置数据库名
			Username: req.SourceDB.Username, // 设置用户名
			Password: req.SourceDB.Password, // 设置密码
		}
	}
	if req.TargetDB != nil { // 如果提供了目标数据库配置
		targetDB = &taskEntity.DatabaseConfig{ // 创建目标数据库配置
			Host:     req.TargetDB.Host,     // 设置主机
			Port:     req.TargetDB.Port,     // 设置端口
			Database: req.TargetDB.Database, // 设置数据库名
			Username: req.TargetDB.Username, // 设置用户名
			Password: req.TargetDB.Password, // 设置密码
		}
	}

	// 设置同步级别（支持大小写不敏感）
	syncLevel := taskEntity.SyncLevelTable            // 默认为表级别
	if strings.ToUpper(req.SyncLevel) == "DATABASE" { // 如果是数据库级别
		syncLevel = taskEntity.SyncLevelDatabase // 设置为数据库级别
	} else { // 否则
		syncLevel = taskEntity.SyncLevelTable // 设置为表级别
	}

	taskCfg := taskEntity.TaskConfig{ // 创建任务配置
		ID:                       generateID(),                  // 生成任务ID
		Name:                     req.Name,                      // 设置任务名称
		SyncLevel:                syncLevel,                     // 设置同步级别
		SourceSchema:             req.SourceSchema,              // 设置源模式
		TargetSchema:             req.TargetSchema,              // 设置目标模式
		SourceDatabases:          req.SourceDatabases,           // 设置源数据库列表
		TargetDatabase:           req.TargetDatabase,            // 设置目标数据库
		TargetDatabases:          req.TargetDatabases,           // 设置目标数据库列表
		Tables:                   req.Tables,                    // 设置源表列表
		TargetTables:             req.TargetTables,              // 设置目标表列表
		Mode:                     taskEntity.SyncMode(req.Mode), // 设置同步模式
		BatchSize:                req.BatchSize,                 // 设置批处理大小
		WorkerCount:              req.WorkerCount,               // 设置工作线程数
		IntraTableWorkerCount:    req.IntraTableWorkerCount,     // 设置表内工作线程数
		EnableLimitOne:           req.EnableLimitOne,            // 设置LIMIT 1优化开关
		OptimizeIndex:            req.OptimizeIndex,             // 设置索引优化开关
		EnableReadOnly:           req.EnableReadOnly,            // 设置只读管理开关
		EnableDropTableBeforeDDL: req.EnableDropTableBeforeDDL,  // 设置DDL前DROP TABLE开关
		SourceDB:                 sourceDB,                      // 设置源数据库配置
		TargetDB:                 targetDB,                      // 设置目标数据库配置
	}
	task, err := h.taskService.CreateTask(taskCfg) // 创建任务
	if err != nil {                                // 如果创建失败
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()}) // 返回错误
		return
	}

	c.JSON(http.StatusCreated, task) // 返回创建的任务
}

// StartTaskRequest 启动任务请求结构体（可选）
type StartTaskRequest struct {
	ScheduledAt       *time.Time `json:"scheduled_at,omitempty"`        // 定时启动时间（为空表示立即启动）
	RepeatCount       int        `json:"repeat_count,omitempty"`        // 重复启动次数（包含首次执行）
	RepeatIntervalSec int        `json:"repeat_interval_sec,omitempty"` // 重复启动间隔（秒）
	ScheduleMode      string     `json:"schedule_mode,omitempty"`       // 定时模式：once / repeat / cron
	CronExpression    string     `json:"cron_expression,omitempty"`     // Cron 表达式
	CronTimezone      string     `json:"cron_timezone,omitempty"`       // Cron 时区
}

// StartTask 启动任务方法（支持立即启动和定时启动）
func (h *TaskHandler) StartTask(c *gin.Context) { // 启动指定任务
	taskID := c.Param("id") // 获取任务ID参数

	// 尝试解析请求体中的 scheduled_at（可选，body 为空时立即启动）
	var req StartTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil && !errors.Is(err, io.EOF) {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.ScheduledAt != nil { // 定时启动
		if req.ScheduledAt.Before(time.Now()) { // 校验时间不能是过去
			c.JSON(http.StatusBadRequest, gin.H{"error": "定时启动时间不能早于当前时间"})
			return
		}
		if req.RepeatCount < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "repeat_count 不能小于 0"})
			return
		}
		if req.RepeatCount > 0 && req.RepeatIntervalSec < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "repeat_interval_sec 不能小于 0"})
			return
		}
		if strings.EqualFold(req.ScheduleMode, "cron") {
			if strings.TrimSpace(req.CronExpression) == "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": "cron_expression 不能为空"})
				return
			}
			if err := h.taskService.ScheduleCronTask(taskID, *req.ScheduledAt, req.CronExpression, req.CronTimezone); err != nil {
				errMsg := err.Error()
				if strings.Contains(errMsg, "task not found") {
					c.JSON(http.StatusNotFound, gin.H{"error": errMsg})
				} else {
					c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
				}
				return
			}
			c.JSON(http.StatusOK, gin.H{"message": "Task scheduled", "scheduled_at": req.ScheduledAt, "schedule_mode": "cron", "cron_expression": req.CronExpression, "cron_timezone": req.CronTimezone})
			return
		}
		if req.RepeatCount > 0 {
			if err := h.taskService.ScheduleTaskWithRepeat(taskID, *req.ScheduledAt, req.RepeatCount, req.RepeatIntervalSec); err != nil {
				errMsg := err.Error()
				if strings.Contains(errMsg, "task not found") {
					c.JSON(http.StatusNotFound, gin.H{"error": errMsg})
				} else {
					c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
				}
				return
			}
			c.JSON(http.StatusOK, gin.H{"message": "Task scheduled", "scheduled_at": req.ScheduledAt, "repeat_count": req.RepeatCount, "repeat_interval_sec": req.RepeatIntervalSec})
			return
		}
		if err := h.taskService.ScheduleTask(taskID, *req.ScheduledAt); err != nil {
			errMsg := err.Error()
			if strings.Contains(errMsg, "task not found") {
				c.JSON(http.StatusNotFound, gin.H{"error": errMsg})
			} else {
				c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
			}
			return
		}
		c.JSON(http.StatusOK, gin.H{"message": "Task scheduled", "scheduled_at": req.ScheduledAt})
		return
	}

	// 立即启动
	if err := h.taskService.StartTask(c.Request.Context(), taskID); err != nil { // 启动任务
		errMsg := err.Error()
		switch {
		case strings.Contains(errMsg, "task not found"):
			c.JSON(http.StatusNotFound, gin.H{"error": errMsg})
		case strings.Contains(errMsg, "already running"):
			c.JSON(http.StatusConflict, gin.H{"error": errMsg})
		case strings.Contains(errMsg, "failed to initialize database connections"):
			c.JSON(http.StatusInternalServerError, gin.H{"error": errMsg})
		default:
			c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
		}
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Task started"}) // 返回成功消息
}

// CancelSchedule 取消定时启动方法
func (h *TaskHandler) CancelSchedule(c *gin.Context) { // 取消定时启动
	taskID := c.Param("id") // 获取任务ID参数

	if err := h.taskService.CancelSchedule(taskID); err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "task not found") {
			c.JSON(http.StatusNotFound, gin.H{"error": errMsg})
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
		}
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Schedule cancelled"}) // 返回成功消息
}

// PauseTask 暂停任务方法
func (h *TaskHandler) PauseTask(c *gin.Context) { // 暂停指定任务
	taskID := c.Param("id") // 获取任务ID参数

	if err := h.taskService.PauseTask(taskID); err != nil { // 暂停任务
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()}) // 返回错误
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Task paused"}) // 返回成功消息
}

// GetTask 获取任务详情方法
func (h *TaskHandler) GetTask(c *gin.Context) { // 获取任务详情
	taskID := c.Param("id") // 获取任务ID参数

	task, exists := h.taskService.GetTask(taskID) // 获取任务
	if !exists {                                  // 如果任务不存在
		c.JSON(http.StatusNotFound, gin.H{"error": "Task not found"}) // 返回错误
		return
	}

	c.JSON(http.StatusOK, task) // 返回任务详情
}

// GetAllTasks 获取任务列表方法，支持分页/筛选/搜索/排序
func (h *TaskHandler) GetAllTasks(c *gin.Context) { // 获取所有任务列表
	page := 1
	pageSize := 10
	status := c.Query("status")
	keyword := c.Query("keyword")
	sortBy := c.Query("sort")

	if v := c.Query("page"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			page = n
		}
	}
	if v := c.Query("page_size"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			pageSize = n
		}
	}

	items, total, page, pageSize := h.taskService.GetTasksPage(page, pageSize, status, keyword, sortBy)
	c.JSON(http.StatusOK, gin.H{
		"total":     total,
		"page":      page,
		"page_size": pageSize,
		"items":     items,
	})
}

// GetTaskMetrics 获取任务指标方法
func (h *TaskHandler) GetTaskMetrics(c *gin.Context) { // 获取任务指标
	taskID := c.Param("id") // 获取任务ID参数

	metrics, err := h.taskService.GetTaskMetrics(taskID) // 获取任务指标
	if err != nil {                                      // 如果获取失败
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()}) // 返回错误
		return
	}

	c.JSON(http.StatusOK, metrics) // 返回任务指标
}

// SkipError 跳过错误方法
func (h *TaskHandler) SkipError(c *gin.Context) { // 跳过当前错误
	taskID := c.Param("id") // 获取任务ID参数

	if err := h.taskService.SkipError(taskID); err != nil { // 跳过错误
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()}) // 返回错误
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Error skipped"}) // 返回成功消息
}

// GetDefaultConfig 获取当前全局配置方法
func (h *TaskHandler) GetDefaultConfig(c *gin.Context) { // 获取全局配置
	cfg := config.GlobalConfig // 获取全局配置
	if cfg == nil {            // 如果配置为空
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Config not loaded"}) // 返回错误
		return
	}

	c.JSON(http.StatusOK, cfg) // 返回配置
}

// UpdateGlobalConfig 更新全局配置方法
func (h *TaskHandler) UpdateGlobalConfig(c *gin.Context) { // 更新全局配置
	var req config.Config                          // 定义配置请求变量
	if err := c.ShouldBindJSON(&req); err != nil { // 绑定JSON请求
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()}) // 返回错误
		return
	}

	oldCfg := config.GlobalConfig
	if oldCfg == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Config not loaded"})
		return
	}

	// 保留前端不会发送的敏感/内部字段（如加密密钥、同步调优参数）
	req.Security = oldCfg.Security
	req.Sync = oldCfg.Sync

	// 保存到文件
	if err := config.SaveConfig("etc/application.toml", &req); err != nil { // 保存配置到文件
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save config: " + err.Error()}) // 返回错误
		return
	}

	// 更新内存中的配置
	*oldCfg = req // 更新全局配置

	// 动态重初始化存储后端（如从 file 切换到 mysql 时会自动建表）
	hotReloaded := []string{}
	warnings := []string{}

	storageMsg := ""                                                         // 定义存储消息变量
	if err := h.taskService.ReinitStorage(config.GlobalConfig); err != nil { // 重初始化存储
		storageMsg = fmt.Sprintf("Warning: storage reinitialization failed: %v", err) // 设置错误消息
		warnings = append(warnings, storageMsg)
	} else if req.Storage.Mode == "mysql" { // 如果是MySQL存储模式
		storageMsg = "MySQL storage initialized, sys_sync_tasks table ensured" // 设置成功消息
		hotReloaded = append(hotReloaded, "storage")
	} else {
		hotReloaded = append(hotReloaded, "storage")
	}

	checkpointMsg := ""
	if err := h.taskService.ReinitCheckpointManager(config.GlobalConfig); err != nil {
		checkpointMsg = fmt.Sprintf("Warning: checkpoint manager reinitialization failed: %v", err)
		warnings = append(warnings, checkpointMsg)
	} else if req.Redis.Host != "" {
		checkpointMsg = "Redis checkpoint manager initialized"
		hotReloaded = append(hotReloaded, "redis")
	} else {
		checkpointMsg = "In-memory checkpoint manager initialized"
		hotReloaded = append(hotReloaded, "redis")
	}

	logMsg := ""
	logCfg := buildLoggerConfig(&req.Log)
	if err := logger.ReconfigureGlobal(logCfg); err != nil {
		logMsg = fmt.Sprintf("Warning: logger reconfigure failed: %v", err)
		warnings = append(warnings, logMsg)
	} else {
		logMsg = fmt.Sprintf("Logger reconfigured: level=%s, console=%v, file=%v", logCfg.Level, logCfg.Console, logCfg.File)
		hotReloaded = append(hotReloaded, "logger")
	}

	resp := gin.H{"message": "Configuration updated successfully"}
	if storageMsg != "" {
		resp["storage"] = storageMsg
	}
	if checkpointMsg != "" {
		resp["redis"] = checkpointMsg
	}
	if logMsg != "" {
		resp["logger"] = logMsg
	}
	if len(hotReloaded) > 0 {
		resp["hotReloaded"] = hotReloaded
	}
	if len(warnings) > 0 {
		resp["warnings"] = warnings
	}
	c.JSON(http.StatusOK, resp) // 返回响应
}

// buildLoggerConfig 从应用层 LogConfig 构建 logger.Config
func buildLoggerConfig(lc *config.LogConfig) *logger.Config {
	cfg := &logger.Config{
		Level:       lc.Level,
		Console:     lc.Console.Enable,
		File:        lc.File.Enable,
		EnableColor: !lc.Console.NoColor,
	}
	if cfg.Level == "" {
		cfg.Level = "info"
	}
	if !cfg.Console && !cfg.File {
		cfg.Console = true
		cfg.EnableColor = true
	}
	return cfg
}

// UpdateLogConfigRequest 日志配置热更新请求
type UpdateLogConfigRequest struct {
	Level   string                `json:"level"`
	Console *config.ConsoleConfig `json:"console,omitempty"`
	File    *config.FileConfig    `json:"file,omitempty"`
}

// GetLogConfig 获取当前运行时生效的日志配置
func (h *TaskHandler) GetLogConfig(c *gin.Context) {
	lc := logger.GetGlobalConfig()
	if lc == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Logger not initialized"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"level":        lc.Level,
		"console":      lc.Console,
		"file":         lc.File,
		"enable_color": lc.EnableColor,
	})
}

// UpdateLogConfig 独立的日志配置热加载接口——只修改日志级别和输出，不影响其他配置
func (h *TaskHandler) UpdateLogConfig(c *gin.Context) {
	var req UpdateLogConfigRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	cfg := config.GlobalConfig
	if cfg == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Config not loaded"})
		return
	}

	if req.Level != "" {
		cfg.Log.Level = req.Level
	}
	if req.Console != nil {
		cfg.Log.Console = *req.Console
	}
	if req.File != nil {
		cfg.Log.File = *req.File
	}

	logCfg := buildLoggerConfig(&cfg.Log)
	if err := logger.ReconfigureGlobal(logCfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Logger reconfigure failed: " + err.Error()})
		return
	}

	if err := config.SaveConfig("etc/application.toml", cfg); err != nil {
		logger.Warn("日志配置已热加载但持久化失败: %v", err)
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "日志配置已热加载",
		"level":   logCfg.Level,
		"console": logCfg.Console,
		"file":    logCfg.File,
	})
}

// MetadataHandler 元数据处理器结构体
type MetadataHandler struct {
	analyzer metadataService.IdentityAnalyzer
}

// NewMetadataHandler 创建元数据处理器函数
func NewMetadataHandler(analyzer metadataService.IdentityAnalyzer) *MetadataHandler { // 创建元数据处理器实例
	return &MetadataHandler{analyzer: analyzer} // 返回元数据处理器实例
}

// GetDatabases 获取数据库列表方法
func (h *MetadataHandler) GetDatabases(c *gin.Context) { // 获取数据库列表
	if h.analyzer == nil { // 如果分析器未初始化
		c.JSON(http.StatusBadRequest, gin.H{ // 返回错误
			"error": "Database not connected. Please create a task with database configuration first, or configure datasource in config file.",
		})
		return
	}

	databases, err := h.analyzer.GetAllDatabases() // 获取数据库列表
	if err != nil {                                // 如果获取失败
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()}) // 返回错误
		return
	}

	c.JSON(http.StatusOK, databases) // 返回数据库列表
}

// GetTables 获取表列表方法
func (h *MetadataHandler) GetTables(c *gin.Context) { // 获取指定数据库的表列表
	if h.analyzer == nil { // 如果分析器未初始化
		c.JSON(http.StatusBadRequest, gin.H{ // 返回错误
			"error": "Database not connected. Please create a task with database configuration first, or configure datasource in config file.",
		})
		return
	}

	schema := c.Query("schema") // 获取schema参数
	if schema == "" {           // 如果schema为空
		c.JSON(http.StatusBadRequest, gin.H{"error": "schema is required"}) // 返回错误
		return
	}

	tables, err := h.analyzer.GetAllTables(schema) // 获取表列表
	if err != nil {                                // 如果获取失败
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()}) // 返回错误
		return
	}

	c.JSON(http.StatusOK, tables) // 返回表列表
}

// RefreshMetadata 刷新元数据方法（重新从数据库加载）
func (h *MetadataHandler) RefreshMetadata(c *gin.Context) { // 刷新元数据
	// 元数据是从 information_schema 实时查询的
	// 这里只需要返回成功，前端会重新调用获取接口
	c.JSON(http.StatusOK, gin.H{ // 返回成功响应
		"message": "Metadata refresh triggered", // 提示消息
		"success": true,                         // 成功标志
	})
}

// GetTableIdentity 获取表标识信息方法
func (h *MetadataHandler) GetTableIdentity(c *gin.Context) { // 获取表的标识信息
	if h.analyzer == nil { // 如果分析器未初始化
		c.JSON(http.StatusBadRequest, gin.H{ // 返回错误
			"error": "Database not connected. Please create a task with database configuration first, or configure datasource in config file.",
		})
		return
	}

	schema := c.Query("schema")   // 获取schema参数
	tableName := c.Query("table") // 获取表名参数

	if schema == "" || tableName == "" { // 如果参数为空
		c.JSON(http.StatusBadRequest, gin.H{"error": "schema and table are required"}) // 返回错误
		return
	}

	identity, err := h.analyzer.AnalyzeTable(schema, tableName) // 分析表标识
	if err != nil {                                             // 如果分析失败
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()}) // 返回错误
		return
	}

	// 构建响应，包含风险提示
	response := gin.H{ // 创建响应对象
		"table_name":    identity.TableName,    // 表名
		"strategy":      identity.Strategy,     // 标识策略
		"identify_cols": identity.IdentifyCols, // 标识列
		"has_pk":        identity.HasPK,        // 是否有主键
		"has_uk":        identity.HasUK,        // 是否有唯一键
		"columns":       identity.Columns,      // 列信息
	}

	// 无主键表风险提示
	if identity.Strategy == "FULL_COLUMNS_STRATEGY" { // 如果是全列匹配策略
		response["warning"] = "该表将采用全列匹配模式，同步性能可能受限" // 添加警告信息
	}

	c.JSON(http.StatusOK, response) // 返回响应
}

// UpdateTask 更新任务方法
func (h *TaskHandler) UpdateTask(c *gin.Context) { // 更新任务配置
	taskID := c.Param("id")                        // 获取任务ID参数
	var req UpdateTaskRequest                      // 定义更新请求变量
	if err := c.ShouldBindJSON(&req); err != nil { // 绑定JSON请求
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()}) // 返回错误
		return
	}

	task, exists := h.taskService.GetTask(taskID) // 获取任务
	if !exists {                                  // 如果任务不存在
		c.JSON(http.StatusNotFound, gin.H{"error": "Task not found"}) // 返回错误
		return
	}

	// 只允许更新非运行状态的任务
	if task.Context.Status == taskEntity.TaskStatusRunning { // 如果任务正在运行
		c.JSON(http.StatusBadRequest, gin.H{"error": "Cannot update running task"}) // 返回错误
		return
	}

	// 更新任务配置
	if req.Name != "" { // 如果提供了名称
		task.Config.Name = req.Name // 更新名称
	}
	// 更新同步级别（支持大小写不敏感）
	if req.SyncLevel != "" { // 如果提供了同步级别
		if strings.ToUpper(req.SyncLevel) == "DATABASE" { // 如果是数据库级别
			task.Config.SyncLevel = taskEntity.SyncLevelDatabase // 设置为数据库级别
		} else { // 否则
			task.Config.SyncLevel = taskEntity.SyncLevelTable // 设置为表级别
		}
	}
	if req.SourceSchema != "" { // 如果提供了源schema
		task.Config.SourceSchema = req.SourceSchema // 更新源schema
	}
	if req.TargetSchema != "" { // 如果提供了目标schema
		task.Config.TargetSchema = req.TargetSchema // 更新目标schema
	}
	if len(req.SourceDatabases) > 0 || req.SyncLevel != "" { // 如果提供了源数据库列表或同步级别
		task.Config.SourceDatabases = req.SourceDatabases // 更新源数据库列表
	}
	if len(req.TargetDatabases) > 0 || req.SyncLevel != "" { // 如果提供了目标数据库列表或同步级别
		task.Config.TargetDatabases = req.TargetDatabases // 更新目标数据库列表
	}
	// 表列表：允许清空（库级别同步时 tables 为空）
	task.Config.Tables = req.Tables // 更新表列表
	if req.Mode != "" {             // 如果提供了同步模式
		task.Config.Mode = taskEntity.SyncMode(req.Mode) // 更新同步模式
	}
	if req.BatchSize > 0 { // 如果提供了批处理大小
		task.Config.BatchSize = req.BatchSize // 更新批处理大小
	}
	if req.WorkerCount > 0 { // 如果提供了工作线程数
		task.Config.WorkerCount = req.WorkerCount // 更新工作线程数
	}
	if req.IntraTableWorkerCount != nil { // 如果提供了表内工作线程数
		task.Config.IntraTableWorkerCount = *req.IntraTableWorkerCount // 更新表内工作线程数
	}
	// enable_limit_one 是 bool 类型，直接赋值
	task.Config.EnableLimitOne = req.EnableLimitOne // 更新LIMIT 1优化开关
	if req.OptimizeIndex != nil {                   // 如果提供了索引优化开关
		task.Config.OptimizeIndex = *req.OptimizeIndex // 更新索引优化开关
	}
	if req.EnableReadOnly != nil { // 如果提供了只读管理开关
		task.Config.EnableReadOnly = *req.EnableReadOnly // 更新只读管理开关
	}
	if req.EnableDropTableBeforeDDL != nil { // 如果提供了DDL前DROP TABLE开关
		task.Config.EnableDropTableBeforeDDL = *req.EnableDropTableBeforeDDL // 更新DDL前DROP TABLE开关
	}

	// 更新数据库配置
	if req.SourceDB != nil { // 如果提供了源数据库配置
		task.Config.SourceDB = &taskEntity.DatabaseConfig{ // 更新源数据库配置
			Host:     req.SourceDB.Host,     // 设置主机
			Port:     req.SourceDB.Port,     // 设置端口
			Database: req.SourceDB.Database, // 设置数据库名
			Username: req.SourceDB.Username, // 设置用户名
			Password: req.SourceDB.Password, // 设置密码
		}
	}
	if req.TargetDB != nil { // 如果提供了目标数据库配置
		task.Config.TargetDB = &taskEntity.DatabaseConfig{ // 更新目标数据库配置
			Host:     req.TargetDB.Host,     // 设置主机
			Port:     req.TargetDB.Port,     // 设置端口
			Database: req.TargetDB.Database, // 设置数据库名
			Username: req.TargetDB.Username, // 设置用户名
			Password: req.TargetDB.Password, // 设置密码
		}
	}

	if err := h.taskService.UpdateTask(task); err != nil { // 更新任务
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()}) // 返回错误
		return
	}

	c.JSON(http.StatusOK, task) // 返回更新后的任务
}

// DeleteTask 删除任务方法
func (h *TaskHandler) DeleteTask(c *gin.Context) { // 删除任务
	taskID := c.Param("id") // 获取任务ID参数

	task, exists := h.taskService.GetTask(taskID) // 获取任务
	if !exists {                                  // 如果任务不存在
		c.JSON(http.StatusNotFound, gin.H{"error": "Task not found"}) // 返回错误
		return
	}

	// 只允许删除非运行状态的任务
	if task.Context.Status == taskEntity.TaskStatusRunning { // 如果任务正在运行
		c.JSON(http.StatusBadRequest, gin.H{"error": "Cannot delete running task"}) // 返回错误
		return
	}

	if err := h.taskService.DeleteTask(taskID); err != nil { // 删除任务
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()}) // 返回错误
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Task deleted"}) // 返回成功消息
}

// UpdateTaskRequest 更新任务请求结构体
type UpdateTaskRequest struct { // 定义更新任务请求结构体
	Name                     string                 `json:"name"`                                   // 任务名称
	SyncLevel                string                 `json:"sync_level"`                             // 同步级别
	SourceSchema             string                 `json:"source_schema"`                          // 源模式名
	TargetSchema             string                 `json:"target_schema"`                          // 目标模式名
	SourceDatabases          []string               `json:"source_databases"`                       // 源数据库列表
	TargetDatabases          []string               `json:"target_databases"`                       // 目标数据库列表
	Tables                   []string               `json:"tables"`                                 // 源表列表
	TargetTables             []string               `json:"target_tables"`                          // 目标表列表（与 Tables 一一对应；空则沿用源表名）
	Mode                     string                 `json:"mode"`                                   // 同步模式
	BatchSize                int                    `json:"batch_size"`                             // 批处理大小
	WorkerCount              int                    `json:"worker_count"`                           // 工作线程数
	IntraTableWorkerCount    *int                   `json:"intra_table_worker_count,omitempty"`     // 表内工作线程数（可选）
	EnableLimitOne           bool                   `json:"enable_limit_one"`                       // 是否启用LIMIT 1优化
	OptimizeIndex            *bool                  `json:"optimize_index,omitempty"`               // 索引优化（可选）
	EnableReadOnly           *bool                  `json:"enable_read_only,omitempty"`             // 只读管理（可选）
	EnableDropTableBeforeDDL *bool                  `json:"enable_drop_table_before_ddl,omitempty"` // DDL前是否先DROP TABLE（可选）
	SourceDB                 *DatabaseConfigRequest `json:"source_db,omitempty"`                    // 源数据库配置（可选）
	TargetDB                 *DatabaseConfigRequest `json:"target_db,omitempty"`                    // 目标数据库配置（可选）
}

// generateID 生成唯一ID函数
func generateID() string { // 生成唯一的任务ID
	return "task_" + randomString(8) // 返回task_前缀加上随机字符串
}

// randomString 生成随机字符串函数
func randomString(n int) string { // 生成指定长度的随机字符串
	b := make([]byte, n)                    // 创建字节切片
	if _, err := rand.Read(b); err != nil { // 读取随机字节
		// 如果加密随机数生成失败，使用时间戳作为备用方案
		return hex.EncodeToString([]byte(time.Now().Format("20060102150405")))[:n] // 使用时间戳编码
	}
	return hex.EncodeToString(b)[:n] // 返回十六进制编码的随机字符串
}

// TestConnectionRequest 测试连接请求结构体
type TestConnectionRequest struct { // 定义测试连接请求结构体
	Host     string `json:"host"`     // 数据库主机地址
	Port     int    `json:"port"`     // 数据库端口
	Username string `json:"username"` // 用户名
	Password string `json:"password"` // 密码
	Database string `json:"database"` // 数据库名称
}

// TestConnection 测试数据库连接方法
func (h *TaskHandler) TestConnection(c *gin.Context) { // 测试数据库连接
	var req TestConnectionRequest                  // 定义请求变量
	if err := c.ShouldBindJSON(&req); err != nil { // 绑定JSON请求
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()}) // 返回错误
		return
	}

	// 构建DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&timeout=5s", // 构建数据源名称
		req.Username, // 用户名
		req.Password, // 密码
		req.Host,     // 主机
		req.Port,     // 端口
		req.Database, // 数据库名
	)

	// 尝试连接
	db, err := sql.Open("mysql", dsn) // 打开数据库连接
	if err != nil {                   // 如果打开失败
		c.JSON(http.StatusOK, gin.H{ // 返回错误响应
			"success": false,                        // 成功标志为false
			"message": fmt.Sprintf("连接失败: %v", err), // 错误消息
		})
		return
	}
	defer db.Close() // 延迟关闭数据库连接

	// 设置超时
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // 创建5秒超时的上下文
	defer cancel()                                                          // 延迟取消上下文

	// 测试连接
	if err := db.PingContext(ctx); err != nil { // 测试数据库连接
		c.JSON(http.StatusOK, gin.H{ // 返回错误响应
			"success": false,                        // 成功标志为false
			"message": fmt.Sprintf("连接失败: %v", err), // 错误消息
		})
		return
	}

	// 获取版本信息
	var version string                                                                 // 定义版本变量
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil { // 查询数据库版本
		version = "unknown" // 如果查询失败，设置为unknown
	}

	c.JSON(http.StatusOK, gin.H{ // 返回成功响应
		"success": true,                                    // 成功标志为true
		"message": fmt.Sprintf("连接成功 (MySQL %s)", version), // 成功消息
		"version": version,                                 // 版本信息
	})
}

// GetDatabasesWithConfig 使用自定义配置获取数据库列表方法
func (h *MetadataHandler) GetDatabasesWithConfig(c *gin.Context) { // 使用自定义配置获取数据库列表
	var req TestConnectionRequest                  // 定义请求变量
	if err := c.ShouldBindJSON(&req); err != nil { // 绑定JSON请求
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()}) // 返回错误
		return
	}

	// 构建DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local", // 构建数据源名称
		req.Username, // 用户名
		req.Password, // 密码
		req.Host,     // 主机
		req.Port,     // 端口
		req.Database, // 数据库名
	)

	// 连接数据库
	db, err := sql.Open("mysql", dsn) // 打开数据库连接
	if err != nil {                   // 如果打开失败
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("连接失败: %v", err)}) // 返回错误
		return
	}
	defer db.Close() // 延迟关闭数据库连接

	// 创建分析器
	schemaDetector := infrastructure.NewSchemaDetector(db)         // 创建模式检测器
	analyzer := service.NewIdentityAnalyzerService(schemaDetector) // 创建标识符分析器

	// 获取数据库列表
	databases, err := analyzer.GetAllDatabases() // 获取所有数据库
	if err != nil {                              // 如果获取失败
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()}) // 返回错误
		return
	}

	c.JSON(http.StatusOK, databases) // 返回数据库列表
}

// GetTablesWithConfig 使用自定义配置获取表列表方法
func (h *MetadataHandler) GetTablesWithConfig(c *gin.Context) { // 使用自定义配置获取表列表
	var req struct { // 定义请求结构体
		TestConnectionRequest        // 嵌入测试连接请求
		Schema                string `json:"schema"` // Schema参数
	}
	if err := c.ShouldBindJSON(&req); err != nil { // 绑定JSON请求
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()}) // 返回错误
		return
	}

	if req.Schema == "" { // 如果schema为空
		c.JSON(http.StatusBadRequest, gin.H{"error": "schema is required"}) // 返回错误
		return
	}

	// 构建DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local", // 构建数据源名称
		req.Username, // 用户名
		req.Password, // 密码
		req.Host,     // 主机
		req.Port,     // 端口
		req.Database, // 数据库名
	)

	// 连接数据库
	db, err := sql.Open("mysql", dsn) // 打开数据库连接
	if err != nil {                   // 如果打开失败
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("连接失败: %v", err)}) // 返回错误
		return
	}
	defer db.Close() // 延迟关闭数据库连接

	// 创建分析器
	schemaDetector := infrastructure.NewSchemaDetector(db)         // 创建模式检测器
	analyzer := service.NewIdentityAnalyzerService(schemaDetector) // 创建标识符分析器

	// 获取表列表
	tables, err := analyzer.GetAllTables(req.Schema) // 获取指定schema的所有表
	if err != nil {                                  // 如果获取失败
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()}) // 返回错误
		return
	}

	c.JSON(http.StatusOK, tables) // 返回表列表
}
