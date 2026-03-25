package router

import (
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"mysql-to-async/internal/api/handler"
	"mysql-to-async/internal/config"
	"mysql-to-async/internal/metadata/domain/service"
	"mysql-to-async/internal/metrics"
	taskService "mysql-to-async/internal/task/application/service"
)

// SetupRouter 设置路由
func SetupRouter(taskSvc *taskService.TaskService, analyzer service.IdentityAnalyzer, cfg *config.Config) *gin.Engine {
	// 确保在创建 Engine 之前设置了正确的模式
	if cfg != nil && cfg.Datasource.Debug {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	// 强制设置 Gin 的输出到控制台，确保日志可见
	gin.DefaultWriter = os.Stdout
	gin.DefaultErrorWriter = os.Stderr

	// 创建 Engine
	router := gin.New()

	// 无论如何，强制加上 Logger 和 Recovery 中间件
	router.Use(gin.Logger())
	router.Use(gin.Recovery())

	// 配置CORS
	router.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	// 创建处理器
	taskHandler := handler.NewTaskHandler(taskSvc, analyzer)
	metadataHandler := handler.NewMetadataHandler(analyzer)

	// API分组
	api := router.Group("/api")
	{
		// 任务管理
		tasks := api.Group("/tasks")
		{
			tasks.POST("", taskHandler.CreateTask)                // 创建任务
			tasks.GET("", taskHandler.GetAllTasks)                // 获取所有任务
			tasks.GET("/:id", taskHandler.GetTask)                // 获取任务详情
			tasks.PUT("/:id", taskHandler.UpdateTask)             // 更新任务
			tasks.DELETE("/:id", taskHandler.DeleteTask)          // 删除任务
			tasks.POST("/:id/start", taskHandler.StartTask)       // 启动任务
			tasks.POST("/:id/pause", taskHandler.PauseTask)       // 暂停任务
			tasks.GET("/:id/metrics", taskHandler.GetTaskMetrics) // 获取任务指标
			tasks.POST("/:id/skip", taskHandler.SkipError)        // 跳过错误
		}

		// 配置
		api.GET("/config/default", taskHandler.GetDefaultConfig)        // 获取默认配置
		api.POST("/config/update", taskHandler.UpdateGlobalConfig)      // 更新全局配置
		api.POST("/config/test-connection", taskHandler.TestConnection) // 测试数据库连接

		// 元数据
		metadata := api.Group("/metadata")
		{
			metadata.GET("/databases", metadataHandler.GetDatabases)                        // 获取数据库列表
			metadata.GET("/tables", metadataHandler.GetTables)                              // 获取表列表
			metadata.GET("/identity", metadataHandler.GetTableIdentity)                     // 获取表标识信息
			metadata.POST("/refresh", metadataHandler.RefreshMetadata)                      // 刷新元数据
			metadata.POST("/databases-with-config", metadataHandler.GetDatabasesWithConfig) // 使用自定义配置获取数据库列表
			metadata.POST("/tables-with-config", metadataHandler.GetTablesWithConfig)       // 使用自定义配置获取表列表
		}

		// 健康检查接口
		api.GET("/health", func(c *gin.Context) {
			health := gin.H{
				"status":    "ok",
				"timestamp": time.Now().Format(time.RFC3339),
				"version":   "1.0.0",
				"uptime":    time.Now().Format(time.RFC3339), // 实际应该记录启动时间
				"tasks": gin.H{
					"total":   len(taskSvc.GetAllTasks()),
					"running": taskSvc.GetRunningTaskCount(),
				},
			}
			c.JSON(http.StatusOK, health)
		})
	}

	// 简单健康检查（用于负载均衡器）
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status": "ok",
		})
	})

	// Prometheus指标端点
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// 更新指标中间件
	router.Use(func(c *gin.Context) {
		c.Next()

		// 每次请求后更新指标
		tasks := taskSvc.GetAllTasks()
		total := len(tasks)
		running := 0
		completed := 0
		failed := 0

		for _, task := range tasks {
			switch task.Context.Status {
			case "RUNNING":
				running++
			case "COMPLETED":
				completed++
			case "FAILED":
				failed++
			}
		}

		metrics.GetMetrics().UpdateTaskMetrics(total, running, completed, failed)
	})

	// 静态文件服务 - 提供前端页面
	router.Static("/web", "./web")

	// 根路径重定向到前端页面
	router.GET("/", func(c *gin.Context) {
		c.Redirect(302, "/web/index.html")
	})

	return router
}
