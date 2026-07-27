package router // 声明当前文件属于router包，用于路由配置

import ( // 导入外部包和内部模块
	"net/http" // 导入net/http包，用于HTTP服务器
	"os"       // 导入os包，用于操作系统接口
	"time"     // 导入time包，用于时间处理

	"github.com/gin-gonic/gin"                                // 导入Gin框架，用于构建HTTP API
	"github.com/prometheus/client_golang/prometheus/promhttp" // 导入Prometheus HTTP处理器

	"mysql-to-sync/internal/api/handler"                          // 导入处理器模块
	"mysql-to-sync/internal/config"                               // 导入配置模块
	"mysql-to-sync/internal/metadata/domain/service"              // 导入元数据领域服务
	"mysql-to-sync/internal/metrics"                              // 导入指标模块
	taskService "mysql-to-sync/internal/task/application/service" // 导入任务应用服务
)

// SetupRouter 设置路由函数
func SetupRouter(taskSvc *taskService.TaskService, analyzer service.IdentityAnalyzer, cfg *config.Config) *gin.Engine { // 设置HTTP路由
	// 确保在创建 Engine 之前设置了正确的模式
	if cfg != nil && cfg.Datasource.Debug { // 如果配置了调试模式
		gin.SetMode(gin.DebugMode) // 设置Gin为调试模式
	} else { // 否则
		gin.SetMode(gin.ReleaseMode) // 设置Gin为发布模式
	}

	// 强制设置 Gin 的输出到控制台，确保日志可见
	gin.DefaultWriter = os.Stdout      // 设置Gin的默认输出为标准输出
	gin.DefaultErrorWriter = os.Stderr // 设置Gin的错误输出为标准错误

	// 创建 Engine
	router := gin.New() // 创建新的Gin引擎

	// 无论如何，强制加上 Logger 和 Recovery 中间件
	router.Use(gin.Logger())   // 使用日志中间件
	router.Use(gin.Recovery()) // 使用恢复中间件，防止panic导致服务崩溃

	// 配置CORS
	router.Use(func(c *gin.Context) { // 使用CORS中间件
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")                                // 设置允许的来源为所有
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS") // 设置允许的HTTP方法
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")     // 设置允许的请求头
		if c.Request.Method == "OPTIONS" {                                                       // 如果是OPTIONS请求
			c.AbortWithStatus(204) // 返回204状态码
			return                 // 直接返回
		}
		c.Next() // 继续处理下一个中间件
	})

	// 创建处理器
	taskHandler := handler.NewTaskHandler(taskSvc, analyzer) // 创建任务处理器
	taskEventHandler := handler.NewTaskEventHandler(taskSvc)
	metadataHandler := handler.NewMetadataHandler(analyzer)  // 创建元数据处理器

	// API分组
	api := router.Group("/api") // 创建API路由分组
	{                           // 分组开始
		// 任务管理
		tasks := api.Group("/tasks") // 创建任务路由分组
		{                            // 分组开始
			tasks.GET("/sort-options", taskHandler.GetTaskSortOptions)     // 获取任务排序选项：GET /api/tasks/sort-options
			tasks.POST("", taskHandler.CreateTask)                         // 创建任务路由：POST /api/tasks
			tasks.GET("", taskHandler.GetAllTasks)                         // 获取所有任务路由：GET /api/tasks
			tasks.GET("/:id", taskHandler.GetTask)                         // 获取任务详情路由：GET /api/tasks/:id
			tasks.PUT("/:id", taskHandler.UpdateTask)                      // 更新任务路由：PUT /api/tasks/:id
			tasks.DELETE("/:id", taskHandler.DeleteTask)                   // 删除任务路由：DELETE /api/tasks/:id
			tasks.POST("/:id/start", taskHandler.StartTask)                // 启动任务路由：POST /api/tasks/:id/start
			tasks.POST("/:id/pause", taskHandler.PauseTask)                // 暂停任务路由：POST /api/tasks/:id/pause
			tasks.POST("/:id/end", taskHandler.EndTask)                    // 结束 ALL 任务路由（增量阶段手动结束，进入 STOPPED 终态）：POST /api/tasks/:id/end
			tasks.POST("/:id/row-count-comparison", taskHandler.RowCountComparison) // 行数对比路由（后台核对源/目标端 COUNT(*)）：POST /api/tasks/:id/row-count-comparison
			tasks.GET("/:id/metrics", taskHandler.GetTaskMetrics)          // 获取任务指标路由：GET /api/tasks/:id/metrics
			tasks.GET("/:id/progress", taskHandler.GetTaskProgress)        // 获取任务运行时进度路由：GET /api/tasks/:id/progress
			tasks.POST("/:id/skip", taskHandler.SkipError)                 // 跳过错误路由：POST /api/tasks/:id/skip
			tasks.POST("/:id/cancel-schedule", taskHandler.CancelSchedule) // 取消定时启动路由：POST /api/tasks/:id/cancel-schedule
			tasks.GET("/:id/events", taskEventHandler.ListTaskEvents)
			tasks.GET("/:id/event-executions", taskEventHandler.ListTaskEventExecutions)
		} // 分组结束

		// 配置
		api.GET("/config/default", taskHandler.GetDefaultConfig)
		api.POST("/config/update", taskHandler.UpdateGlobalConfig)
		api.POST("/config/test-connection", taskHandler.TestConnection)

		// 日志配置热加载
		api.GET("/config/log", taskHandler.GetLogConfig)
		api.POST("/config/log", taskHandler.UpdateLogConfig)

		// 元数据
		metadata := api.Group("/metadata") // 创建元数据路由分组
		{                                  // 分组开始
			metadata.GET("/databases", metadataHandler.GetDatabases)                        // 获取数据库列表路由：GET /api/metadata/databases
			metadata.GET("/tables", metadataHandler.GetTables)                              // 获取表列表路由：GET /api/metadata/tables
			metadata.GET("/identity", metadataHandler.GetTableIdentity)                     // 获取表标识信息路由：GET /api/metadata/identity
			metadata.POST("/refresh", metadataHandler.RefreshMetadata)                      // 刷新元数据路由：POST /api/metadata/refresh
			metadata.POST("/databases-with-config", metadataHandler.GetDatabasesWithConfig) // 使用自定义配置获取数据库列表路由：POST /api/metadata/databases-with-config
			metadata.POST("/tables-with-config", metadataHandler.GetTablesWithConfig)       // 使用自定义配置获取表列表路由：POST /api/metadata/tables-with-config
		} // 分组结束

		// 健康检查接口
		api.GET("/health", func(c *gin.Context) { // 健康检查路由：GET /api/health
			health := gin.H{ // 创建健康检查响应
				"status":    "ok",                            // 状态为ok
				"timestamp": time.Now().Format(time.RFC3339), // 当前时间戳
				"version":   "1.0.0",                         // 版本号
				"uptime":    time.Now().Format(time.RFC3339), // 运行时间（实际应该记录启动时间）
				"tasks": gin.H{ // 任务信息
					"total":   len(taskSvc.GetAllTasks()),    // 总任务数
					"running": taskSvc.GetRunningTaskCount(), // 运行中的任务数
				},
			}
			c.JSON(http.StatusOK, health) // 返回健康检查结果
		})
	} // 分组结束

	// 简单健康检查（用于负载均衡器）
	router.GET("/health", func(c *gin.Context) { // 简单健康检查路由：GET /health
		c.JSON(http.StatusOK, gin.H{ // 返回健康检查结果
			"status": "ok", // 状态为ok
		})
	})

	// Prometheus指标端点
	router.GET("/metrics", gin.WrapH(promhttp.Handler())) // Prometheus指标路由：GET /metrics

	// 更新指标中间件
	router.Use(func(c *gin.Context) { // 使用指标更新中间件
		c.Next() // 继续处理请求

		// 每次请求后更新指标
		tasks := taskSvc.GetAllTasks() // 获取所有任务
		total := len(tasks)            // 计算总任务数
		running := 0                   // 初始化运行中任务数
		completed := 0                 // 初始化已完成任务数
		failed := 0                    // 初始化失败任务数

		for _, task := range tasks { // 遍历所有任务
			switch task.Context.Status { // 根据任务状态分类计数
			case "RUNNING": // 如果任务正在运行
				running++ // 运行中任务数加1
			case "COMPLETED": // 如果任务已完成
				completed++ // 已完成任务数加1
			case "FAILED": // 如果任务已失败
				failed++ // 失败任务数加1
			}
		}

		metrics.GetMetrics().UpdateTaskMetrics(total, running, completed, failed) // 更新任务指标
	})

	// 静态文件服务 - 提供前端页面
	router.Static("/web", "./web") // 静态文件路由：/web/*

	// 根路径重定向到前端页面
	router.GET("/", func(c *gin.Context) { // 根路径路由：GET /
		c.Redirect(302, "/web/index.html") // 重定向到前端页面
	})

	return router // 返回路由实例
}
