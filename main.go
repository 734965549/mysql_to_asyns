package main // 声明当前文件属于main包，是程序的入口

import ( // 导入外部包和内部模块
	"context"      // 导入context包，用于处理请求超时和取消
	"database/sql" // 导入database/sql包，用于数据库操作
	"fmt"          // 导入fmt包，用于格式化输入输出
	"net/http"     // 导入net/http包，用于HTTP服务器
	"os"           // 导入os包，用于操作系统接口
	"os/signal"    // 导入os/signal包，用于捕获系统信号
	"syscall"      // 导入syscall包，用于系统调用
	"time"         // 导入time包，用于时间处理

	"github.com/gin-gonic/gin"         // 导入Gin框架，用于构建HTTP API
	_ "github.com/go-sql-driver/mysql" // 导入MySQL驱动，下划线表示仅导入init函数

	"mysql-to-sync/internal/api/router"                           // 导入路由模块
	"mysql-to-sync/internal/config"                               // 导入配置模块
	"mysql-to-sync/internal/metadata/domain/service"              // 导入元数据领域服务
	"mysql-to-sync/internal/metadata/infrastructure"              // 导入元数据基础设施
	taskService "mysql-to-sync/internal/task/application/service" // 导入任务应用服务
	"mysql-to-sync/pkg/logger"                                    // 导入logger包，用于日志输出
)

func main() { // main函数是程序的入口点
	// 加载配置（仅用于开发环境的默认值，不是必需）
	cfg, cfgErr := config.LoadConfig("etc/application.toml") // 从指定路径加载配置文件
	if cfgErr != nil {                                     // 如果加载配置文件失败
		cfg = &config.Config{ // 创建默认配置对象
			Http: config.HttpConfig{ // 设置HTTP配置
				Host: "127.0.0.1", // 设置默认监听地址
				Port: 8080,        // 设置默认监听端口
			},
		}
		if envErr := config.ApplyEnvOverrides(cfg); envErr != nil {
			fmt.Fprintf(os.Stderr, "Failed to apply environment overrides: %v\n", envErr)
			os.Exit(1)
		}
		config.GlobalConfig = cfg
	}

	// Initialize logger
	logCfg := &logger.Config{
		Level:       cfg.Log.Level,
		Console:     cfg.Log.Console.Enable,
		File:        cfg.Log.File.Enable,
		EnableColor: !cfg.Log.Console.NoColor,
	}
	if logCfg.Level == "" {
		logCfg.Level = "info"
	}
	if !logCfg.Console && !logCfg.File {
		logCfg.Console = true
		logCfg.EnableColor = true
	}
	logger.Init(logCfg)
	defer logger.Close()

	if cfgErr != nil {
		logger.Warn("Failed to load config file: %v, using empty config", cfgErr)
	}

	// 创建任务服务（不注入数据库连接，在执行任务时动态创建）
	taskSvc := taskService.NewTaskService(cfg) // 创建任务服务实例

	// 创建元数据分析器
	// 如果配置文件中有完整的数据库配置，则自动建立连接
	var analyzer service.IdentityAnalyzer // 声明分析器变量
	if cfg.Datasource.Host != "" {        // 检查是否配置了数据源主机
		logger.Info("Connecting to datasource database %s:%d/%s...", cfg.Datasource.Host, cfg.Datasource.Port, cfg.Datasource.Database) // 输出连接信息
		dsn := cfg.Datasource.GetDSN()                                                                                                 // 获取数据源名称
		db, err := sql.Open("mysql", dsn)                                                                                              // 打开数据库连接
		if err != nil {                                                                                                                // 如果连接失败
			logger.Warn("Failed to connect to datasource database: %v", err) // 输出警告日志
		} else { // 如果连接成功
			// 测试连接
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // 创建带超时的上下文
			if err := db.PingContext(ctx); err != nil {                             // 测试数据库连接
				logger.Warn("Failed to ping datasource database: %v", err) // 输出警告日志
			} else { // 如果连接测试成功
				logger.Info("Successfully connected to datasource database")          // 输出成功日志
				config.ApplySyncMySQLPool(db, &cfg.Sync, true, "metadata-datasource") // 应用连接池配置
				// 创建分析器
				schemaDetector := infrastructure.NewSchemaDetector(db)        // 创建模式检测器
				analyzer = service.NewIdentityAnalyzerService(schemaDetector) // 创建标识符分析器服务
			}
			cancel() // 取消上下文
		}
	} else { // 如果未配置数据源
		logger.Info("No datasource configuration found in config file, metadata browser will use task-specific database connections") // 输出提示信息
	}

	// 1. 设置 Gin 模式
	if cfg.Datasource.Debug { // 如果配置了调试模式
		gin.SetMode(gin.DebugMode) // 设置Gin为调试模式
	} else { // 如果未配置调试模式
		gin.SetMode(gin.ReleaseMode) // 设置Gin为发布模式
	}

	// 2. 强制设置 Gin 的输出到控制台，确保日志可见
	gin.DefaultWriter = os.Stdout      // 设置Gin的默认输出到标准输出
	gin.DefaultErrorWriter = os.Stderr // 设置Gin的错误输出到标准错误

	// 3. 设置路由
	r := router.SetupRouter(taskSvc, analyzer, cfg) // 设置路由

	// 创建HTTP服务器
	srv := &http.Server{ // 创建HTTP服务器实例
		Addr:    fmt.Sprintf("%s:%d", cfg.Http.Host, cfg.Http.Port), // 设置监听地址
		Handler: r,                                                  // 设置路由处理器
	}

	// 启动HTTP服务（在goroutine中）
	go func() { // 在新的goroutine中启动HTTP服务
		logger.Info("Server starting on %s", srv.Addr)                       // 输出启动信息
		logger.Info("API Endpoints:")                                         // 输出API端点提示
		logger.Info("  POST   /api/tasks           - Create task")            // 输出创建任务端点
		logger.Info("  GET    /api/tasks           - List all tasks")       // 输出列出任务端点
		logger.Info("  GET    /api/tasks/:id       - Get task details")     // 输出获取任务详情端点
		logger.Info("  POST   /api/tasks/:id/start - Start task")           // 输出启动任务端点
		logger.Info("  POST   /api/tasks/:id/pause - Pause task")           // 输出暂停任务端点
		logger.Info("  GET    /api/tasks/:id/metrics - Get task metrics")   // 输出获取任务指标端点
		logger.Info("  POST   /api/tasks/:id/skip  - Skip error")           // 输出跳过错误端点
		logger.Info("  GET    /api/metadata/tables - List tables")          // 输出列出表端点
		logger.Info("  GET    /api/metadata/identity - Get table identity") // 输出获取表标识端点
		logger.Info("  GET    /api/health          - Health check")         // 输出健康检查端点

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed { // 启动HTTP服务器
			logger.Fatal("Failed to start server: %v", err) // 如果启动失败则输出错误并退出
		}
	}()

	// 等待中断信号以优雅关闭服务器
	quit := make(chan os.Signal, 1)                      // 创建信号通道
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM) // 注册信号监听
	<-quit                                               // 阻塞等待信号
	logger.Info("Shutting down server...")               // 输出关闭信息

	// 给优雅关闭最多等待30秒
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // 创建30秒超时的上下文
	defer cancel()                                                           // 延迟取消上下文

	// 1. 关闭HTTP服务器（停止接受新请求）
	if err := srv.Shutdown(ctx); err != nil { // 关闭HTTP服务器
		logger.Error("Server forced to shutdown: %v", err) // 如果关闭失败则输出错误
	}

	// 2. 关闭任务服务（保存所有任务状态并关闭数据库连接）
	if err := taskSvc.Close(); err != nil { // 关闭任务服务
		logger.Error("Error closing task service: %v", err) // 如果关闭失败则输出错误
	}

	logger.Info("Server exited") // 输出退出信息
}
