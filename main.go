package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"

	"mysql-to-async/internal/api/router"
	"mysql-to-async/internal/config"
	"mysql-to-async/internal/metadata/domain/service"
	"mysql-to-async/internal/metadata/infrastructure"
	taskService "mysql-to-async/internal/task/application/service"
)

func main() {
	// 加载配置（仅用于开发环境的默认值，不是必需）
	cfg, err := config.LoadConfig("etc/application.toml")
	if err != nil {
		log.Printf("Warning: Failed to load config file: %v, using empty config", err)
		cfg = &config.Config{
			Http: config.HttpConfig{
				Host: "127.0.0.1",
				Port: 8081,
			},
		}
	}

	// 创建任务服务（不注入数据库连接，在执行任务时动态创建）
	taskSvc := taskService.NewTaskService(cfg)

	// 创建元数据分析器
	// 如果配置文件中有完整的数据库配置，则自动建立连接
	var analyzer service.IdentityAnalyzer
	if cfg.Datasource.Host != "" {
		log.Printf("Connecting to datasource database %s:%d/%s...", cfg.Datasource.Host, cfg.Datasource.Port, cfg.Datasource.Database)
		dsn := cfg.Datasource.GetDSN()
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			log.Printf("Warning: Failed to connect to datasource database: %v", err)
		} else {
			// 测试连接
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := db.PingContext(ctx); err != nil {
				log.Printf("Warning: Failed to ping datasource database: %v", err)
			} else {
				log.Printf("Successfully connected to datasource database")
				config.ApplySyncMySQLPool(db, &cfg.Sync, true, "metadata-datasource")
				// 创建分析器
				schemaDetector := infrastructure.NewSchemaDetector(db)
				analyzer = service.NewIdentityAnalyzerService(schemaDetector)
			}
			cancel()
		}
	} else {
		log.Printf("No datasource configuration found in config file, metadata browser will use task-specific database connections")
	}

	// 1. 设置 Gin 模式
	if cfg.Datasource.Debug {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	// 2. 强制设置 Gin 的输出到控制台，确保日志可见
	gin.DefaultWriter = os.Stdout
	gin.DefaultErrorWriter = os.Stderr

	// 3. 设置路由
	r := router.SetupRouter(taskSvc, analyzer, cfg)

	// 创建HTTP服务器
	srv := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", cfg.Http.Host, cfg.Http.Port),
		Handler: r,
	}

	// 启动HTTP服务（在goroutine中）
	go func() {
		log.Printf("Server starting on %s", srv.Addr)
		log.Println("API Endpoints:")
		log.Println("  POST   /api/tasks           - Create task")
		log.Println("  GET    /api/tasks           - List all tasks")
		log.Println("  GET    /api/tasks/:id       - Get task details")
		log.Println("  POST   /api/tasks/:id/start - Start task")
		log.Println("  POST   /api/tasks/:id/pause - Pause task")
		log.Println("  GET    /api/tasks/:id/metrics - Get task metrics")
		log.Println("  POST   /api/tasks/:id/skip  - Skip error")
		log.Println("  GET    /api/metadata/tables - List tables")
		log.Println("  GET    /api/metadata/identity - Get table identity")
		log.Println("  GET    /api/health          - Health check")

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// 等待中断信号以优雅关闭服务器
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	// 给优雅关闭最多等待30秒
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. 关闭HTTP服务器（停止接受新请求）
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	// 2. 关闭任务服务（保存所有任务状态并关闭数据库连接）
	if err := taskSvc.Close(); err != nil {
		log.Printf("Error closing task service: %v", err)
	}

	log.Println("Server exited")
}
