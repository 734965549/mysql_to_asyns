package config

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
)

// Validator 配置验证器
type Validator struct {
	config *Config
}

// NewValidator 创建配置验证器
func NewValidator(cfg *Config) *Validator {
	return &Validator{config: cfg}
}

// ValidateConfig 验证配置格式（不验证数据库连接）
func (v *Validator) ValidateConfig() error {
	log.Println("Validating configuration format...")

	// 验证HTTP配置
	if err := v.ValidateHTTP(); err != nil {
		return fmt.Errorf("http validation failed: %w", err)
	}

	log.Println("Configuration format validation passed ✓")
	return nil
}

// ValidateAll 验证所有配置
func (v *Validator) ValidateAll() error {
	log.Println("Validating configuration...")

	// 1. 验证源数据库
	if err := v.ValidateSourceDatabase(); err != nil {
		return fmt.Errorf("source database validation failed: %w", err)
	}

	// 2. 验证目标数据库
	if err := v.ValidateTargetDatabase(); err != nil {
		return fmt.Errorf("target database validation failed: %w", err)
	}

	// 3. 验证Redis（如果配置了）
	if v.config.Redis.Host != "" {
		if err := v.ValidateRedis(); err != nil {
			log.Printf("Warning: Redis validation failed: %v. Incremental sync checkpoints may not be persisted to Redis.", err)
		}
	}

	// 4. 验证HTTP配置
	if err := v.ValidateHTTP(); err != nil {
		return fmt.Errorf("http validation failed: %w", err)
	}

	log.Println("Configuration validation passed ✓")
	return nil
}

// ValidateSourceDatabase 验证源数据库
func (v *Validator) ValidateSourceDatabase() error {
	log.Printf("Validating source database: %s:%d/%s",
		v.config.Datasource.Host,
		v.config.Datasource.Port,
		v.config.Datasource.Database)

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&timeout=5s",
		v.config.Datasource.Username,
		v.config.Datasource.Password,
		v.config.Datasource.Host,
		v.config.Datasource.Port,
		v.config.Datasource.Database,
	)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// 设置连接超时
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 测试连接
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	// 检查Binlog配置
	if err := v.checkBinlogConfig(db); err != nil {
		return err
	}

	// 检查用户权限
	if err := v.checkUserPrivileges(db); err != nil {
		return err
	}

	log.Println("  Source database connected successfully ✓")
	return nil
}

// ValidateTargetDatabase 验证目标数据库
func (v *Validator) ValidateTargetDatabase() error {
	if v.config.Target.Host == "" {
		log.Println("  Target database not configured, using source as target")
		return nil
	}

	log.Printf("Validating target database: %s:%d/%s",
		v.config.Target.Host,
		v.config.Target.Port,
		v.config.Target.Database)

	// 连接不指定数据库，以便创建数据库
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&parseTime=True&loc=Local&timeout=5s",
		v.config.Target.Username,
		v.config.Target.Password,
		v.config.Target.Host,
		v.config.Target.Port,
	)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 测试连接
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	// 尝试创建数据库（如果不存在）
	_, err = db.ExecContext(ctx,
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
			v.config.Target.Database))
	if err != nil {
		log.Printf("  Warning: failed to create database: %v", err)
	} else {
		log.Printf("  Database '%s' created or already exists ✓", v.config.Target.Database)
	}

	log.Println("  Target database connected successfully ✓")
	return nil
}

// ValidateRedis 验证Redis连接
func (v *Validator) ValidateRedis() error {
	log.Printf("Validating Redis: %s:%d", v.config.Redis.Host, v.config.Redis.Port)

	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", v.config.Redis.Host, v.config.Redis.Port),
		Password: v.config.Redis.Password,
		DB:       v.config.Redis.DB,
	})
	defer rdb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 测试连接
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to ping redis: %w", err)
	}

	// 测试写入权限
	testKey := "dts:validation:test"
	if err := rdb.Set(ctx, testKey, "test", 10*time.Second).Err(); err != nil {
		return fmt.Errorf("failed to write to redis: %w", err)
	}

	log.Println("  Redis connected successfully ✓")
	return nil
}

// ValidateHTTP 验证HTTP配置
func (v *Validator) ValidateHTTP() error {
	if v.config.Http.Port < 1 || v.config.Http.Port > 65535 {
		return fmt.Errorf("invalid http port: %d", v.config.Http.Port)
	}

	log.Printf("HTTP server will listen on %s:%d ✓", v.config.Http.Host, v.config.Http.Port)
	return nil
}

// checkBinlogConfig 检查Binlog配置
func (v *Validator) checkBinlogConfig(db *sql.DB) error {
	// 检查log_bin
	var logBin string
	err := db.QueryRow("SHOW VARIABLES LIKE 'log_bin'").Scan(&logBin, &logBin)
	if err != nil {
		return fmt.Errorf("failed to check log_bin: %w", err)
	}
	if logBin != "ON" && logBin != "1" {
		return fmt.Errorf("binlog is not enabled (log_bin=%s), incremental sync will not work", logBin)
	}

	// 检查binlog_format
	var binlogFormat string
	err = db.QueryRow("SHOW VARIABLES LIKE 'binlog_format'").Scan(&binlogFormat, &binlogFormat)
	if err != nil {
		return fmt.Errorf("failed to check binlog_format: %w", err)
	}
	if binlogFormat != "ROW" {
		return fmt.Errorf("binlog_format is %s, must be ROW for incremental sync", binlogFormat)
	}

	// 检查binlog_row_image
	var binlogRowImage string
	err = db.QueryRow("SHOW VARIABLES LIKE 'binlog_row_image'").Scan(&binlogRowImage, &binlogRowImage)
	if err != nil {
		// 可能是旧版本MySQL，忽略此检查
		log.Println("  Warning: binlog_row_image variable not found (might be older MySQL version)")
	} else if binlogRowImage != "FULL" {
		log.Printf("  Warning: binlog_row_image is %s, recommended to be FULL for no-PK tables", binlogRowImage)
	}

	log.Println("  Binlog configuration validated ✓")
	return nil
}

// checkUserPrivileges 检查用户权限
func (v *Validator) checkUserPrivileges(db *sql.DB) error {
	// 检查REPLICATION权限
	var hasReplicationSlave, hasReplicationClient, hasSelect string

	rows, err := db.Query("SHOW GRANTS")
	if err != nil {
		return fmt.Errorf("failed to check user privileges: %w", err)
	}
	defer rows.Close()

	grants := ""
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			continue
		}
		grants += grant + "; "

		// 检查是否包含必要权限
		if contains(grant, "REPLICATION SLAVE") || contains(grant, "ALL PRIVILEGES") {
			hasReplicationSlave = "YES"
		}
		if contains(grant, "REPLICATION CLIENT") || contains(grant, "ALL PRIVILEGES") {
			hasReplicationClient = "YES"
		}
		if contains(grant, "SELECT") || contains(grant, "ALL PRIVILEGES") {
			hasSelect = "YES"
		}
	}

	if hasReplicationSlave == "" {
		log.Println("  Warning: REPLICATION SLAVE privilege not found, incremental sync may not work")
	}
	if hasReplicationClient == "" {
		log.Println("  Warning: REPLICATION CLIENT privilege not found, incremental sync may not work")
	}
	if hasSelect == "" {
		log.Println("  Warning: SELECT privilege not found")
	}

	log.Printf("  User privileges validated ✓")
	return nil
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
