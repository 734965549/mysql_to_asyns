package config // 声明当前文件属于config包，用于配置管理

import ( // 导入外部包
	"context" // 导入context包，用于处理请求超时和取消
	"database/sql" // 导入database/sql包，用于数据库操作
	"fmt" // 导入fmt包，用于格式化输入输出
	"log" // 导入log包，用于日志输出
	"time" // 导入time包，用于时间处理

	_ "github.com/go-sql-driver/mysql" // 导入MySQL驱动，下划线表示仅导入init函数
	"github.com/redis/go-redis/v9" // 导入Redis客户端包
)

// Validator 配置验证器结构体
type Validator struct { // 定义配置验证器结构体
	config *Config // 配置实例
}

// NewValidator 创建配置验证器函数
func NewValidator(cfg *Config) *Validator { // 创建配置验证器实例
	return &Validator{config: cfg} // 返回验证器实例
}

// ValidateConfig 验证配置格式（不验证数据库连接）方法
func (v *Validator) ValidateConfig() error { // 验证配置格式
	log.Println("Validating configuration format...") // 输出验证开始日志

	// 验证HTTP配置
	if err := v.ValidateHTTP(); err != nil { // 验证HTTP配置
		return fmt.Errorf("http validation failed: %w", err) // 返回错误
	}

	log.Println("Configuration format validation passed ✓") // 输出验证通过日志
	return nil // 返回nil表示成功
}

// ValidateAll 验证所有配置方法
func (v *Validator) ValidateAll() error { // 验证所有配置
	log.Println("Validating configuration...") // 输出验证开始日志

	// 1. 验证源数据库
	if err := v.ValidateSourceDatabase(); err != nil { // 验证源数据库配置
		return fmt.Errorf("source database validation failed: %w", err) // 返回错误
	}

	// 2. 验证目标数据库
	if err := v.ValidateTargetDatabase(); err != nil { // 验证目标数据库配置
		return fmt.Errorf("target database validation failed: %w", err) // 返回错误
	}

	// 3. 验证Redis（如果配置了）
	if v.config.Redis.Host != "" { // 如果配置了Redis主机
		if err := v.ValidateRedis(); err != nil { // 验证Redis配置
			log.Printf("Warning: Redis validation failed: %v. Incremental sync checkpoints may not be persisted to Redis.", err) // 输出警告日志
		}
	}

	// 4. 验证HTTP配置
	if err := v.ValidateHTTP(); err != nil { // 验证HTTP配置
		return fmt.Errorf("http validation failed: %w", err) // 返回错误
	}

	log.Println("Configuration validation passed ✓") // 输出验证通过日志
	return nil // 返回nil表示成功
}

// ValidateSourceDatabase 验证源数据库方法
func (v *Validator) ValidateSourceDatabase() error { // 验证源数据库连接
	log.Printf("Validating source database: %s:%d/%s", // 输出验证开始日志
		v.config.Datasource.Host, // 主机
		v.config.Datasource.Port, // 端口
		v.config.Datasource.Database) // 数据库名

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&timeout=5s", // 构建数据源名称
		v.config.Datasource.Username, // 用户名
		v.config.Datasource.Password, // 密码
		v.config.Datasource.Host, // 主机
		v.config.Datasource.Port, // 端口
		v.config.Datasource.Database, // 数据库名
	)

	db, err := sql.Open("mysql", dsn) // 打开数据库连接
	if err != nil { // 如果打开失败
		return fmt.Errorf("failed to open database: %w", err) // 返回错误
	}
	defer db.Close() // 延迟关闭数据库连接

	// 设置连接超时
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // 创建5秒超时的上下文
	defer cancel() // 延迟取消上下文

	// 测试连接
	if err := db.PingContext(ctx); err != nil { // 测试数据库连接
		return fmt.Errorf("failed to ping database: %w", err) // 返回错误
	}

	// 检查Binlog配置
	if err := v.checkBinlogConfig(db); err != nil { // 检查Binlog配置
		return err // 返回错误
	}

	// 检查用户权限
	if err := v.checkUserPrivileges(db); err != nil { // 检查用户权限
		return err // 返回错误
	}

	log.Println("  Source database connected successfully ✓") // 输出成功日志
	return nil // 返回nil表示成功
}

// ValidateTargetDatabase 验证目标数据库方法
func (v *Validator) ValidateTargetDatabase() error { // 验证目标数据库连接
	if v.config.Target.Host == "" { // 如果未配置目标数据库
		log.Println("  Target database not configured, using source as target") // 输出提示信息
		return nil // 返回nil
	}

	log.Printf("Validating target database: %s:%d/%s", // 输出验证开始日志
		v.config.Target.Host, // 主机
		v.config.Target.Port, // 端口
		v.config.Target.Database) // 数据库名

	// 连接不指定数据库，以便创建数据库
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&parseTime=True&loc=Local&timeout=5s", // 构建数据源名称
		v.config.Target.Username, // 用户名
		v.config.Target.Password, // 密码
		v.config.Target.Host, // 主机
		v.config.Target.Port, // 端口
	)

	db, err := sql.Open("mysql", dsn) // 打开数据库连接
	if err != nil { // 如果打开失败
		return fmt.Errorf("failed to open database: %w", err) // 返回错误
	}
	defer db.Close() // 延迟关闭数据库连接

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // 创建5秒超时的上下文
	defer cancel() // 延迟取消上下文

	// 测试连接
	if err := db.PingContext(ctx); err != nil { // 测试数据库连接
		return fmt.Errorf("failed to ping database: %w", err) // 返回错误
	}

	// 尝试创建数据库（如果不存在）
	_, err = db.ExecContext(ctx, // 执行SQL语句
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", // 创建数据库SQL
			v.config.Target.Database)) // 数据库名
	if err != nil { // 如果创建失败
		log.Printf("  Warning: failed to create database: %v", err) // 输出警告日志
	} else { // 如果创建成功
		log.Printf("  Database '%s' created or already exists ✓", v.config.Target.Database) // 输出成功日志
	}

	log.Println("  Target database connected successfully ✓") // 输出成功日志
	return nil // 返回nil表示成功
}

// ValidateRedis 验证Redis连接方法
func (v *Validator) ValidateRedis() error { // 验证Redis连接
	log.Printf("Validating Redis: %s:%d", v.config.Redis.Host, v.config.Redis.Port) // 输出验证开始日志

	rdb := redis.NewClient(&redis.Options{ // 创建Redis客户端
		Addr:     fmt.Sprintf("%s:%d", v.config.Redis.Host, v.config.Redis.Port), // 设置地址
		Password: v.config.Redis.Password, // 设置密码
		DB:       v.config.Redis.DB, // 设置数据库编号
	})
	defer rdb.Close() // 延迟关闭Redis客户端

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // 创建5秒超时的上下文
	defer cancel() // 延迟取消上下文

	// 测试连接
	if err := rdb.Ping(ctx).Err(); err != nil { // 测试Redis连接
		return fmt.Errorf("failed to ping redis: %w", err) // 返回错误
	}

	// 测试写入权限
	testKey := "dts:validation:test" // 定义测试键
	if err := rdb.Set(ctx, testKey, "test", 10*time.Second).Err(); err != nil { // 测试写入
		return fmt.Errorf("failed to write to redis: %w", err) // 返回错误
	}

	log.Println("  Redis connected successfully ✓") // 输出成功日志
	return nil // 返回nil表示成功
}

// ValidateHTTP 验证HTTP配置方法
func (v *Validator) ValidateHTTP() error { // 验证HTTP配置
	if v.config.Http.Port < 1 || v.config.Http.Port > 65535 { // 如果端口号无效
		return fmt.Errorf("invalid http port: %d", v.config.Http.Port) // 返回错误
	}

	log.Printf("HTTP server will listen on %s:%d ✓", v.config.Http.Host, v.config.Http.Port) // 输出成功日志
	return nil // 返回nil表示成功
}

// checkBinlogConfig 检查Binlog配置方法
func (v *Validator) checkBinlogConfig(db *sql.DB) error { // 检查MySQL Binlog配置
	// 检查log_bin
	var logBin string // 定义log_bin变量
	err := db.QueryRow("SHOW VARIABLES LIKE 'log_bin'").Scan(&logBin, &logBin) // 查询log_bin变量
	if err != nil { // 如果查询失败
		return fmt.Errorf("failed to check log_bin: %w", err) // 返回错误
	}
	if logBin != "ON" && logBin != "1" { // 如果binlog未启用
		return fmt.Errorf("binlog is not enabled (log_bin=%s), incremental sync will not work", logBin) // 返回错误
	}

	// 检查binlog_format
	var binlogFormat string // 定义binlog_format变量
	err = db.QueryRow("SHOW VARIABLES LIKE 'binlog_format'").Scan(&binlogFormat, &binlogFormat) // 查询binlog_format变量
	if err != nil { // 如果查询失败
		return fmt.Errorf("failed to check binlog_format: %w", err) // 返回错误
	}
	if binlogFormat != "ROW" { // 如果格式不是ROW
		return fmt.Errorf("binlog_format is %s, must be ROW for incremental sync", binlogFormat) // 返回错误
	}

	// 检查binlog_row_image
	var binlogRowImage string // 定义binlog_row_image变量
	err = db.QueryRow("SHOW VARIABLES LIKE 'binlog_row_image'").Scan(&binlogRowImage, &binlogRowImage) // 查询binlog_row_image变量
	if err != nil { // 如果查询失败
		// 可能是旧版本MySQL，忽略此检查
		log.Println("  Warning: binlog_row_image variable not found (might be older MySQL version)") // 输出警告
	} else if binlogRowImage != "FULL" { // 如果不是FULL模式
		log.Printf("  Warning: binlog_row_image is %s, recommended to be FULL for no-PK tables", binlogRowImage) // 输出警告
	}

	log.Println("  Binlog configuration validated ✓") // 输出成功日志
	return nil // 返回nil表示成功
}

// checkUserPrivileges 检查用户权限方法
func (v *Validator) checkUserPrivileges(db *sql.DB) error { // 检查MySQL用户权限
	// 检查REPLICATION权限
	var hasReplicationSlave, hasReplicationClient, hasSelect string // 定义权限变量

	rows, err := db.Query("SHOW GRANTS") // 查询用户权限
	if err != nil { // 如果查询失败
		return fmt.Errorf("failed to check user privileges: %w", err) // 返回错误
	}
	defer rows.Close() // 延迟关闭结果集

	grants := "" // 定义权限字符串
	for rows.Next() { // 遍历权限结果
		var grant string // 定义单个权限变量
		if err := rows.Scan(&grant); err != nil { // 扫描权限
			continue // 跳过错误
		}
		grants += grant + "; " // 拼接权限字符串

		// 检查是否包含必要权限
		if contains(grant, "REPLICATION SLAVE") || contains(grant, "ALL PRIVILEGES") { // 如果包含复制权限或全部权限
			hasReplicationSlave = "YES" // 设置标志
		}
		if contains(grant, "REPLICATION CLIENT") || contains(grant, "ALL PRIVILEGES") { // 如果包含复制客户端权限或全部权限
			hasReplicationClient = "YES" // 设置标志
		}
		if contains(grant, "SELECT") || contains(grant, "ALL PRIVILEGES") { // 如果包含查询权限或全部权限
			hasSelect = "YES" // 设置标志
		}
	}

	if hasReplicationSlave == "" { // 如果没有复制权限
		log.Println("  Warning: REPLICATION SLAVE privilege not found, incremental sync may not work") // 输出警告
	}
	if hasReplicationClient == "" { // 如果没有复制客户端权限
		log.Println("  Warning: REPLICATION CLIENT privilege not found, incremental sync may not work") // 输出警告
	}
	if hasSelect == "" { // 如果没有查询权限
		log.Println("  Warning: SELECT privilege not found") // 输出警告
	}

	log.Printf("  User privileges validated ✓") // 输出成功日志
	return nil // 返回nil表示成功
}

func contains(s, substr string) bool { // 检查字符串包含函数
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr)) // 返回包含结果
}

func containsHelper(s, substr string) bool { // 字符串包含辅助函数
	for i := 0; i <= len(s)-len(substr); i++ { // 遍历字符串
		if s[i:i+len(substr)] == substr { // 如果找到子串
			return true // 返回true
		}
	}
	return false // 返回false
}
