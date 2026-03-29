package config // 声明当前文件属于config包，用于配置管理

import ( // 导入外部包
	"database/sql" // 导入database/sql包，用于数据库操作
	"log" // 导入log包，用于日志输出
	"time" // 导入time包，用于时间处理
)

// ApplySyncMySQLPool 为同步/元数据使用的 *sql.DB 设置连接池函数。max_open 未配置时默认 256，避免驱动默认 max_idle 过小导致高并发时频繁建连。
func ApplySyncMySQLPool(db *sql.DB, tune *SyncTuneConfig, source bool, logLabel string) { // 为数据库连接设置连接池参数
	if db == nil { // 如果数据库连接为空
		return // 直接返回
	}
	maxOpen, maxIdle, lifeSec := 0, 0, 0 // 定义连接池参数变量
	if tune != nil { // 如果调优配置不为空
		if source { // 如果是源数据库
			maxOpen = tune.SourceMaxOpenConns // 设置源数据库最大连接数
			maxIdle = tune.SourceMaxIdleConns // 设置源数据库最大空闲连接数
		} else { // 否则是目标数据库
			maxOpen = tune.TargetMaxOpenConns // 设置目标数据库最大连接数
			maxIdle = tune.TargetMaxIdleConns // 设置目标数据库最大空闲连接数
		}
		lifeSec = tune.ConnMaxLifetimeSeconds // 设置连接最大存活时间
	}
	if maxOpen <= 0 { // 如果最大连接数未设置
		maxOpen = 256 // 使用默认值256
	}
	db.SetMaxOpenConns(maxOpen) // 设置最大打开连接数
	if maxIdle <= 0 { // 如果最大空闲连接数未设置
		maxIdle = maxOpen / 2 // 设置为最大连接数的一半
		if maxIdle < 16 { // 如果计算值小于16
			maxIdle = 16 // 设置为16
		}
		if maxIdle > maxOpen { // 如果计算值大于最大连接数
			maxIdle = maxOpen // 设置为最大连接数
		}
	}
	db.SetMaxIdleConns(maxIdle) // 设置最大空闲连接数
	// 未配置时默认 180s：须小于 MySQL wait_timeout，否则池中空闲连接易被服务端掐断，复用时出现 invalid connection
	if lifeSec <= 0 { // 如果连接最大存活时间未设置
		lifeSec = 180 // 使用默认值180秒
	}
	db.SetConnMaxLifetime(time.Duration(lifeSec) * time.Second) // 设置连接最大存活时间
	if logLabel != "" { // 如果有日志标签
		log.Printf("[%s] MySQL pool: max_open=%d max_idle=%d conn_max_lifetime=%ds", logLabel, maxOpen, maxIdle, lifeSec) // 输出连接池配置日志
	}
}
