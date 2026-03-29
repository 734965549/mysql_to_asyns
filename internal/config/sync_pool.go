package config

import (
	"database/sql"
	"log"
	"time"
)

// ApplySyncMySQLPool 为同步/元数据使用的 *sql.DB 设置连接池。max_open 未配置时默认 256，避免驱动默认 max_idle 过小导致高并发时频繁建连。
func ApplySyncMySQLPool(db *sql.DB, tune *SyncTuneConfig, source bool, logLabel string) {
	if db == nil {
		return
	}
	maxOpen, maxIdle, lifeSec := 0, 0, 0
	if tune != nil {
		if source {
			maxOpen = tune.SourceMaxOpenConns
			maxIdle = tune.SourceMaxIdleConns
		} else {
			maxOpen = tune.TargetMaxOpenConns
			maxIdle = tune.TargetMaxIdleConns
		}
		lifeSec = tune.ConnMaxLifetimeSeconds
	}
	if maxOpen <= 0 {
		maxOpen = 256
	}
	db.SetMaxOpenConns(maxOpen)
	if maxIdle <= 0 {
		maxIdle = maxOpen / 2
		if maxIdle < 16 {
			maxIdle = 16
		}
		if maxIdle > maxOpen {
			maxIdle = maxOpen
		}
	}
	db.SetMaxIdleConns(maxIdle)
	// 未配置时默认 180s：须小于 MySQL wait_timeout，否则池中空闲连接易被服务端掐断，复用时出现 invalid connection
	if lifeSec <= 0 {
		lifeSec = 180
	}
	db.SetConnMaxLifetime(time.Duration(lifeSec) * time.Second)
	if logLabel != "" {
		log.Printf("[%s] MySQL pool: max_open=%d max_idle=%d conn_max_lifetime=%ds", logLabel, maxOpen, maxIdle, lifeSec)
	}
}
