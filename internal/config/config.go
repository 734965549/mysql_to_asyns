package config

import (
	"os"
	"strconv"

	"github.com/BurntSushi/toml"
)

// Config 全局配置结构
type Config struct {
	Http       HttpConfig       `toml:"http"        json:"http"`
	Datasource DatasourceConfig `toml:"datasource"  json:"datasource"`
	Log        LogConfig        `toml:"log"         json:"log"`
	Redis      RedisConfig      `toml:"redis"       json:"redis"`
	Target     TargetConfig     `toml:"target"      json:"target"`
	Storage    StorageConfig    `toml:"storage"     json:"storage"`
	Sync       SyncTuneConfig   `toml:"sync"        json:"sync"`
}

// SyncTuneConfig 全量同步并发与连接池（按实例 max_connections、CPU、磁盘调整；不设则使用内置默认池大小）
type SyncTuneConfig struct {
	SourceMaxOpenConns int `toml:"source_max_open_conns" json:"source_max_open_conns"`
	TargetMaxOpenConns int `toml:"target_max_open_conns" json:"target_max_open_conns"`
	SourceMaxIdleConns int `toml:"source_max_idle_conns" json:"source_max_idle_conns"`
	TargetMaxIdleConns int `toml:"target_max_idle_conns" json:"target_max_idle_conns"`
	// ConnMaxLifetimeSeconds 连接最大存活时间（秒）；<=0 时默认 180，应小于 MySQL wait_timeout
	ConnMaxLifetimeSeconds int `toml:"conn_max_lifetime_seconds" json:"conn_max_lifetime_seconds"`
	// IntraTableLegacyCap 任务 intra_table_worker_count=0 时单表内并行封顶，默认行为见 entity.EffectiveIntraTableWorkers；<=0 用内置 16
	IntraTableLegacyCap int `toml:"intra_table_legacy_cap" json:"intra_table_legacy_cap"`
	// IntraTableHardMax 显式 intra 时的绝对上限；<=0 用内置 64
	IntraTableHardMax int `toml:"intra_table_hard_max" json:"intra_table_hard_max"`
	// APIDefaultWorkerCount 创建任务未传 worker_count 时的默认表并发；<=0 则用 4
	APIDefaultWorkerCount int `toml:"api_default_worker_count" json:"api_default_worker_count"`
	// APIDefaultBatchSize 创建任务未传 batch_size 时的默认批量；<=0 则用 1000
	APIDefaultBatchSize int `toml:"api_default_batch_size" json:"api_default_batch_size"`
}

// StorageConfig 持久化配置
type StorageConfig struct {
	Mode     string `toml:"mode"     json:"mode"`
	DataDir  string `toml:"data_dir" json:"data_dir"`
	Host     string `toml:"host"     json:"host"`
	Port     int    `toml:"port"     json:"port"`
	Database string `toml:"database" json:"database"`
	Username string `toml:"username" json:"username"`
	Password string `toml:"password" json:"password"`
}

// HttpConfig HTTP服务配置
type HttpConfig struct {
	Host string `toml:"host" json:"host"`
	Port int    `toml:"port" json:"port"`
}

// DatasourceConfig 源数据库配置
type DatasourceConfig struct {
	Provider string `toml:"provider" json:"provider"`
	Host     string `toml:"host"     json:"host"`
	Port     int    `toml:"port"     json:"port"`
	Database string `toml:"database" json:"database"`
	Username string `toml:"username" json:"username"`
	Password string `toml:"password" json:"password"`
	Debug    bool   `toml:"debug"    json:"debug"`
	// Compress 启用 MySQL 客户端协议压缩，跨机房或大 JSON/文本字段时通常能明显降低传输耗时（略增 CPU）
	Compress bool `toml:"compress" json:"compress"`
}

// TargetConfig 目标数据库配置
type TargetConfig struct {
	Host     string `toml:"host"     json:"host"`
	Port     int    `toml:"port"     json:"port"`
	Database string `toml:"database" json:"database"`
	Username string `toml:"username" json:"username"`
	Password string `toml:"password" json:"password"`
	Compress bool   `toml:"compress" json:"compress"`
}

// LogConfig 日志配置
type LogConfig struct {
	Level   string        `toml:"level"   json:"level"`
	Console ConsoleConfig `toml:"console" json:"console"`
	File    FileConfig    `toml:"file"    json:"file"`
}

// ConsoleConfig 控制台日志配置
type ConsoleConfig struct {
	Enable  bool `toml:"enable"   json:"enable"`
	NoColor bool `toml:"no_color" json:"no_color"`
}

// FileConfig 文件日志配置
type FileConfig struct {
	Enable bool `toml:"enable" json:"enable"`
}

// RedisConfig Redis配置
type RedisConfig struct {
	Host     string `toml:"host"     json:"host"`
	Port     int    `toml:"port"     json:"port"`
	Password string `toml:"password" json:"password"`
	DB       int    `toml:"db"       json:"db"`
}

// GlobalConfig 全局配置实例
var GlobalConfig *Config

// LoadConfig 加载配置文件
func LoadConfig(path string) (*Config, error) {
	var config Config
	_, err := toml.DecodeFile(path, &config)
	if err != nil {
		return nil, err
	}
	GlobalConfig = &config
	return &config, nil
}

// SaveConfig 保存配置到文件
func SaveConfig(path string, cfg *Config) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	encoder := toml.NewEncoder(f)
	return encoder.Encode(cfg)
}

// GetDSN 获取源数据库DSN
func (c *DatasourceConfig) GetDSN() string {
	return c.Username + ":" + c.Password + "@tcp(" + c.Host + ":" + strconv.Itoa(c.Port) + ")/" + c.Database + "?" + MySQLTCPParams(c.Compress)
}

// GetDSN 获取目标数据库DSN
func (c *TargetConfig) GetDSN() string {
	return c.Username + ":" + c.Password + "@tcp(" + c.Host + ":" + strconv.Itoa(c.Port) + ")/" + c.Database + "?" + MySQLTCPParams(c.Compress)
}

// MySQLTCPParams go-sql-driver 连接查询串（不含前导 ?）；compress=true 时启用协议压缩，利于大字段跨网络传输
func MySQLTCPParams(compress bool) string {
	s := "charset=utf8mb4&parseTime=True&loc=Local"
	if compress {
		s += "&compress=true"
	}
	return s
}

// GetDSN 获取存储数据库DSN
func (c *StorageConfig) GetDSN() string {
	return c.Username + ":" + c.Password + "@tcp(" + c.Host + ":" + strconv.Itoa(c.Port) + ")/" + c.Database + "?charset=utf8mb4&parseTime=True&loc=Local"
}
