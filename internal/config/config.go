package config // 声明当前文件属于config包，用于配置管理

import ( // 导入外部包
	"os" // 导入os包，用于文件操作
	"strconv" // 导入strconv包，用于字符串转换

	"github.com/BurntSushi/toml" // 导入toml包，用于TOML配置文件解析
)

// Config 全局配置结构体
type Config struct { // 定义全局配置结构体
	Http       HttpConfig       `toml:"http"        json:"http"` // HTTP服务配置
	Datasource DatasourceConfig `toml:"datasource"  json:"datasource"` // 数据源配置
	Log        LogConfig        `toml:"log"         json:"log"` // 日志配置
	Redis      RedisConfig      `toml:"redis"       json:"redis"` // Redis配置
	Target     TargetConfig     `toml:"target"      json:"target"` // 目标数据库配置
	Storage    StorageConfig    `toml:"storage"     json:"storage"` // 持久化配置
	Sync       SyncTuneConfig   `toml:"sync"        json:"sync"` // 同步调优配置
}

// SyncTuneConfig 同步调优配置结构体（全量同步并发与连接池，按实例 max_connections、CPU、磁盘调整；不设则使用内置默认池大小）
type SyncTuneConfig struct { // 定义同步调优配置结构体
	SourceMaxOpenConns int `toml:"source_max_open_conns" json:"source_max_open_conns"` // 源数据库最大连接数
	TargetMaxOpenConns int `toml:"target_max_open_conns" json:"target_max_open_conns"` // 目标数据库最大连接数
	SourceMaxIdleConns int `toml:"source_max_idle_conns" json:"source_max_idle_conns"` // 源数据库最大空闲连接数
	TargetMaxIdleConns int `toml:"target_max_idle_conns" json:"target_max_idle_conns"` // 目标数据库最大空闲连接数
	// ConnMaxLifetimeSeconds 连接最大存活时间（秒）；<=0 时默认 180，应小于 MySQL wait_timeout
	ConnMaxLifetimeSeconds int `toml:"conn_max_lifetime_seconds" json:"conn_max_lifetime_seconds"` // 连接最大存活时间（秒）
	// IntraTableLegacyCap 任务 intra_table_worker_count=0 时单表内并行封顶，默认行为见 entity.EffectiveIntraTableWorkers；<=0 用内置 16
	IntraTableLegacyCap int `toml:"intra_table_legacy_cap" json:"intra_table_legacy_cap"` // 单表内并行封顶
	// IntraTableHardMax 显式 intra 时的绝对上限；<=0 用内置 64
	IntraTableHardMax int `toml:"intra_table_hard_max" json:"intra_table_hard_max"` // 单表内并行绝对上限
	// APIDefaultWorkerCount 创建任务未传 worker_count 时的默认表并发；<=0 则用 4
	APIDefaultWorkerCount int `toml:"api_default_worker_count" json:"api_default_worker_count"` // API默认工作线程数
	// APIDefaultBatchSize 创建任务未传 batch_size 时的默认批量；<=0 则用 1000
	APIDefaultBatchSize int `toml:"api_default_batch_size" json:"api_default_batch_size"` // API默认批处理大小
}

// StorageConfig 持久化配置结构体
type StorageConfig struct { // 定义持久化配置结构体
	Mode     string `toml:"mode"     json:"mode"` // 持久化模式（file或mysql）
	DataDir  string `toml:"data_dir" json:"data_dir"` // 数据目录（file模式使用）
	Host     string `toml:"host"     json:"host"` // 数据库主机（mysql模式使用）
	Port     int    `toml:"port"     json:"port"` // 数据库端口（mysql模式使用）
	Database string `toml:"database" json:"database"` // 数据库名（mysql模式使用）
	Username string `toml:"username" json:"username"` // 数据库用户名（mysql模式使用）
	Password string `toml:"password" json:"password"` // 数据库密码（mysql模式使用）
}

// HttpConfig HTTP服务配置结构体
type HttpConfig struct { // 定义HTTP服务配置结构体
	Host string `toml:"host" json:"host"` // HTTP服务监听地址
	Port int    `toml:"port" json:"port"` // HTTP服务监听端口
}

// DatasourceConfig 源数据库配置结构体
type DatasourceConfig struct { // 定义源数据库配置结构体
	Provider string `toml:"provider" json:"provider"` // 数据库提供者
	Host     string `toml:"host"     json:"host"` // 数据库主机地址
	Port     int    `toml:"port"     json:"port"` // 数据库端口
	Database string `toml:"database" json:"database"` // 数据库名称
	Username string `toml:"username" json:"username"` // 数据库用户名
	Password string `toml:"password" json:"password"` // 数据库密码
	Debug    bool   `toml:"debug"    json:"debug"` // 调试模式开关
	// Compress 启用 MySQL 客户端协议压缩，跨机房或大 JSON/文本字段时通常能明显降低传输耗时（略增 CPU）
	Compress bool   `toml:"compress" json:"compress"` // 启用MySQL协议压缩
}

// TargetConfig 目标数据库配置结构体
type TargetConfig struct { // 定义目标数据库配置结构体
	Host     string `toml:"host"     json:"host"` // 数据库主机地址
	Port     int    `toml:"port"     json:"port"` // 数据库端口
	Database string `toml:"database" json:"database"` // 数据库名称
	Username string `toml:"username" json:"username"` // 数据库用户名
	Password string `toml:"password" json:"password"` // 数据库密码
	Compress bool   `toml:"compress" json:"compress"` // 启用MySQL协议压缩
}

// LogConfig 日志配置结构体
type LogConfig struct { // 定义日志配置结构体
	Level   string        `toml:"level"   json:"level"` // 日志级别
	Console ConsoleConfig `toml:"console" json:"console"` // 控制台日志配置
	File    FileConfig    `toml:"file"    json:"file"` // 文件日志配置
}

// ConsoleConfig 控制台日志配置结构体
type ConsoleConfig struct { // 定义控制台日志配置结构体
	Enable  bool `toml:"enable"   json:"enable"` // 启用控制台日志
	NoColor bool `toml:"no_color" json:"no_color"` // 禁用彩色输出
}

// FileConfig 文件日志配置结构体
type FileConfig struct { // 定义文件日志配置结构体
	Enable bool `toml:"enable" json:"enable"` // 启用文件日志
}

// RedisConfig Redis配置结构体
type RedisConfig struct { // 定义Redis配置结构体
	Host     string `toml:"host"     json:"host"` // Redis主机地址
	Port     int    `toml:"port"     json:"port"` // Redis端口
	Password string `toml:"password" json:"password"` // Redis密码
	DB       int    `toml:"db"       json:"db"` // Redis数据库编号
}

// GlobalConfig 全局配置实例变量
var GlobalConfig *Config // 定义全局配置变量

// LoadConfig 加载配置文件函数
func LoadConfig(path string) (*Config, error) { // 从指定路径加载配置文件
	var config Config // 定义配置变量
	_, err := toml.DecodeFile(path, &config) // 解析TOML配置文件
	if err != nil { // 如果解析失败
		return nil, err // 返回错误
	}
	GlobalConfig = &config // 设置全局配置
	return &config, nil // 返回配置和nil
}

// SaveConfig 保存配置到文件函数
func SaveConfig(path string, cfg *Config) error { // 保存配置到指定路径
	f, err := os.Create(path) // 创建文件
	if err != nil { // 如果创建失败
		return err // 返回错误
	}
	defer f.Close() // 延迟关闭文件

	encoder := toml.NewEncoder(f) // 创建TOML编码器
	return encoder.Encode(cfg) // 编码并保存配置
}

// GetDSN 获取源数据库DSN方法
func (c *DatasourceConfig) GetDSN() string { // 获取数据源名称
	return c.Username + ":" + c.Password + "@tcp(" + c.Host + ":" + strconv.Itoa(c.Port) + ")/" + c.Database + "?" + MySQLTCPParams(c.Compress) // 构建DSN字符串
}

// GetDSN 获取目标数据库DSN方法
func (c *TargetConfig) GetDSN() string { // 获取数据源名称
	return c.Username + ":" + c.Password + "@tcp(" + c.Host + ":" + strconv.Itoa(c.Port) + ")/" + c.Database + "?" + MySQLTCPParams(c.Compress) // 构建DSN字符串
}

// MySQLTCPParams go-sql-driver 连接查询串（不含前导 ?）；compress=true 时启用协议压缩，利于大字段跨网络传输
func MySQLTCPParams(compress bool) string { // 生成MySQL连接参数字符串
	s := "charset=utf8mb4&parseTime=True&loc=Local" // 基础连接参数
	if compress { // 如果启用压缩
		s += "&compress=true" // 添加压缩参数
	}
	return s // 返回连接参数
}

// GetDSN 获取存储数据库DSN方法
func (c *StorageConfig) GetDSN() string { // 获取存储数据库的数据源名称
	return c.Username + ":" + c.Password + "@tcp(" + c.Host + ":" + strconv.Itoa(c.Port) + ")/" + c.Database + "?charset=utf8mb4&parseTime=True&loc=Local" // 构建DSN字符串
}
