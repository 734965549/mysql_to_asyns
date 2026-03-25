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
}

// TargetConfig 目标数据库配置
type TargetConfig struct {
	Host     string `toml:"host"     json:"host"`
	Port     int    `toml:"port"     json:"port"`
	Database string `toml:"database" json:"database"`
	Username string `toml:"username" json:"username"`
	Password string `toml:"password" json:"password"`
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
	return c.Username + ":" + c.Password + "@tcp(" + c.Host + ":" + strconv.Itoa(c.Port) + ")/" + c.Database + "?charset=utf8mb4&parseTime=True&loc=Local"
}

// GetDSN 获取目标数据库DSN
func (c *TargetConfig) GetDSN() string {
	return c.Username + ":" + c.Password + "@tcp(" + c.Host + ":" + strconv.Itoa(c.Port) + ")/" + c.Database + "?charset=utf8mb4&parseTime=True&loc=Local"
}

// GetDSN 获取存储数据库DSN
func (c *StorageConfig) GetDSN() string {
	return c.Username + ":" + c.Password + "@tcp(" + c.Host + ":" + strconv.Itoa(c.Port) + ")/" + c.Database + "?charset=utf8mb4&parseTime=True&loc=Local"
}
