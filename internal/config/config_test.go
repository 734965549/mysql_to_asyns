package config

import (
	"os"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "config_*.toml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	configContent := `
[http]
host = "127.0.0.1"
port = 8081

[datasource]
host = "127.0.0.1"
port = 3306
username = "root"
password = "password"
database = "source_db"

[target]
host = "127.0.0.1"
port = 3306
username = "root"
password = "password"
database = "target_db"

[log]
level = "info"

[log.console]
enable = true
no_color = false

[log.file]
enable = false

[redis]
host = "127.0.0.1"
port = 6379
password = ""
db = 0
`
	if _, err := tmpFile.WriteString(configContent); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}
	tmpFile.Close()

	cfg, err := LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if cfg.Http.Port != 8081 {
		t.Errorf("Expected HTTP port 8081, got %d", cfg.Http.Port)
	}
	if cfg.Datasource.Host != "127.0.0.1" {
		t.Errorf("Expected datasource host 127.0.0.1, got %s", cfg.Datasource.Host)
	}
	if cfg.Target.Database != "target_db" {
		t.Errorf("Expected target database target_db, got %s", cfg.Target.Database)
	}
	if cfg.Redis.Host != "127.0.0.1" {
		t.Errorf("Expected Redis host 127.0.0.1, got %s", cfg.Redis.Host)
	}
}

func TestApplyEnvOverrides(t *testing.T) {
	cfg := &Config{}

	t.Setenv("MYSQL_TO_ASYNC_HTTP_HOST", "0.0.0.0")
	t.Setenv("MYSQL_TO_ASYNC_HTTP_PORT", "8080")
	t.Setenv("MYSQL_TO_ASYNC_DATASOURCE_HOST", "mysql-source")
	t.Setenv("MYSQL_TO_ASYNC_DATASOURCE_PORT", "3306")
	t.Setenv("MYSQL_TO_ASYNC_DATASOURCE_DATABASE", "source_db")
	t.Setenv("MYSQL_TO_ASYNC_DATASOURCE_USERNAME", "sync_user")
	t.Setenv("MYSQL_TO_ASYNC_DATASOURCE_PASSWORD", "sync_password")
	t.Setenv("MYSQL_TO_ASYNC_DATASOURCE_DEBUG", "false")
	t.Setenv("MYSQL_TO_ASYNC_TARGET_HOST", "mysql-target")
	t.Setenv("MYSQL_TO_ASYNC_TARGET_PORT", "3306")
	t.Setenv("MYSQL_TO_ASYNC_STORAGE_MODE", "file")
	t.Setenv("MYSQL_TO_ASYNC_STORAGE_DATA_DIR", "/app/data")
	t.Setenv("MYSQL_TO_ASYNC_REDIS_HOST", "redis")
	t.Setenv("MYSQL_TO_ASYNC_REDIS_PORT", "6379")
	t.Setenv("MYSQL_TO_ASYNC_REDIS_DB", "1")

	err := ApplyEnvOverrides(cfg)
	if err != nil {
		t.Fatalf("ApplyEnvOverrides failed: %v", err)
	}

	if cfg.Http.Host != "0.0.0.0" || cfg.Http.Port != 8080 {
		t.Fatalf("unexpected http config: %+v", cfg.Http)
	}
	if cfg.Datasource.Host != "mysql-source" || cfg.Datasource.Port != 3306 {
		t.Fatalf("unexpected datasource config: %+v", cfg.Datasource)
	}
	if cfg.Target.Host != "mysql-target" || cfg.Target.Port != 3306 {
		t.Fatalf("unexpected target config: %+v", cfg.Target)
	}
	if cfg.Storage.Mode != "file" || cfg.Storage.DataDir != "/app/data" {
		t.Fatalf("unexpected storage config: %+v", cfg.Storage)
	}
	if cfg.Redis.Host != "redis" || cfg.Redis.Port != 6379 || cfg.Redis.DB != 1 {
		t.Fatalf("unexpected redis config: %+v", cfg.Redis)
	}
}

func TestApplyEnvOverrides_InvalidInt(t *testing.T) {
	cfg := &Config{}
	t.Setenv("MYSQL_TO_ASYNC_HTTP_PORT", "abc")

	err := ApplyEnvOverrides(cfg)
	if err == nil {
		t.Fatal("expected error for invalid env int value")
	}
}

func TestDatasourceConfig_GetDSN(t *testing.T) {
	cfg := DatasourceConfig{
		Host:     "localhost",
		Port:     3306,
		Username: "root",
		Password: "password",
		Database: "testdb",
	}

	dsn := cfg.GetDSN()
	expected := "root:password@tcp(localhost:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local"

	if dsn != expected {
		t.Errorf("Expected DSN %s, got %s", expected, dsn)
	}
}

func TestDatasourceConfig_GetDSN_Compress(t *testing.T) {
	cfg := DatasourceConfig{
		Host:     "localhost",
		Port:     3306,
		Username: "root",
		Password: "password",
		Database: "testdb",
		Compress: true,
	}
	dsn := cfg.GetDSN()
	expected := "root:password@tcp(localhost:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local&compress=true"
	if dsn != expected {
		t.Errorf("Expected DSN %s, got %s", expected, dsn)
	}
}

func TestTargetConfig_GetDSN(t *testing.T) {
	cfg := TargetConfig{
		Host:     "localhost",
		Port:     3306,
		Username: "root",
		Password: "password",
		Database: "testdb",
	}

	dsn := cfg.GetDSN()
	expected := "root:password@tcp(localhost:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local"

	if dsn != expected {
		t.Errorf("Expected DSN %s, got %s", expected, dsn)
	}
}

func TestTargetConfig_GetDSN_Compress(t *testing.T) {
	cfg := TargetConfig{
		Host:     "localhost",
		Port:     3306,
		Username: "root",
		Password: "password",
		Database: "testdb",
		Compress: true,
	}
	dsn := cfg.GetDSN()
	expected := "root:password@tcp(localhost:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local&compress=true"
	if dsn != expected {
		t.Errorf("Expected DSN %s, got %s", expected, dsn)
	}
}

func TestSaveConfig(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "config_save_*.toml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	cfg := &Config{
		Http: HttpConfig{
			Host: "127.0.0.1",
			Port: 8081,
		},
		Datasource: DatasourceConfig{
			Host:     "localhost",
			Port:     3306,
			Username: "root",
			Password: "password",
			Database: "testdb",
		},
		Target: TargetConfig{
			Host:     "localhost",
			Port:     3306,
			Username: "root",
			Password: "password",
			Database: "targetdb",
		},
		Log: LogConfig{
			Level: "debug",
			Console: ConsoleConfig{
				Enable:  true,
				NoColor: false,
			},
			File: FileConfig{
				Enable: true,
			},
		},
		Redis: RedisConfig{
			Host:     "127.0.0.1",
			Port:     6379,
			Password: "",
			DB:       0,
		},
	}

	err = SaveConfig(tmpFile.Name(), cfg)
	if err != nil {
		t.Fatalf("Failed to save config: %v", err)
	}

	// 验证保存的配置可以重新加载
	loaded, err := LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to load saved config: %v", err)
	}

	if loaded.Http.Port != 8081 {
		t.Errorf("Expected HTTP port 8081, got %d", loaded.Http.Port)
	}
	if loaded.Datasource.Database != "testdb" {
		t.Errorf("Expected datasource database testdb, got %s", loaded.Datasource.Database)
	}
}

func TestLoadConfig_InvalidFile(t *testing.T) {
	_, err := LoadConfig("non_existent_file.toml")
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
}

func TestLoadConfig_InvalidContent(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "config_invalid_*.toml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// 写入无效的TOML内容
	if _, err := tmpFile.WriteString("invalid toml content {{{"); err != nil {
		t.Fatalf("Failed to write invalid content: %v", err)
	}
	tmpFile.Close()

	_, err = LoadConfig(tmpFile.Name())
	if err == nil {
		t.Error("Expected error for invalid TOML content")
	}
}
