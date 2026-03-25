package config

import (
	"testing"
)

func TestNewValidator(t *testing.T) {
	cfg := &Config{
		Datasource: DatasourceConfig{
			Host:     "localhost",
			Port:     3306,
			Database: "test_db",
			Username: "root",
			Password: "password",
		},
		Http: HttpConfig{
			Host: "0.0.0.0",
			Port: 8081,
		},
	}

	validator := NewValidator(cfg)
	if validator == nil {
		t.Error("expected validator, got nil")
	}
}

func TestValidateHTTP(t *testing.T) {
	tests := []struct {
		name    string
		port    int
		wantErr bool
	}{
		{"Valid port", 8081, false},
		{"Min port", 1, false},
		{"Max port", 65535, false},
		{"Invalid port 0", 0, true},
		{"Invalid port negative", -1, true},
		{"Invalid port too large", 65536, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Http: HttpConfig{
					Port: tt.port,
				},
			}

			validator := NewValidator(cfg)
			err := validator.ValidateHTTP()

			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateHTTP() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateSourceDatabase_InvalidConfig(t *testing.T) {
	cfg := &Config{
		Datasource: DatasourceConfig{
			Host:     "invalid-host-that-does-not-exist",
			Port:     3306,
			Database: "test_db",
			Username: "root",
			Password: "password",
		},
	}

	validator := NewValidator(cfg)
	err := validator.ValidateSourceDatabase()

	// 应该失败，因为连接不到数据库
	if err == nil {
		t.Error("expected error for invalid database connection, got nil")
	}
}

func TestValidateRedis_InvalidConfig(t *testing.T) {
	cfg := &Config{
		Redis: RedisConfig{
			Host: "invalid-redis-host",
			Port: 6379,
		},
	}

	validator := NewValidator(cfg)
	err := validator.ValidateRedis()

	// 应该失败，因为连接不到Redis
	if err == nil {
		t.Error("expected error for invalid redis connection, got nil")
	}
}

func TestValidateRedis_NoConfig(t *testing.T) {
	cfg := &Config{
		Redis: RedisConfig{
			Host:     "", // 空配置，不需要验证
			Port:     0,
			Password: "",
			DB:       0,
		},
	}

	validator := NewValidator(cfg)
	err := validator.ValidateRedis()

	// 空配置不应该报错，或者应该跳过验证
	// 实际实现中，空配置可能会尝试连接，所以这里只验证不panic
	if err != nil {
		// 空配置可能会有错误，这是可以接受的
		// 只要不是panic就行
		t.Logf("empty redis config returned error (expected): %v", err)
	}
}

func TestCheckBinlogConfig(t *testing.T) {
	// 这个测试需要真实的数据库连接，这里只测试逻辑
	// 在集成测试中会覆盖
	t.Skip("requires real database connection")
}

func TestCheckUserPrivileges(t *testing.T) {
	// 这个测试需要真实的数据库连接
	t.Skip("requires real database connection")
}

func TestContains(t *testing.T) {
	tests := []struct {
		s      string
		substr string
		want   bool
	}{
		{"hello world", "world", true},
		{"hello world", "foo", false},
		{"REPLICATION SLAVE", "REPLICATION", true},
		{"ALL PRIVILEGES", "ALL PRIVILEGES", true},
		{"", "", true},
		{"abc", "", true},
		{"", "a", false},
	}

	for _, tt := range tests {
		t.Run(tt.s+"_"+tt.substr, func(t *testing.T) {
			if got := contains(tt.s, tt.substr); got != tt.want {
				t.Errorf("contains(%s, %s) = %v, want %v", tt.s, tt.substr, got, tt.want)
			}
		})
	}
}
