package sink

import (
	"context"
	"database/sql"
	"fmt"

	"mysql-to-async/internal/metadata/domain/service"
	"mysql-to-async/internal/sync/domain/sink"
	"mysql-to-async/internal/sync/infrastructure/sink/kafka"
	mysqlsink "mysql-to-async/internal/sync/infrastructure/sink/mysql"
	"mysql-to-async/internal/sync/infrastructure/sink/webhook"
)

// SinkType 目标类型常量
type SinkType string

const (
	SinkTypeMySQL       SinkType = "MYSQL"
	SinkTypeKafka       SinkType = "KAFKA"
	SinkTypeHTTPWebhook SinkType = "HTTP_WEBHOOK"
)

// SinkConfig 目标端配置（与任务关联）
type SinkConfig struct {
	Type    SinkType               `json:"type"`
	Options map[string]interface{} `json:"options"`
}

// SinkFactory 根据任务目标配置创建具体 Sink
type SinkFactory struct{}

// NewSinkFactory 创建 SinkFactory
func NewSinkFactory() *SinkFactory {
	return &SinkFactory{}
}

// CreateSink 根据配置创建具体 Sink 实例
func (f *SinkFactory) CreateSink(ctx context.Context, cfg *SinkConfig, deps *SinkDeps) (sink.Sink, error) {
	if cfg == nil {
		return nil, fmt.Errorf("sink config is nil")
	}

	switch cfg.Type {
	case SinkTypeMySQL:
		return f.createMySQLSink(cfg, deps)
	case SinkTypeKafka:
		return f.createKafkaSink(cfg)
	case SinkTypeHTTPWebhook:
		return f.createWebhookSink(cfg)
	default:
		return nil, fmt.Errorf("unsupported sink type: %s", cfg.Type)
	}
}

// SinkDeps MySQL Sink 所需的外部依赖
type SinkDeps struct {
	TargetDB *sql.DB
	Analyzer service.IdentityAnalyzer
}

func (f *SinkFactory) createMySQLSink(cfg *SinkConfig, deps *SinkDeps) (sink.Sink, error) {
	if deps == nil || deps.TargetDB == nil || deps.Analyzer == nil {
		return nil, fmt.Errorf("mysql sink requires target db and analyzer")
	}

	targetSchema := getStringOption(cfg.Options, "target_schema", "")
	targetDatabases := getStringSliceOption(cfg.Options, "target_databases")
	batchSize := getIntOption(cfg.Options, "batch_size", 1000)

	return mysqlsink.NewMySQLSink(mysqlsink.Config{
		TargetDB:        deps.TargetDB,
		Analyzer:        deps.Analyzer,
		TargetSchema:    targetSchema,
		TargetDatabases: targetDatabases,
		BatchSize:       batchSize,
	}), nil
}

func (f *SinkFactory) createKafkaSink(cfg *SinkConfig) (sink.Sink, error) {
	brokers := getStringSliceOption(cfg.Options, "brokers")
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka sink requires at least one broker")
	}

	topic := getStringOption(cfg.Options, "topic", "mysql_cdc")
	keyMode := getStringOption(cfg.Options, "key_mode", "pk")
	batchSize := getIntOption(cfg.Options, "batch_size", 100)
	batchTimeoutMs := getIntOption(cfg.Options, "batch_timeout_ms", 500)
	requiredAcks := getIntOption(cfg.Options, "required_acks", 1)

	return kafka.NewKafkaSink(kafka.Config{
		Brokers:        brokers,
		Topic:          topic,
		KeyMode:        keyMode,
		BatchSize:      batchSize,
		BatchTimeoutMs: batchTimeoutMs,
		RequiredAcks:   requiredAcks,
	}), nil
}

func (f *SinkFactory) createWebhookSink(cfg *SinkConfig) (sink.Sink, error) {
	url := getStringOption(cfg.Options, "url", "")
	if url == "" {
		return nil, fmt.Errorf("webhook sink requires url")
	}

	method := getStringOption(cfg.Options, "method", "POST")
	timeoutMs := getIntOption(cfg.Options, "timeout_ms", 3000)
	retryTimes := getIntOption(cfg.Options, "retry_times", 3)
	headers := getStringMapOption(cfg.Options, "headers")

	return webhook.NewWebhookSink(webhook.Config{
		URL:        url,
		Method:     method,
		TimeoutMs:  timeoutMs,
		RetryTimes: retryTimes,
		Headers:    headers,
	}), nil
}

// --- option helpers ---

func getStringOption(opts map[string]interface{}, key, defaultVal string) string {
	if v, ok := opts[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return defaultVal
}

func getIntOption(opts map[string]interface{}, key string, defaultVal int) int {
	if v, ok := opts[key]; ok {
		switch n := v.(type) {
		case float64:
			return int(n)
		case int:
			return n
		case int64:
			return int(n)
		}
	}
	return defaultVal
}

func getStringSliceOption(opts map[string]interface{}, key string) []string {
	if v, ok := opts[key]; ok {
		switch arr := v.(type) {
		case []interface{}:
			result := make([]string, 0, len(arr))
			for _, item := range arr {
				if s, ok := item.(string); ok {
					result = append(result, s)
				}
			}
			return result
		case []string:
			return arr
		}
	}
	return nil
}

func getStringMapOption(opts map[string]interface{}, key string) map[string]string {
	if v, ok := opts[key]; ok {
		switch m := v.(type) {
		case map[string]interface{}:
			result := make(map[string]string, len(m))
			for k, val := range m {
				if s, ok := val.(string); ok {
					result[k] = s
				}
			}
			return result
		case map[string]string:
			return m
		}
	}
	return nil
}
