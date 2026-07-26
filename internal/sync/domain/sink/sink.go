package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"mysql-to-sync/pkg/crypto"
)

type SinkType string

const (
	SinkTypeMYSQL       SinkType = "MYSQL"
	SinkTypeKAFKA       SinkType = "KAFKA"
	SinkTypeHTTPWebhook SinkType = "HTTP_WEBHOOK"
)

type SinkConfig struct {
	Type    SinkType               `json:"type"`
	Options map[string]interface{} `json:"options"`
}

type ChangeEvent struct {
	TaskID       string                 `json:"task_id"`
	SourceSchema string                 `json:"source_schema"`
	SourceTable  string                 `json:"source_table"`
	EventType    string                 `json:"event_type"`
	EventTime    time.Time              `json:"event_time"`
	BinlogFile   string                 `json:"binlog_file"`
	BinlogPos    uint32                 `json:"binlog_pos"`
	PrimaryKeys  map[string]interface{} `json:"primary_keys"`
	Before       map[string]interface{} `json:"before,omitempty"`
	After        map[string]interface{} `json:"after,omitempty"`
	TraceID      string                 `json:"trace_id,omitempty"`
}

type Sink interface {
	Type() SinkType
	Open(ctx context.Context) error
	Write(ctx context.Context, event *ChangeEvent) error
	Flush(ctx context.Context) error
	Close(ctx context.Context) error
}

// TransactionalSink 支持按源 binlog 事务边界延迟提交的目标端。
// 同一源事务内的 Write 在 CommitTransaction 之前不得对外可见；失败时必须 RollbackTransaction。
type TransactionalSink interface {
	Sink
	BeginTransaction(ctx context.Context) error
	CommitTransaction(ctx context.Context) error
	RollbackTransaction(ctx context.Context) error
}

// DurableTxnPositionSink 在目标端事务内持久化源 binlog 提交位点。
// 用于消除「目标事务已提交、外部 checkpoint 尚未保存」窗口：重启重放前先查位点，已应用则跳过写入。
// MarkAppliedTxn 必须在活跃目标事务内、CommitTransaction 之前调用，与业务数据同事务提交。
type DurableTxnPositionSink interface {
	HasAppliedTxn(ctx context.Context, taskID string, pos mysql.Position) (bool, error)
	MarkAppliedTxn(ctx context.Context, taskID string, pos mysql.Position) error
}

type TablePreparer interface {
	PrepareTables(ctx context.Context, dbMapping map[string]string, tables []string) error
}

type BatchSink interface {
	WriteBatch(ctx context.Context, events []*ChangeEvent) error
}

// CloneConfigs returns a deep copy of sink configurations. Sink options are
// mutable maps, so a shallow slice copy is not enough when storage encryption
// or API redaction temporarily rewrites secret values.
func CloneConfigs(configs []SinkConfig) []SinkConfig {
	if configs == nil {
		return nil
	}
	cloned := make([]SinkConfig, len(configs))
	for i, cfg := range configs {
		cloned[i] = SinkConfig{
			Type:    cfg.Type,
			Options: cloneStringInterfaceMap(cfg.Options),
		}
	}
	return cloned
}

func cloneStringInterfaceMap(src map[string]interface{}) map[string]interface{} {
	if src == nil {
		return nil
	}
	dst := make(map[string]interface{}, len(src))
	for key, value := range src {
		dst[key] = cloneOptionValue(value)
	}
	return dst
}

func cloneOptionValue(value interface{}) interface{} {
	switch v := value.(type) {
	case map[string]interface{}:
		return cloneStringInterfaceMap(v)
	case map[string]string:
		cloned := make(map[string]string, len(v))
		for key, item := range v {
			cloned[key] = item
		}
		return cloned
	case []interface{}:
		cloned := make([]interface{}, len(v))
		for i, item := range v {
			cloned[i] = cloneOptionValue(item)
		}
		return cloned
	case []string:
		return append([]string(nil), v...)
	default:
		return value
	}
}

func SecretPaths(sinkType SinkType) []string {
	switch sinkType {
	case SinkTypeKAFKA:
		return []string{"security.sasl_password"}
	case SinkTypeHTTPWebhook:
		return []string{"headers"}
	default:
		return nil
	}
}

func getNestedValue(m map[string]interface{}, path string) (interface{}, bool) {
	parts := strings.Split(path, ".")
	var current interface{} = m
	for _, part := range parts {
		cm, ok := current.(map[string]interface{})
		if !ok {
			return nil, false
		}
		val, ok := cm[part]
		if !ok {
			return nil, false
		}
		current = val
	}
	return current, true
}

func setNestedValue(m map[string]interface{}, path string, value interface{}) bool {
	parts := strings.Split(path, ".")
	var current interface{} = m
	for i := 0; i < len(parts)-1; i++ {
		cm, ok := current.(map[string]interface{})
		if !ok {
			return false
		}
		val, ok := cm[parts[i]]
		if !ok {
			return false
		}
		current = val
	}
	cm, ok := current.(map[string]interface{})
	if !ok {
		return false
	}
	cm[parts[len(parts)-1]] = value
	return true
}

func EncryptSinkSecrets(configs []SinkConfig, key string) error {
	if key == "" {
		return nil
	}
	k := crypto.NormalizeKey(key)
	for i := range configs {
		if err := encryptSinkConfigSecrets(&configs[i], k); err != nil {
			return err
		}
	}
	return nil
}

func DecryptSinkSecrets(configs []SinkConfig, key string) error {
	if key == "" {
		return nil
	}
	k := crypto.NormalizeKey(key)
	for i := range configs {
		if err := decryptSinkConfigSecrets(&configs[i], k); err != nil {
			return err
		}
	}
	return nil
}

func encryptSinkConfigSecrets(cfg *SinkConfig, key string) error {
	paths := SecretPaths(cfg.Type)
	for _, path := range paths {
		switch path {
		case "security.sasl_password":
			val, ok := getNestedValue(cfg.Options, path)
			if !ok {
				continue
			}
			strVal, ok := val.(string)
			if !ok || strVal == "" || crypto.IsEncrypted(strVal) {
				continue
			}
			enc, err := crypto.Encrypt(strVal, key)
			if err != nil {
				return fmt.Errorf("encrypt %s.%s: %w", cfg.Type, path, err)
			}
			if !setNestedValue(cfg.Options, path, enc) {
				return fmt.Errorf("encrypt %s.%s: failed to set nested value", cfg.Type, path)
			}
		case "headers":
			headersVal, ok := cfg.Options["headers"]
			if !ok {
				continue
			}
			if strVal, ok := headersVal.(string); ok && crypto.IsEncrypted(strVal) {
				continue
			}
			jsonBytes, err := json.Marshal(headersVal)
			if err != nil {
				return fmt.Errorf("encrypt %s.headers: %w", cfg.Type, err)
			}
			enc, err := crypto.Encrypt(string(jsonBytes), key)
			if err != nil {
				return fmt.Errorf("encrypt %s.headers: %w", cfg.Type, err)
			}
			cfg.Options["headers"] = enc
		}
	}
	return nil
}

func decryptSinkConfigSecrets(cfg *SinkConfig, key string) error {
	paths := SecretPaths(cfg.Type)
	for _, path := range paths {
		switch path {
		case "security.sasl_password":
			val, ok := getNestedValue(cfg.Options, path)
			if !ok {
				continue
			}
			strVal, ok := val.(string)
			if !ok || strVal == "" {
				continue
			}
			dec, err := crypto.Decrypt(strVal, key)
			if err != nil {
				return fmt.Errorf("decrypt %s.%s: %w", cfg.Type, path, err)
			}
			if !setNestedValue(cfg.Options, path, dec) {
				return fmt.Errorf("decrypt %s.%s: failed to set nested value", cfg.Type, path)
			}
		case "headers":
			headersVal, ok := cfg.Options["headers"]
			if !ok {
				continue
			}
			strVal, ok := headersVal.(string)
			if !ok || strVal == "" {
				continue
			}
			dec, err := crypto.Decrypt(strVal, key)
			if err != nil {
				return fmt.Errorf("decrypt %s.headers: %w", cfg.Type, err)
			}
			var restored interface{}
			if err := json.Unmarshal([]byte(dec), &restored); err != nil {
				cfg.Options["headers"] = dec
				continue
			}
			cfg.Options["headers"] = restored
		}
	}
	return nil
}
