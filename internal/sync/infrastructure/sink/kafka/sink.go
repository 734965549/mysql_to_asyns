package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"mysql-to-async/internal/sync/domain/sink"

	kafkago "github.com/segmentio/kafka-go"
)

// Config Kafka Sink 配置
type Config struct {
	Brokers        []string `json:"brokers"`
	Topic          string   `json:"topic"`
	KeyMode        string   `json:"key_mode"`         // "pk" 或 "table"
	BatchSize      int      `json:"batch_size"`
	BatchTimeoutMs int      `json:"batch_timeout_ms"`
	RequiredAcks   int      `json:"required_acks"`
}

// KafkaSink Kafka 目标端实现
// key 默认取主键拼接值；无主键表则退化为 schema.table + binlog position
// 第一阶段默认单 topic
// 发送成功后才提交 checkpoint
// 同一个任务内部事件处理顺序优先保证"单表近似有序"，不强求全局严格有序
type KafkaSink struct {
	config Config
	writer *kafkago.Writer
	closed bool
}

// NewKafkaSink 创建 Kafka Sink
func NewKafkaSink(cfg Config) *KafkaSink {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.BatchTimeoutMs <= 0 {
		cfg.BatchTimeoutMs = 500
	}
	if cfg.RequiredAcks <= 0 {
		cfg.RequiredAcks = 1
	}
	return &KafkaSink{
		config: cfg,
	}
}

func (s *KafkaSink) Type() string {
	return "KAFKA"
}

func (s *KafkaSink) Open(ctx context.Context) error {
	s.writer = &kafkago.Writer{
		Addr:         kafkago.TCP(s.config.Brokers...),
		Topic:        s.config.Topic,
		Balancer:     &kafkago.LeastBytes{},
		BatchSize:    s.config.BatchSize,
		BatchTimeout: time.Duration(s.config.BatchTimeoutMs) * time.Millisecond,
		RequiredAcks: kafkago.RequiredAcks(s.config.RequiredAcks),
		Async:        false, // 同步写入，确保发送成功后才提交 checkpoint
	}

	log.Printf("[KafkaSink] Opened: brokers=%v, topic=%s, batch_size=%d",
		s.config.Brokers, s.config.Topic, s.config.BatchSize)
	return nil
}

// Write 写入单条变更事件到 Kafka
func (s *KafkaSink) Write(ctx context.Context, event *sink.ChangeEvent) error {
	if event == nil {
		return nil
	}

	key := s.buildMessageKey(event)
	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("kafka sink: marshal event failed: %w", err)
	}

	msg := kafkago.Message{
		Key:   []byte(key),
		Value: value,
	}

	if err := s.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafka sink: write message failed: %w", err)
	}
	return nil
}

func (s *KafkaSink) Flush(ctx context.Context) error {
	// kafka-go Writer 在同步模式下会自动 flush
	return nil
}

func (s *KafkaSink) Close(ctx context.Context) error {
	if s.closed {
		return nil
	}
	s.closed = true

	if s.writer != nil {
		if err := s.writer.Close(); err != nil {
			log.Printf("[KafkaSink] Close error: %v", err)
			return err
		}
	}

	log.Printf("[KafkaSink] Closed")
	return nil
}

// buildMessageKey 生成消息 key
// pk 模式：主键拼接值
// 无主键：退化为 schema.table + binlog position
func (s *KafkaSink) buildMessageKey(event *sink.ChangeEvent) string {
	if s.config.KeyMode == "pk" && len(event.PrimaryKeys) > 0 {
		parts := make([]string, 0, len(event.PrimaryKeys))
		for k, v := range event.PrimaryKeys {
			parts = append(parts, fmt.Sprintf("%s=%v", k, v))
		}
		return strings.Join(parts, ",")
	}

	// 退化 key：schema.table:binlog_file:binlog_pos
	return fmt.Sprintf("%s.%s:%s:%d",
		event.SourceSchema, event.SourceTable,
		event.BinlogFile, event.BinlogPos)
}
