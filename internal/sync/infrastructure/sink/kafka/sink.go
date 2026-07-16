package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"mysql-to-sync/internal/sync/domain/sink"
	"mysql-to-sync/pkg/logger"
)

type KafkaSink struct {
	mu     sync.RWMutex
	writer messageWriter
	closed bool

	brokers        []string
	topic          string
	routingMode    string
	topicPrefix    string
	keyMode        string
	batchSize      int
	batchTimeoutMs int
	requiredAcks   int
	security       map[string]interface{}
}

type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

func NewKafkaSink(options map[string]interface{}) *KafkaSink {
	return &KafkaSink{
		brokers:        getStringSlice(options, "brokers"),
		topic:          getString(options, "topic"),
		routingMode:    getStringWithDefault(options, "routing_mode", "single_topic"),
		topicPrefix:    getString(options, "topic_prefix"),
		keyMode:        getStringWithDefault(options, "key_mode", "pk"),
		batchSize:      getIntWithDefault(options, "batch_size", 1000),
		batchTimeoutMs: getIntWithDefault(options, "batch_timeout_ms", 500),
		requiredAcks:   getIntWithDefault(options, "required_acks", 1),
		security:       getMap(options, "security"),
	}
}

func (s *KafkaSink) Type() sink.SinkType {
	return sink.SinkTypeKAFKA
}

func (s *KafkaSink) Validate() error {
	if len(s.brokers) == 0 {
		return fmt.Errorf("brokers is required")
	}
	for i, broker := range s.brokers {
		if strings.TrimSpace(broker) == "" {
			return fmt.Errorf("brokers[%d] must not be empty", i)
		}
	}
	switch s.routingMode {
	case "single_topic":
		if strings.TrimSpace(s.topic) == "" {
			return fmt.Errorf("topic is required for single_topic routing")
		}
	case "per_table":
		if strings.TrimSpace(s.topicPrefix) == "" {
			return fmt.Errorf("topic_prefix is required for per_table routing")
		}
	default:
		return fmt.Errorf("routing_mode must be single_topic or per_table")
	}
	if s.keyMode != "pk" && s.keyMode != "none" {
		return fmt.Errorf("key_mode must be pk or none")
	}
	if s.batchSize <= 0 {
		return fmt.Errorf("batch_size must be greater than 0")
	}
	if s.batchTimeoutMs < 0 {
		return fmt.Errorf("batch_timeout_ms must not be negative")
	}
	if s.requiredAcks != -1 && s.requiredAcks != 0 && s.requiredAcks != 1 {
		return fmt.Errorf("required_acks must be -1, 0, or 1")
	}
	if s.security == nil {
		return nil
	}
	mechanism := getString(s.security, "sasl_mechanism")
	if mechanism != "" {
		if getString(s.security, "sasl_username") == "" {
			return fmt.Errorf("security.sasl_username is required when SASL is enabled")
		}
		if getString(s.security, "sasl_password") == "" {
			return fmt.Errorf("security.sasl_password is required when SASL is enabled")
		}
		if _, err := s.buildSASLMechanism(mechanism, "validation", "validation"); err != nil {
			return err
		}
	}
	certPath := getString(s.security, "client_cert_path")
	keyPath := getString(s.security, "client_key_path")
	if (certPath == "") != (keyPath == "") {
		return fmt.Errorf("security.client_cert_path and security.client_key_path must be configured together")
	}
	if (certPath != "" || getString(s.security, "ca_cert_path") != "") && !getBool(s.security, "tls_enabled") {
		return fmt.Errorf("security.tls_enabled must be true when TLS certificate paths are configured")
	}
	return nil
}

func (s *KafkaSink) Open(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("kafka sink is closed")
	}
	if s.writer != nil {
		return fmt.Errorf("kafka sink is already open")
	}
	if err := s.Validate(); err != nil {
		return err
	}

	transport, err := s.buildTransport()
	if err != nil {
		return fmt.Errorf("build kafka transport: %w", err)
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(s.brokers...),
		Balancer:     &kafka.LeastBytes{},
		Transport:    transport,
		Async:        false,
		RequiredAcks: kafka.RequiredAcks(s.requiredAcks),
		BatchSize:    s.batchSize,
		BatchTimeout: time.Duration(s.batchTimeoutMs) * time.Millisecond,
	}

	s.writer = writer

	logger.Info("[KafkaSink] Opened writer to brokers=%v topic=%s routing_mode=%s key_mode=%s batch_size=%d batch_timeout=%dms required_acks=%d",
		s.brokers, s.topic, s.routingMode, s.keyMode, s.batchSize, s.batchTimeoutMs, s.requiredAcks)

	return nil
}

func (s *KafkaSink) Write(ctx context.Context, event *sink.ChangeEvent) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.writer == nil {
		return fmt.Errorf("kafka sink is not open")
	}
	if event == nil {
		return fmt.Errorf("change event is required")
	}

	topic := s.resolveTopic(event)
	key := s.buildKey(event)
	value, err := s.buildValue(event)
	if err != nil {
		return fmt.Errorf("serialize event: %w", err)
	}

	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
	}

	if err := s.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write kafka message to topic %s: %w", topic, err)
	}

	return nil
}

func (s *KafkaSink) Flush(ctx context.Context) error {
	return nil
}

func (s *KafkaSink) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	if s.writer != nil {
		if err := s.writer.Close(); err != nil {
			return fmt.Errorf("close kafka writer: %w", err)
		}
		s.writer = nil
	}

	return nil
}

func (s *KafkaSink) resolveTopic(event *sink.ChangeEvent) string {
	if s.routingMode == "per_table" {
		return fmt.Sprintf("%s.%s.%s", s.topicPrefix, event.SourceSchema, event.SourceTable)
	}
	return s.topic
}

func (s *KafkaSink) buildKey(event *sink.ChangeEvent) string {
	if s.keyMode == "none" || len(event.PrimaryKeys) == 0 {
		return fmt.Sprintf("%s.%s:%d", event.SourceSchema, event.SourceTable, event.BinlogPos)
	}
	var parts []string
	for _, col := range sortedKeys(event.PrimaryKeys) {
		parts = append(parts, fmt.Sprintf("%v", event.PrimaryKeys[col]))
	}
	return strings.Join(parts, ":")
}

func sortedKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 0; i < len(keys)-1; i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	return keys
}

func (s *KafkaSink) buildValue(event *sink.ChangeEvent) ([]byte, error) {
	return json.Marshal(event)
}

func (s *KafkaSink) buildTransport() (*kafka.Transport, error) {
	transport := &kafka.Transport{}

	if s.security != nil {
		if saslMechanism := getString(s.security, "sasl_mechanism"); saslMechanism != "" {
			saslUsername := getString(s.security, "sasl_username")
			saslPassword := getString(s.security, "sasl_password")

			mechanism, err := s.buildSASLMechanism(saslMechanism, saslUsername, saslPassword)
			if err != nil {
				return nil, err
			}
			transport.SASL = mechanism
		}

		if getBool(s.security, "tls_enabled") {
			tlsConfig, err := s.buildTLSConfig()
			if err != nil {
				return nil, err
			}
			transport.TLS = tlsConfig
		}
	}

	return transport, nil
}

func (s *KafkaSink) buildSASLMechanism(mechanism, username, password string) (sasl.Mechanism, error) {
	switch strings.ToUpper(mechanism) {
	case "PLAIN":
		return plain.Mechanism{
			Username: username,
			Password: password,
		}, nil
	case "SCRAM-SHA-256":
		return scram.Mechanism(scram.SHA256, username, password)
	case "SCRAM-SHA-512":
		return scram.Mechanism(scram.SHA512, username, password)
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", mechanism)
	}
}

func (s *KafkaSink) buildTLSConfig() (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: getBool(s.security, "insecure_skip_verify"),
	}

	if caCertPath := getString(s.security, "ca_cert_path"); caCertPath != "" {
		caCert, err := os.ReadFile(caCertPath)
		if err != nil {
			return nil, fmt.Errorf("read CA cert %s: %w", caCertPath, err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA cert %s", caCertPath)
		}
		tlsConfig.RootCAs = caCertPool
	}

	clientCertPath := getString(s.security, "client_cert_path")
	clientKeyPath := getString(s.security, "client_key_path")
	if clientCertPath != "" && clientKeyPath != "" {
		cert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
		if err != nil {
			return nil, fmt.Errorf("load client cert/key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

func getString(m map[string]interface{}, key string) string {
	v, ok := m[key]
	if !ok {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
}

func getStringWithDefault(m map[string]interface{}, key string, defaultVal string) string {
	v := getString(m, key)
	if v == "" {
		return defaultVal
	}
	return v
}

func getStringSlice(m map[string]interface{}, key string) []string {
	v, ok := m[key]
	if !ok {
		return nil
	}
	switch val := v.(type) {
	case []string:
		return val
	case []interface{}:
		result := make([]string, 0, len(val))
		for _, item := range val {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}
		return result
	}
	return nil
}

func getIntWithDefault(m map[string]interface{}, key string, defaultVal int) int {
	v, ok := m[key]
	if !ok {
		return defaultVal
	}
	switch val := v.(type) {
	case float64:
		return int(val)
	case int:
		return val
	case int64:
		return int(val)
	case json.Number:
		n, err := val.Int64()
		if err != nil {
			return defaultVal
		}
		return int(n)
	}
	return defaultVal
}

func getBool(m map[string]interface{}, key string) bool {
	v, ok := m[key]
	if !ok {
		return false
	}
	b, ok := v.(bool)
	if !ok {
		return false
	}
	return b
}

func getMap(m map[string]interface{}, key string) map[string]interface{} {
	v, ok := m[key]
	if !ok {
		return nil
	}
	mp, ok := v.(map[string]interface{})
	if !ok {
		return nil
	}
	return mp
}
