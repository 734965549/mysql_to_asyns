package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"mysql-to-async/internal/sync/domain/sink"
)

// Config Webhook Sink 配置
type Config struct {
	URL        string            `json:"url"`
	Method     string            `json:"method"`
	TimeoutMs  int               `json:"timeout_ms"`
	RetryTimes int               `json:"retry_times"`
	Headers    map[string]string `json:"headers"`
}

// WebhookSink HTTP Webhook 目标端实现
// 每条事件单独 POST
// 返回 HTTP 2xx 视为成功
// 非 2xx 视为失败并重试
// 重试耗尽后任务进入失败状态
type WebhookSink struct {
	config Config
	client *http.Client
	closed bool
}

// NewWebhookSink 创建 Webhook Sink
func NewWebhookSink(cfg Config) *WebhookSink {
	if cfg.Method == "" {
		cfg.Method = "POST"
	}
	if cfg.TimeoutMs <= 0 {
		cfg.TimeoutMs = 3000
	}
	if cfg.RetryTimes <= 0 {
		cfg.RetryTimes = 3
	}
	return &WebhookSink{
		config: cfg,
	}
}

func (s *WebhookSink) Type() string {
	return "HTTP_WEBHOOK"
}

func (s *WebhookSink) Open(ctx context.Context) error {
	s.client = &http.Client{
		Timeout: time.Duration(s.config.TimeoutMs) * time.Millisecond,
	}

	log.Printf("[WebhookSink] Opened: url=%s, method=%s, timeout=%dms, retries=%d",
		s.config.URL, s.config.Method, s.config.TimeoutMs, s.config.RetryTimes)
	return nil
}

// Write 发送单条变更事件到 Webhook
func (s *WebhookSink) Write(ctx context.Context, event *sink.ChangeEvent) error {
	if event == nil {
		return nil
	}

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("webhook sink: marshal event failed: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= s.config.RetryTimes; attempt++ {
		if attempt > 0 {
			// 简单退避：attempt * 500ms
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt*500) * time.Millisecond):
			}
			log.Printf("[WebhookSink] Retry attempt %d/%d for %s.%s",
				attempt, s.config.RetryTimes, event.SourceSchema, event.SourceTable)
		}

		lastErr = s.doPost(ctx, body)
		if lastErr == nil {
			return nil
		}
	}

	return fmt.Errorf("webhook sink: all %d retries exhausted: %w", s.config.RetryTimes, lastErr)
}

func (s *WebhookSink) Flush(ctx context.Context) error {
	return nil
}

func (s *WebhookSink) Close(ctx context.Context) error {
	if s.closed {
		return nil
	}
	s.closed = true

	if s.client != nil {
		s.client.CloseIdleConnections()
	}

	log.Printf("[WebhookSink] Closed")
	return nil
}

func (s *WebhookSink) doPost(ctx context.Context, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, s.config.Method, s.config.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request failed: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range s.config.Headers {
		req.Header.Set(k, v)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	return fmt.Errorf("http status %d", resp.StatusCode)
}
