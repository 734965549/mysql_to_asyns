package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	"mysql-to-async/internal/metadata/domain/entity"
	"mysql-to-async/internal/metadata/domain/service"
	"mysql-to-async/internal/sync/domain/sink"
	"mysql-to-async/internal/sync/infrastructure/writer"
)

// Config MySQL Sink 配置
type Config struct {
	TargetDB        *sql.DB
	Analyzer        service.IdentityAnalyzer
	TargetSchema    string
	TargetDatabases []string
	BatchSize       int
}

// MySQLSink 封装现有 MySQL writer，实现 Sink 接口
// INSERT、UPDATE、DELETE 行为必须与当前实现保持一致。
// 无主键表仍沿用 before image + FullColumnsStrategy。
type MySQLSink struct {
	config     Config
	writers    map[string]*writer.BufferedWriter
	identities map[string]*entity.TableIdentity
	schemas    map[string]string // key -> targetSchema
	mu         sync.RWMutex
	closed     bool
}

// NewMySQLSink 创建 MySQL Sink
func NewMySQLSink(cfg Config) *MySQLSink {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}
	return &MySQLSink{
		config:     cfg,
		writers:    make(map[string]*writer.BufferedWriter),
		identities: make(map[string]*entity.TableIdentity),
		schemas:    make(map[string]string),
	}
}

func (s *MySQLSink) Type() string {
	return "MYSQL"
}

func (s *MySQLSink) Open(ctx context.Context) error {
	// MySQL Sink 的 Open 是惰性的：writer 在首次 Write 时按需创建
	return nil
}

// Write 写入单条变更事件到 MySQL 目标端
func (s *MySQLSink) Write(ctx context.Context, event *sink.ChangeEvent) error {
	if event == nil {
		return nil
	}

	key := fmt.Sprintf("%s.%s", event.SourceSchema, event.SourceTable)
	targetSchema := s.resolveTargetSchema(event.SourceSchema)

	identity, err := s.getOrCreateIdentity(event.SourceSchema, event.SourceTable)
	if err != nil {
		return fmt.Errorf("mysql sink: analyze table %s failed: %w", key, err)
	}

	switch event.EventType {
	case "INSERT":
		return s.handleInsert(ctx, event, key, targetSchema, identity)
	case "UPDATE":
		return s.handleUpdate(ctx, event, key, targetSchema, identity)
	case "DELETE":
		return s.handleDelete(ctx, event, key, targetSchema, identity)
	default:
		return fmt.Errorf("mysql sink: unknown event type: %s", event.EventType)
	}
}

func (s *MySQLSink) Flush(ctx context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for key, w := range s.writers {
		if err := w.Flush(); err != nil {
			return fmt.Errorf("mysql sink: flush writer %s failed: %w", key, err)
		}
	}
	return nil
}

func (s *MySQLSink) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	for _, w := range s.writers {
		w.Close()
	}
	return nil
}

// --- internal helpers ---

func (s *MySQLSink) resolveTargetSchema(sourceSchema string) string {
	// 优先使用 TargetDatabases 映射（如果有的话）
	// 否则使用 TargetSchema
	// 否则使用 sourceSchema 本身
	if s.config.TargetSchema != "" {
		return s.config.TargetSchema
	}
	return sourceSchema
}

func (s *MySQLSink) getOrCreateIdentity(schema, table string) (*entity.TableIdentity, error) {
	key := fmt.Sprintf("%s.%s", schema, table)

	s.mu.RLock()
	if identity, ok := s.identities[key]; ok {
		s.mu.RUnlock()
		return identity, nil
	}
	s.mu.RUnlock()

	identity, err := s.config.Analyzer.AnalyzeTable(schema, table)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.identities[key] = identity
	s.mu.Unlock()

	return identity, nil
}

func (s *MySQLSink) getOrCreateWriter(key, targetSchema string, identity *entity.TableIdentity) *writer.BufferedWriter {
	s.mu.RLock()
	if w, ok := s.writers[key]; ok {
		s.mu.RUnlock()
		return w
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	// double check
	if w, ok := s.writers[key]; ok {
		return w
	}

	w := writer.NewBufferedWriterWithSchema(
		s.config.TargetDB,
		identity,
		s.config.BatchSize,
		500*time.Millisecond,
		targetSchema,
	)
	s.writers[key] = w
	s.schemas[key] = targetSchema

	return w
}

func (s *MySQLSink) handleInsert(ctx context.Context, event *sink.ChangeEvent, key, targetSchema string, identity *entity.TableIdentity) error {
	w := s.getOrCreateWriter(key, targetSchema, identity)

	if event.After != nil {
		if err := w.Write(event.After); err != nil {
			return err
		}
	}
	return w.Flush()
}

func (s *MySQLSink) handleUpdate(ctx context.Context, event *sink.ChangeEvent, key, targetSchema string, identity *entity.TableIdentity) error {
	batchWriter := writer.NewBatchWriterWithSchema(s.config.TargetDB, identity, s.config.BatchSize, targetSchema)

	if identity.Strategy == entity.FullColumnsStrategy && event.Before != nil {
		// 无主键表：使用 before image 作为 WHERE 条件
		if event.After == nil {
			return nil
		}
		return batchWriter.UpdateWithBeforeImage(ctx, event.After, event.Before)
	}

	// 有主键/唯一键表：直接使用 after image
	if event.After != nil {
		return batchWriter.Update(ctx, event.After)
	}
	return nil
}

func (s *MySQLSink) handleDelete(ctx context.Context, event *sink.ChangeEvent, key, targetSchema string, identity *entity.TableIdentity) error {
	batchWriter := writer.NewBatchWriterWithSchema(s.config.TargetDB, identity, s.config.BatchSize, targetSchema)

	row := event.Before
	if row == nil && event.After != nil {
		row = event.After
	}
	if row == nil {
		log.Printf("[MySQLSink] Warning: DELETE event has no row data for %s", key)
		return nil
	}
	return batchWriter.Delete(ctx, row)
}
