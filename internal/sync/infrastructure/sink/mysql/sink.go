package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"mysql-to-sync/internal/metadata/domain/entity"
	"mysql-to-sync/internal/metadata/domain/service"
	"mysql-to-sync/internal/metrics"
	"mysql-to-sync/internal/sync/domain/sink"
	"mysql-to-sync/internal/sync/infrastructure/writer"
	"mysql-to-sync/pkg/logger"
)

type MySQLSink struct {
	targetDB  *sql.DB
	writeConn *sql.Conn
	analyzer  service.IdentityAnalyzer
	batchSize int

	mu            sync.RWMutex
	writers       map[string]*writer.BufferedWriter
	identities    map[string]*entity.TableIdentity
	targetSchemas map[string]string
	closed        bool
}

func NewMySQLSink(targetDB *sql.DB, analyzer service.IdentityAnalyzer, batchSize int) *MySQLSink {
	return &MySQLSink{
		targetDB:      targetDB,
		analyzer:      analyzer,
		batchSize:     batchSize,
		writers:       make(map[string]*writer.BufferedWriter),
		identities:    make(map[string]*entity.TableIdentity),
		targetSchemas: make(map[string]string),
	}
}

func (s *MySQLSink) Type() sink.SinkType {
	return sink.SinkTypeMYSQL
}

func (s *MySQLSink) Open(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("mysql sink is closed")
	}
	if s.writeConn != nil {
		return fmt.Errorf("mysql sink is already open")
	}
	if s.targetDB == nil {
		return fmt.Errorf("target database is required")
	}
	if s.analyzer == nil {
		return fmt.Errorf("identity analyzer is required")
	}
	if s.batchSize <= 0 {
		return fmt.Errorf("batch size must be greater than 0")
	}
	writeConn, err := s.targetDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get write connection: %w", err)
	}
	s.writeConn = writeConn
	if _, err := writeConn.ExecContext(ctx, "SET SESSION FOREIGN_KEY_CHECKS=0"); err != nil {
		writeConn.Close()
		s.writeConn = nil
		return fmt.Errorf("failed to disable foreign key checks: %w", err)
	}
	return nil
}

func (s *MySQLSink) PrepareTables(ctx context.Context, dbMapping map[string]string, tables []string) error {
	s.mu.RLock()
	writeConn := s.writeConn
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return fmt.Errorf("mysql sink is closed")
	}
	if writeConn == nil {
		return fmt.Errorf("mysql sink is not open")
	}
	for srcDB, tgtDB := range dbMapping {
		for _, tableName := range tables {
			key := fmt.Sprintf("%s.%s", srcDB, tableName)
			s.mu.RLock()
			_, alreadyPrepared := s.writers[key]
			s.mu.RUnlock()
			if alreadyPrepared {
				continue
			}
			identity, err := s.analyzer.AnalyzeTable(srcDB, tableName)
			if err != nil {
				return err
			}

			bw := writer.NewBufferedWriterWithConn(
				writeConn,
				identity,
				s.batchSize,
				500*time.Millisecond,
				tgtDB,
			)
			bw.EnableUpsert()

			s.mu.Lock()
			s.identities[key] = identity
			s.targetSchemas[key] = tgtDB
			s.writers[key] = bw
			s.mu.Unlock()
		}
	}
	return nil
}

func (s *MySQLSink) Write(ctx context.Context, event *sink.ChangeEvent) error {
	if event == nil {
		return fmt.Errorf("change event is required")
	}
	key := fmt.Sprintf("%s.%s", event.SourceSchema, event.SourceTable)

	s.mu.RLock()
	identity := s.identities[key]
	targetSchema := s.targetSchemas[key]
	w := s.writers[key]
	s.mu.RUnlock()

	if w == nil || identity == nil {
		return fmt.Errorf("table %s is not prepared", key)
	}

	if identity.Strategy == entity.FullColumnsStrategy {
		metrics.GetMetrics().IncrementIncrementalNoPKTableEvents()
		logger.Warn("[NoPK][Task %s] Incremental event on table %s.%s (event=%s, strategy=FullColumns): falling back to full-column WHERE + LIMIT 1; idempotency is best-effort, recommend adding a primary or unique key",
			event.TaskID, event.SourceSchema, event.SourceTable, event.EventType)
	}

	switch event.EventType {
	case "INSERT":
		if event.After == nil {
			return fmt.Errorf("INSERT event for %s has no after image", key)
		}
		return s.handleInsert(w, event)
	case "UPDATE":
		if event.After == nil {
			return fmt.Errorf("UPDATE event for %s has no after image", key)
		}
		return s.handleUpdate(ctx, identity, targetSchema, event)
	case "DELETE":
		if event.Before == nil {
			return fmt.Errorf("DELETE event for %s has no before image", key)
		}
		return s.handleDelete(ctx, identity, targetSchema, event)
	default:
		return fmt.Errorf("unsupported event type %q", event.EventType)
	}
}

func (s *MySQLSink) handleInsert(w *writer.BufferedWriter, event *sink.ChangeEvent) error {
	if err := w.Write(event.After); err != nil {
		return err
	}
	return nil
}

func (s *MySQLSink) handleUpdate(ctx context.Context, identity *entity.TableIdentity, targetSchema string, event *sink.ChangeEvent) error {
	batchWriter := writer.NewBatchWriterWithConn(s.writeConn, identity, 1000, targetSchema)
	sqlBuilder := batchWriter.GetSQLBuilder()

	if identity.Strategy == entity.FullColumnsStrategy && event.Before != nil {
		return batchWriter.UpdateWithBeforeImage(ctx, event.After, event.Before)
	} else if event.Before != nil {
		if sqlBuilder.IdentityChanged(event.Before, event.After) {
			return s.deleteAndUpsert(ctx, identity, targetSchema, event.After, event.Before)
		}
		return batchWriter.UpdateWithBeforeImage(ctx, event.After, event.Before)
	}
	return batchWriter.Update(ctx, event.After)
}

func (s *MySQLSink) handleDelete(ctx context.Context, identity *entity.TableIdentity, targetSchema string, event *sink.ChangeEvent) error {
	batchWriter := writer.NewBatchWriterWithConn(s.writeConn, identity, 1000, targetSchema)
	return batchWriter.Delete(ctx, event.Before)
}

func (s *MySQLSink) deleteAndUpsert(ctx context.Context, identity *entity.TableIdentity, targetSchema string, row, beforeImage map[string]interface{}) error {
	tx, err := s.writeConn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx for identity-change update: %w", err)
	}
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()

	txWriter := writer.NewBatchWriterWithTx(tx, identity, 1000, targetSchema)
	txSQLBuilder := txWriter.GetSQLBuilder()

	deleteQuery, deleteArgs := txSQLBuilder.BuildDelete(beforeImage)
	if _, err := tx.ExecContext(ctx, deleteQuery, deleteArgs...); err != nil {
		return fmt.Errorf("delete old row in identity-change update: %w", err)
	}

	txWriter.EnableUpsert()
	if err := txWriter.WriteBatch(ctx, []map[string]interface{}{row}); err != nil {
		return fmt.Errorf("upsert new row in identity-change update: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit identity-change update: %w", err)
	}
	tx = nil

	logger.Info("[IdentityChange][Task ...] PK/UK identity changed on table %s.%s; deleted old row and upserted new row in transaction",
		targetSchema, identity.TableName)

	return nil
}

func (s *MySQLSink) Flush(ctx context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, w := range s.writers {
		if err := w.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (s *MySQLSink) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	writers := make([]*writer.BufferedWriter, 0, len(s.writers))
	for _, w := range s.writers {
		writers = append(writers, w)
	}
	writeConn := s.writeConn
	s.writeConn = nil
	s.writers = make(map[string]*writer.BufferedWriter)
	s.mu.Unlock()

	var firstErr error
	for _, w := range writers {
		if err := w.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close buffered writer: %w", err)
		}
	}
	if writeConn != nil {
		if _, err := writeConn.ExecContext(ctx, "SET SESSION FOREIGN_KEY_CHECKS=1"); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("restore foreign key checks: %w", err)
		}
		if err := writeConn.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close write connection: %w", err)
		}
	}
	return firstErr
}

func (s *MySQLSink) GetIdentity(key string) *entity.TableIdentity {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.identities[key]
}

func (s *MySQLSink) GetTargetSchema(key string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.targetSchemas[key]
}
