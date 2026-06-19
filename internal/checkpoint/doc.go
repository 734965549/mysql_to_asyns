// Package checkpoint stores incremental binlog positions.
//
// It intentionally does not store full-sync resume cursors. Full-sync table and
// row progress lives in task archives under context.full_sync_resume.
package checkpoint
