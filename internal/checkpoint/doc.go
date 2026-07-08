// Package checkpoint stores incremental binlog positions.
//
// It intentionally does not store full-sync resume cursors. Historical
// full-sync progress fields remain in task archives under context.full_sync_resume.
package checkpoint
