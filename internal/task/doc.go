// Package task owns the task aggregate boundary.
//
// The task domain stores external lifecycle state, sync phase, scheduling
// metadata, runtime isolation, and full-sync resume state. Full-sync resume data
// is persisted with task archives; incremental binlog positions are owned by
// the checkpoint package.
package task
