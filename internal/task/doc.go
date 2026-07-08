// Package task owns the task aggregate boundary.
//
// The task domain stores external lifecycle state, sync phase, scheduling
// metadata, runtime isolation, and historical full-sync checkpoint fields. Full
// sync no longer resumes after interruption; incremental binlog positions are
// owned by the checkpoint package.
package task
