-- sys_sync_task_events 任务关键事件表（无 FK 到 sys_sync_tasks）
-- 由应用在 mysql 存储模式下自动建表；DBA 也可提前执行本脚本。

CREATE TABLE IF NOT EXISTS sys_sync_task_events (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  task_id VARCHAR(64) NOT NULL COMMENT '任务 ID',
  execution_id VARCHAR(64) NOT NULL COMMENT '单次启动 execution',
  seq BIGINT NOT NULL COMMENT '任务内单调序号',
  event_id VARCHAR(64) NOT NULL COMMENT '事件 UUID',
  occurred_at TIMESTAMP(3) NOT NULL COMMENT '事件发生时间',
  severity VARCHAR(16) NOT NULL COMMENT 'INFO/WARN/ERROR',
  visibility VARCHAR(16) NOT NULL COMMENT 'KEY/DIAGNOSTIC',
  category VARCHAR(32) NOT NULL DEFAULT '' COMMENT '事件分类',
  code VARCHAR(64) NOT NULL COMMENT '事件码',
  phase VARCHAR(64) NOT NULL DEFAULT '' COMMENT '同步阶段',
  source_schema VARCHAR(128) NOT NULL DEFAULT '',
  source_table VARCHAR(256) NOT NULL DEFAULT '',
  message TEXT NOT NULL,
  details JSON NULL,
  repeat_count INT NOT NULL DEFAULT 0,
  first_at TIMESTAMP(3) NULL,
  last_at TIMESTAMP(3) NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_task_seq (task_id, seq),
  KEY idx_task_execution_seq (task_id, execution_id, seq),
  KEY idx_task_severity_seq (task_id, severity, seq),
  KEY idx_task_code_seq (task_id, code, seq),
  KEY idx_occurred_at (occurred_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
