-- sys_sync_tasks 升级脚本
-- 说明：请由 DBA 在维护窗口内统一执行，用于对齐任务元数据表结构
-- 适用场景：旧版本升级后，任务表缺少 created_at / updated_at 字段

ALTER TABLE sys_sync_tasks
  ADD COLUMN created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '任务创建时间',
  ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '任务更新时间';

-- 如需补充索引，可由 DBA 按实际查询场景评估后再执行
-- 例如：
-- ALTER TABLE sys_sync_tasks ADD UNIQUE KEY uk_task_id (id);
