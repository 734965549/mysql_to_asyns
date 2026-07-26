import { Message, Modal } from "@arco-design/web-vue";
import { API_BASE, handleApiError } from "./useApi.js";

/**
 * Task lifecycle API helpers shared by list and detail views.
 * @param {{ onChanged?: () => void }} [options]
 */
export function useTaskActions(options = {}) {
  const onChanged = typeof options.onChanged === "function" ? options.onChanged : () => {};

  async function cancelSchedule(taskId) {
    try {
      const res = await fetch(`${API_BASE}/tasks/${taskId}/cancel-schedule`, {
        method: "POST",
      });

      if (res.ok) {
        onChanged();
        Message.success("已取消定时启动");
      } else {
        const errorMsg = await handleApiError(res, "取消定时失败");
        Message.error(errorMsg);
      }
    } catch (e) {
      Message.error("取消定时失败: " + e.message);
    }
  }

  async function pauseTask(taskId) {
    try {
      const res = await fetch(`${API_BASE}/tasks/${taskId}/pause`, {
        method: "POST",
      });

      if (res.ok) {
        onChanged();
        Message.success("任务已暂停");
      } else {
        const errorMsg = await handleApiError(res, "暂停失败");
        Message.error(errorMsg);
      }
    } catch (e) {
      Message.error("暂停失败: " + e.message);
    }
  }

  async function endTask(taskId) {
    try {
      const res = await fetch(`${API_BASE}/tasks/${taskId}/end`, {
        method: "POST",
      });

      if (res.ok) {
        onChanged();
        Message.success("任务已结束");
      } else {
        const errorMsg = await handleApiError(res, "结束失败");
        Message.error(errorMsg);
      }
    } catch (e) {
      Message.error("结束失败: " + e.message);
    }
  }

  function confirmEndTask(taskId) {
    Modal.warning({
      title: "确认结束任务",
      content:
        "结束为终态操作，结束后原任务不能重新启动、编辑或设置定时调度（仍可查看、行数对比、复制新建和删除）。确定要结束该任务吗？",
      okText: "确认结束",
      cancelText: "取消",
      hideCancel: false,
      onOk: async () => {
        await endTask(taskId);
      },
    });
  }

  async function startRowCountComparison(taskId) {
    try {
      const res = await fetch(
        `${API_BASE}/tasks/${taskId}/row-count-comparison`,
        { method: "POST" },
      );

      if (res.status === 202) {
        Message.success("已开始行数对比，请稍后在详情页查看结果");
        onChanged();
        return true;
      }

      const errorMsg = await handleApiError(res, "启动行数对比失败");
      Message.error(errorMsg);
      return false;
    } catch (e) {
      Message.error("启动行数对比失败: " + e.message);
      return false;
    }
  }

  function confirmStartRowCountComparison(taskId) {
    Modal.confirm({
      title: "对比行数",
      content:
        "将对源端和目标端逐表执行精确 COUNT(*)，可能扫描大表并占用数据库资源。源端在任务结束后仍可能继续写入，结果为核对期间的行数快照。是否继续？",
      okText: "开始对比",
      cancelText: "取消",
      onOk: async () => {
        await startRowCountComparison(taskId);
      },
    });
  }

  async function deleteTask(taskId) {
    Modal.confirm({
      title: "确认删除",
      content: "确定要删除这个任务吗？",
      okText: "删除",
      cancelText: "取消",
      onOk: async () => {
        try {
          const res = await fetch(`${API_BASE}/tasks/${taskId}`, {
            method: "DELETE",
          });

          if (res.ok) {
            onChanged();
            Message.success("删除成功");
          } else {
            const errorMsg = await handleApiError(res, "删除失败");
            Message.error(errorMsg);
          }
        } catch (e) {
          Message.error("删除失败: " + e.message);
        }
      },
    });
  }

  async function startTask(taskId, payload = {}) {
    const hasPayload = payload && Object.keys(payload).length > 0;
    const res = await fetch(`${API_BASE}/tasks/${taskId}/start`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: hasPayload ? JSON.stringify(payload) : undefined,
    });
    return res;
  }

  return {
    cancelSchedule,
    pauseTask,
    endTask,
    confirmEndTask,
    startRowCountComparison,
    confirmStartRowCountComparison,
    deleteTask,
    startTask,
  };
}
