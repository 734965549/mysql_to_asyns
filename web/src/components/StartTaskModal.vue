<template>
  <a-modal
    v-model:visible="startModalVisible"
    :title="startMode === 'immediate' ? '确认立即启动' : 'Cron 定时启动'"
    @ok="confirmStartTask"
    @cancel="startModalVisible = false"
    ok-text="确认"
    cancel-text="取消"
    :width="720"
  >
    <a-form layout="vertical">
      <a-alert
        v-if="startMode === 'immediate'"
        type="warning"
        :show-icon="true"
        style="margin-bottom: 16px"
        title="确认后将立即启动任务"
        description="如果你希望设置定时启动，请切换到定时启动方式后再提交。"
      />

      <template v-else>
        <a-alert
          type="info"
          :show-icon="true"
          style="margin-bottom: 16px"
          title="Cron 支持标准语法，并兼容 L / W / #"
          description="例如：0 9 * * 1-5 表示每周一到周五 09:00；0 0 L * * 表示每月最后一天 00:00。"
        />

        <a-form-item label="Cron 表达式">
          <a-input v-model="scheduleCron" placeholder="例如：0 9 * * 1-5" />
        </a-form-item>

        <a-form-item label="时区">
          <a-input v-model="scheduleTimezone" placeholder="例如：Asia/Shanghai" />
        </a-form-item>

        <a-form-item label="快捷模板">
          <a-space wrap>
            <a-button size="small" @click="scheduleCron = '0 9 * * 1-5'">工作日 9:00</a-button>
            <a-button size="small" @click="scheduleCron = '30 9 * * 1-5'">工作日 9:30</a-button>
            <a-button size="small" @click="scheduleCron = '0 0 L * *'">每月最后一个工作日 00:00</a-button>
            <a-button size="small" @click="scheduleCron = '0 10 ? * 1#1'">每月第一个周一 10:00</a-button>
          </a-space>
        </a-form-item>

        <a-typography-text type="secondary" style="font-size: 12px">
          支持标准 cron 与扩展语义。提交后系统会保存原始表达式，并据此计算下一次触发时间。
        </a-typography-text>
      </template>
    </a-form>
  </a-modal>
</template>

<script setup>
import { ref } from "vue";
import { Message } from "@arco-design/web-vue";
import { API_BASE, handleApiError } from "../composables/useApi.js";

const emit = defineEmits(["success"]);

const startModalVisible = ref(false);
const startTaskId = ref("");
const startMode = ref("immediate");
const scheduleCron = ref("0 9 * * 1-5");
const scheduleTimezone = ref(
  Intl.DateTimeFormat().resolvedOptions().timeZone || "Asia/Shanghai",
);

function openStartTaskModal(taskId, mode = "immediate") {
  startTaskId.value = taskId;
  startMode.value = mode;
  scheduleCron.value = "0 9 * * 1-5";
  scheduleTimezone.value =
    Intl.DateTimeFormat().resolvedOptions().timeZone || "Asia/Shanghai";
  startModalVisible.value = true;
}

async function confirmStartTask() {
  try {
    let payload = {};
    let successMsg = "任务已启动";
    let failMsg = "启动失败";

    if (startMode.value === "cron") {
      const expr = String(scheduleCron.value || "").trim();
      if (!expr) {
        Message.error("请输入 cron 表达式");
        return;
      }
      payload = {
        scheduled_at: new Date().toISOString(),
        schedule_mode: "cron",
        cron_expression: expr,
        cron_timezone: String(scheduleTimezone.value || "").trim(),
      };
      successMsg = "定时启动已设置";
      failMsg = "设置定时启动失败";
    }

    const res = await fetch(`${API_BASE}/tasks/${startTaskId.value}/start`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: Object.keys(payload).length ? JSON.stringify(payload) : undefined,
    });

    if (res.ok) {
      emit("success");
      Message.success(successMsg);
      startModalVisible.value = false;
    } else {
      const errorMsg = await handleApiError(res, failMsg);
      Message.error(errorMsg);
    }
  } catch (e) {
    Message.error(
      (startMode.value === "cron" ? "设置定时启动失败" : "启动失败") +
        ": " +
        e.message,
    );
  }
}

defineExpose({ openStartTaskModal });
</script>
