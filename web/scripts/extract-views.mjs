#!/usr/bin/env node
/**
 * Extract template/style chunks from App.vue and generate StartTaskModal.vue.
 * Run from web/: node scripts/extract-views.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const WEB_ROOT = path.resolve(__dirname, "..");
const APP_VUE = path.join(WEB_ROOT, "src", "App.vue");

/** @param {string[]} lines @param {number} start 1-indexed inclusive @param {number} end 1-indexed inclusive */
function extractLines(lines, start, end) {
  if (start < 1 || end > lines.length || start > end) {
    throw new Error(`Invalid line range ${start}-${end} (file has ${lines.length} lines)`);
  }
  return lines.slice(start - 1, end).join("\n");
}

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function writeFile(filePath, content) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, content, "utf8");
  const lineCount = content === "" ? 0 : content.split("\n").length;
  console.log(`Wrote ${path.relative(WEB_ROOT, filePath)} (${lineCount} lines)`);
  return lineCount;
}

function buildStartTaskModal(templateBlock) {
  return `<template>
${templateBlock}
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

    const res = await fetch(\`\${API_BASE}/tasks/\${startTaskId.value}/start\`, {
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
`;
}

const EXTRACTS = [
  { out: "src/_extract/tpl-detail.html", start: 3043, end: 3857 },
  { out: "src/_extract/tpl-select-type.html", start: 3952, end: 4038 },
  { out: "src/_extract/tpl-form.html", start: 4041, end: 6053 },
  { out: "src/_extract/tpl-list.html", start: 6057, end: 6565 },
  { out: "src/_extract/tpl-config.html", start: 6569, end: 6756 },
  { out: "src/_extract/tpl-sider.html", start: 3862, end: 3897 },
  { out: "src/_extract/tpl-header.html", start: 3902, end: 3948 },
  { out: "src/_extract/all-styles.css", start: 7317, end: 10804 },
  { out: "src/_extract/all-scoped.css", start: 7317, end: 10804 },
];

const START_MODAL_TEMPLATE = { start: 7263, end: 7313 };

function main() {
  if (!fs.existsSync(APP_VUE)) {
    console.error(`App.vue not found: ${APP_VUE}`);
    process.exit(1);
  }

  const raw = fs.readFileSync(APP_VUE, "utf8");
  const lines = raw.split(/\r?\n/);

  console.log(`Reading ${path.relative(WEB_ROOT, APP_VUE)} (${lines.length} lines)\n`);

  const summary = [];

  for (const { out, start, end } of EXTRACTS) {
    const content = extractLines(lines, start, end);
    const lineCount = writeFile(path.join(WEB_ROOT, out), content);
    summary.push({ file: out, lineCount, source: `${start}-${end}` });
  }

  const modalTemplate = extractLines(
    lines,
    START_MODAL_TEMPLATE.start,
    START_MODAL_TEMPLATE.end,
  );
  const modalPath = path.join(WEB_ROOT, "src/components/StartTaskModal.vue");
  const modalContent = buildStartTaskModal(modalTemplate);
  const modalLineCount = writeFile(modalPath, modalContent);
  summary.push({
    file: "src/components/StartTaskModal.vue",
    lineCount: modalLineCount,
    source: `${START_MODAL_TEMPLATE.start}-${START_MODAL_TEMPLATE.end} + script`,
  });

  console.log("\nSummary:");
  for (const { file, lineCount, source } of summary) {
    console.log(`  ${file}: ${lineCount} lines (from App.vue ${source})`);
  }
}

main();
