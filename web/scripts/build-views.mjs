#!/usr/bin/env node
/**
 * Build all Vue SFCs for the router split from App.vue + extracts.
 * Run: node web/scripts/build-views.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const WEB = path.resolve(__dirname, "..");
const SRC = path.join(WEB, "src");
const EXT = path.join(SRC, "_extract");

const read = (p) => fs.readFileSync(p, "utf8");
const write = (rel, content) => {
  const p = path.join(WEB, rel);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, content.replace(/\r\n/g, "\n"), "utf8");
  console.log("OK", rel, `(${content.split("\n").length} lines)`);
};

const app = read(path.join(SRC, "App.vue"));
const appLines = app.split(/\r?\n/);
const slice = (a, b) => appLines.slice(a - 1, b).join("\n");

function cleanTpl(html, { stripVif = true } = {}) {
  let t = html;
  if (stripVif) {
    t = t.replace(/\s+v-if="isTaskDetailPage"/g, "");
    t = t.replace(/\s+v-if="!isTaskDetailPage"/g, "");
    t = t.replace(/\s+v-if="taskFormPage === 'select_type'"/g, "");
    t = t.replace(
      /\s+v-if="taskFormPage === 'create' \|\| taskFormPage === 'edit'"/g,
      "",
    );
    t = t.replace(
      /\s+v-show="taskFormPage === 'none' && currentPage === 'tasks'"/g,
      "",
    );
    t = t.replace(
      /\s+v-show="taskFormPage === 'none' && currentPage === 'config'"/g,
      "",
    );
  }
  t = t.replace(/\s*:class="appThemeClass"/g, "");
  t = t.replace(/taskFormPage === 'edit'/g, "editMode");
  t = t.replace(/taskFormPage === \"edit\"/g, "editMode");
  t = t.replace(
    /@click="taskFormPage = 'select_type'"/g,
    `@click="goSelectType"`,
  );
  return t;
}

// ========== StartTaskModal (already good) ==========
{
  const modalTpl = read(path.join(EXT, "tpl-modal.html"));
  // keep existing StartTaskModal if present with emit success
}

// ========== MainLayout ==========
{
  const sider = cleanTpl(read(path.join(EXT, "tpl-sider.html")))
    .replace(
      /v-model:selected-keys="selectedKey"/,
      ':selected-keys="selectedKeys"',
    );
  write(
    "src/layouts/MainLayout.vue",
    `<script setup>
import { computed } from "vue";
import { useRoute, useRouter } from "vue-router";
import {
  formHeaderActions,
  clearFormHeaderActions,
} from "../composables/useFormHeaderActions.js";

const route = useRoute();
const router = useRouter();

const selectedKeys = computed(() =>
  route.path.startsWith("/config") ? ["config"] : ["tasks"],
);

const isTasksList = computed(() => route.name === "tasks");
const isFormRoute = computed(() =>
  ["task-select-type", "task-create", "task-create-config", "task-edit"].includes(
    route.name,
  ),
);
const showBack = computed(() => isFormRoute.value);
const formActions = computed(() => formHeaderActions.value);

const pageTitle = computed(() => {
  if (route.name === "task-edit" || route.meta?.mode === "edit") return "编辑任务";
  if (isFormRoute.value) return "创建同步任务";
  if (route.name === "config") return "系统配置";
  return "任务管理";
});

function onMenuClick(key) {
  clearFormHeaderActions();
  router.push(key === "config" ? "/config" : "/tasks");
}

function goBack() {
  clearFormHeaderActions();
  router.push("/tasks");
}

function goCreate() {
  router.push("/tasks/new/select");
}
</script>

<template>
  <a-layout class="layout-container">
${sider}
    <a-layout>
      <a-layout-header class="header">
        <div class="header-left">
          <a-button v-if="showBack" type="text" style="margin-right: 8px" @click="goBack">
            <template #icon><icon-arrow-left /></template>
            返回
          </a-button>
          <a-typography-title :heading="5" style="margin: 0">{{ pageTitle }}</a-typography-title>
        </div>
        <div class="header-right" v-if="isTasksList">
          <a-button type="primary" @click="goCreate">
            <template #icon><icon-plus /></template>
            创建同步任务
          </a-button>
        </div>
        <div class="header-right" v-if="formActions">
          <a-space>
            <a-button @click="formActions.cancel()">取消</a-button>
            <a-button
              type="primary"
              :loading="formActions.loading.value"
              @click="formActions.submit()"
            >
              {{ formActions.editMode.value ? "更新" : "创建" }}
            </a-button>
          </a-space>
        </div>
      </a-layout-header>
      <a-layout-content class="content">
        <router-view />
      </a-layout-content>
    </a-layout>
  </a-layout>
</template>

<style scoped>
${read(path.join(EXT, "style-layout.css"))}
</style>
`,
  );
}

// ========== TaskSelectTypeView ==========
{
  write(
    "src/views/TaskSelectTypeView.vue",
    `<script setup>
import { useRouter } from "vue-router";
const router = useRouter();
function proceedToCreateTask(type) {
  router.push({ path: "/tasks/new/config", query: { type } });
}
</script>

<template>
${cleanTpl(read(path.join(EXT, "tpl-select-type.html")))}
</template>

<style scoped>
${read(path.join(EXT, "style-select.css"))}
</style>
`,
  );
}

console.log("layout + select done; writing page scripts via page builders...");

// Dump script body of App.vue for reference chunks
const scriptEnd = appLines.findIndex((l) => l.trim() === "</script>");
const scriptBody = appLines.slice(1, scriptEnd).join("\n"); // skip <script setup>
fs.writeFileSync(path.join(EXT, "app-script.js"), scriptBody, "utf8");
console.log("dumped app-script.js", scriptBody.split("\n").length);
