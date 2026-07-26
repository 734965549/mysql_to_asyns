/**
 * Assemble router views/layout from App.vue + _extract templates/styles.
 * Run from repo root: node web/scripts/assemble-all.mjs
 */
import fs from "fs";
import path from "path";

const root = path.resolve("web/src");
const extractDir = path.join(root, "_extract");
const appLines = fs.readFileSync(path.join(root, "App.vue"), "utf8").split(/\r?\n/);
const slice = (a, b) => appLines.slice(a - 1, b).join("\n");
const readExt = (n) => fs.readFileSync(path.join(extractDir, n), "utf8");

fs.mkdirSync(path.join(root, "views"), { recursive: true });
fs.mkdirSync(path.join(root, "layouts"), { recursive: true });

function stripOuterVIf(html, className) {
  // remove first attribute v-if="..." or v-show="..." on root-ish tags
  return html
    .replace(/\s+v-if="[^"]*"/g, (m, offset, s) => {
      // only strip the first few occurrences on layout wrappers
      return m.includes("isTaskDetailPage") ||
        m.includes("taskFormPage") ||
        m.includes("currentPage") ||
        m.includes("!isTaskDetailPage")
        ? ""
        : m;
    })
    .replace(/\s+v-show="[^"]*"/g, (m) =>
      m.includes("taskFormPage") || m.includes("currentPage") ? "" : m,
    )
    .replace(/\s*:class="appThemeClass"/g, "");
}

// ---------- themes already written; ensure exists ----------
if (!fs.existsSync(path.join(root, "styles/themes.css"))) {
  console.warn("themes.css missing");
}

// ---------- MainLayout ----------
{
  const sider = readExt("tpl-sider.html")
    .replace(/v-model:selected-keys="selectedKey"/g, ':selected-keys="selectedKeys"')
    .replace(/@menu-item-click="onMenuClick"/g, "@menu-item-click=\"onMenuClick\"");
  const header = `      <a-layout-header class="header">
        <div class="header-left">
          <a-button
            v-if="showBack"
            type="text"
            style="margin-right: 8px"
            @click="goBack"
          >
            <template #icon><icon-arrow-left /></template>
            返回
          </a-button>
          <a-typography-title :heading="5" style="margin: 0">
            {{ pageTitle }}
          </a-typography-title>
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
            <a-button type="primary" :loading="formActions.loading.value" @click="formActions.submit()">
              {{ formActions.editMode.value ? "更新" : "创建" }}
            </a-button>
          </a-space>
        </div>
      </a-layout-header>`;

  const content = `<script setup>
import { computed } from "vue";
import { useRoute, useRouter } from "vue-router";
import { formHeaderActions, clearFormHeaderActions } from "../composables/useFormHeaderActions.js";

const route = useRoute();
const router = useRouter();

const selectedKeys = computed(() => {
  if (route.path.startsWith("/config")) return ["config"];
  return ["tasks"];
});

const isTasksList = computed(() => route.name === "tasks");
const isFormRoute = computed(() =>
  ["task-select-type", "task-create", "task-create-config", "task-edit"].includes(route.name),
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
  if (formHeaderActions.value) clearFormHeaderActions();
  if (key === "config") router.push("/config");
  else router.push("/tasks");
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
${header}
      <a-layout-content class="content">
        <router-view />
      </a-layout-content>
    </a-layout>
  </a-layout>
</template>

<style scoped>
${readExt("style-layout.css")}
</style>
`;
  fs.writeFileSync(path.join(root, "layouts/MainLayout.vue"), content);
  console.log("MainLayout.vue", content.split("\n").length);
}

// ---------- TaskSelectTypeView ----------
{
  let tpl = readExt("tpl-select-type.html");
  tpl = tpl.replace(/\s*v-if="taskFormPage === 'select_type'"/, "");
  tpl = tpl.replace(
    /@click="proceedToCreateTask\('([^']+)'\)"/g,
    `@click="proceedToCreateTask('$1')"`,
  );
  const content = `<script setup>
import { useRouter } from "vue-router";

const router = useRouter();

function proceedToCreateTask(type) {
  router.push({ path: "/tasks/new/config", query: { type } });
}
</script>

<template>
${tpl}
</template>

<style scoped>
${readExt("style-select.css")}
</style>
`;
  fs.writeFileSync(path.join(root, "views/TaskSelectTypeView.vue"), content);
  console.log("TaskSelectTypeView.vue", content.split("\n").length);
}

console.log("partial assemble done — detail/config/list/form follow in assemble-pages.mjs");
