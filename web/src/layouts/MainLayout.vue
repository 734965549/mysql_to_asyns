<script setup>
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
    <a-layout-sider :width="220" :collapsible="false" class="sider">
      <div class="logo">
        <div class="logo-icon">
          <icon-storage />
        </div>

        <span class="logo-text">MySQL 数据同步</span>
      </div>

      <a-menu
        :selected-keys="selectedKeys"
        :auto-open-selected="true"
        :collapsed="false"
        class="sider-menu"
        theme="dark"
        @menu-item-click="onMenuClick"
      >
        <a-menu-item key="tasks">
          <template #icon><icon-list /></template>

          任务管理
        </a-menu-item>

        <a-menu-item key="config">
          <template #icon><icon-settings /></template>

          系统配置
        </a-menu-item>
      </a-menu>

      <div class="sider-footer">
        <a-typography-text type="secondary">
          MySQL to Async v1.0
        </a-typography-text>
      </div>
    </a-layout-sider>
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
.layout-container {
  height: 100vh;

  background: #f5f7fa;
}
.sider {
  background: linear-gradient(180deg, #1d2129 0%, #165dff 100%);

  display: flex;

  flex-direction: column;
}
.logo {
  height: 64px;

  display: flex;

  align-items: center;

  padding: 0 20px;

  color: #fff;

  border-bottom: 1px solid rgba(255, 255, 255, 0.1);
}
.logo-icon {
  width: 32px;

  height: 32px;

  background: rgba(255, 255, 255, 0.2);

  border-radius: 8px;

  display: flex;

  align-items: center;

  justify-content: center;

  margin-right: 12px;

  font-size: 18px;
}
.logo-text {
  font-size: 16px;

  font-weight: 600;

  letter-spacing: 0.5px;
}
.sider-menu {
  flex: 1;

  background: transparent !important;

  padding: 12px 8px;

  width: 100% !important;
}
/* 禁止菜单收缩 */

.sider-menu:not(.arco-menu-collapsed) {
  width: 100% !important;
}
/* 菜单inner容器 - 禁止动画 */

.sider-menu :deep(.arco-menu-inner) {
  display: flex !important;

  flex-direction: column !important;

  opacity: 1 !important;

  animation: none !important;

  transition: none !important;
}
/* 菜单项 - 常亮显示，禁止所有动画和过渡 */

.sider-menu :deep(.arco-menu-item) {
  color: #fff !important;

  background: transparent !important;

  margin: 4px 0;

  border-radius: 6px;

  opacity: 1 !important;

  visibility: visible !important;

  padding: 0 12px !important;

  height: 40px !important;

  line-height: 40px !important;

  animation: none !important;

  transition: none !important;

  transform: none !important;
}
/* 强制覆盖Arco Design菜单项的所有状态背景 */

.sider-menu :deep(.arco-menu-item),
.sider-menu :deep(.arco-menu-item.arco-menu-selected),
.sider-menu :deep(.arco-menu-item:hover),
.sider-menu :deep(.arco-menu-item:focus),
.sider-menu :deep(.arco-menu-item:active),
.sider-menu :deep(.arco-menu-item[data-key="tasks"]),
.sider-menu :deep(.arco-menu-item[data-key="config"]) {
  background: transparent !important;

  background-color: transparent !important;
}
/* 菜单项图标 - 常亮显示，禁止动画 */

.sider-menu :deep(.arco-menu-item .arco-menu-icon) {
  color: #fff !important;

  opacity: 1 !important;

  visibility: visible !important;

  display: inline-flex !important;

  margin-right: 12px !important;

  animation: none !important;

  transition: none !important;
}
/* 菜单项文字 - 常亮显示，禁止动画 */

.sider-menu :deep(.arco-menu-item .arco-menu-title) {
  color: #fff !important;

  opacity: 1 !important;

  visibility: visible !important;

  display: inline !important;

  animation: none !important;

  transition: none !important;

  transform: none !important;
}
/* 菜单项内所有内容 */

.sider-menu :deep(.arco-menu-item *) {
  color: #fff !important;

  opacity: 1 !important;

  visibility: visible !important;

  animation: none !important;

  transition: none !important;
}
/* 禁止Arco Design菜单的所有动画效果 */

.sider-menu :deep(.arco-menu-collapse-icon) {
  display: none !important;
}
/* 禁止菜单展开/折叠的宽度动画 */

.sider-menu :deep(.arco-menu) {
  transition: none !important;

  animation: none !important;
}
/* 悬停状态 - 只改变背景，不改变透明度 */

.sider-menu :deep(.arco-menu-item:hover) {
  background: rgba(255, 255, 255, 0.15) !important;

  color: #fff !important;
}
/* 选中状态 */

.sider-menu :deep(.arco-menu-item.arco-menu-selected) {
  background: rgba(255, 255, 255, 0.25) !important;

  color: #fff !important;
}
/* 禁止所有伪元素动画 */

.sider-menu :deep(.arco-menu-item::before),
.sider-menu :deep(.arco-menu-item::after) {
  animation: none !important;

  transition: none !important;
}
/* 确保菜单图标svg也常亮 */

.sider-menu :deep(.arco-menu-item svg) {
  opacity: 1 !important;

  visibility: visible !important;

  color: #fff !important;
}
.sider-footer {
  padding: 16px 20px;

  border-top: 1px solid rgba(255, 255, 255, 0.1);
}
.header {
  background: #fff;

  padding: 0 24px;

  display: flex;

  justify-content: space-between;

  align-items: center;

  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.08);

  z-index: 10;
}
.content {
  padding: 24px;

  overflow-y: auto;
}
/* Sci-fi create task surface - uses CSS variables for theme-awareness */
.layout-container {
  background:
    radial-gradient(circle at 12% 8%, var(--app-glow), transparent 28%),
    linear-gradient(135deg, var(--app-bg) 0%, var(--app-surface-strong) 100%);
}
.sider {
  background: linear-gradient(180deg, var(--app-surface-strong) 0%, var(--app-surface) 100%);
  border-right: 1px solid var(--app-border);
  box-shadow: 12px 0 38px rgba(0, 0, 0, 0.28);
}
.sider :deep(.arco-layout-sider-children) {
  display: flex;
  flex-direction: column;
  min-height: 100vh;
}
.sider-footer {
  margin-top: auto;
}
.logo {
  border-bottom-color: var(--app-border);
}
.logo-icon {
  background: linear-gradient(135deg, var(--app-accent) 0%, var(--app-accent-2) 100%);
  box-shadow: 0 0 24px var(--app-glow);
}
.header {
  background: var(--app-surface-strong);
  border-bottom: 1px solid var(--app-border);
  color: var(--app-text);
  backdrop-filter: blur(18px);
  box-shadow: 0 12px 34px rgba(0, 0, 0, 0.24);
}
.header :deep(.arco-typography) {
  color: var(--app-text);
}
.content {
  background:
    linear-gradient(color-mix(in srgb, var(--app-accent) 3.5%, transparent) 1px, transparent 1px),
    linear-gradient(90deg, color-mix(in srgb, var(--app-accent-2) 3.5%, transparent) 1px, transparent 1px);
  background-size: 28px 28px;
}
.header-right :deep(.arco-btn-primary) {
  border: 0;
  background: linear-gradient(135deg, var(--app-accent) 0%, var(--app-accent-2) 100%);
  box-shadow: 0 0 26px var(--app-glow);
}
/* Unified theme pass - default variables (overridden by theme classes) */
.layout-container {
  --app-bg: #07111f;
  --app-surface: rgba(12, 24, 42, 0.94);
  --app-surface-soft: rgba(16, 33, 56, 0.9);
  --app-surface-strong: rgba(8, 17, 31, 0.98);
  --app-border: rgba(72, 188, 226, 0.28);
  --app-border-soft: rgba(113, 168, 199, 0.16);
  --app-text: #edf7ff;
  --app-muted: #9fb5c9;
  --app-accent: #20c7e8;
  --app-accent-2: #3b82f6;
  --app-glow: rgba(32, 199, 232, 0.24);
  background:
    radial-gradient(circle at 15% 12%, var(--app-glow), transparent 30%),
    linear-gradient(135deg, var(--app-bg) 0%, var(--app-surface-strong) 100%);
}
.sider {
  background: linear-gradient(180deg, var(--app-surface-strong) 0%, var(--app-surface) 100%);
  border-right-color: var(--app-border);
  box-shadow: 10px 0 34px rgba(0, 0, 0, 0.28);
}
.logo {
  border-bottom-color: var(--app-border);
}
.logo-icon {
  background: linear-gradient(135deg, var(--app-accent) 0%, var(--app-accent-2) 100%);
  box-shadow: 0 0 22px var(--app-glow);
}
.sider-menu :deep(.arco-menu-item:hover) {
  background: color-mix(in srgb, var(--app-accent) 12%, transparent) !important;
}
.sider-menu :deep(.arco-menu-item.arco-menu-selected) {
  background: color-mix(in srgb, var(--app-muted) 22%, transparent) !important;
  box-shadow: inset 3px 0 0 var(--app-accent);
}
.header {
  background: var(--app-surface-strong);
  border-bottom-color: var(--app-border);
}
.content {
  background:
    linear-gradient(color-mix(in srgb, var(--app-accent-2) 3.5%, transparent) 1px, transparent 1px),
    linear-gradient(90deg, color-mix(in srgb, var(--app-accent) 3.5%, transparent) 1px, transparent 1px);
  background-size: 30px 30px;
}
.header-right :deep(.arco-btn-primary) {
  border: 0;
  background: linear-gradient(135deg, var(--app-accent) 0%, var(--app-accent-2) 100%);
  box-shadow: 0 0 26px var(--app-glow);
}
/* Mobile / narrow viewport: stack sider on top, horizontal menu */
@media (max-width: 768px) {
  .layout-container {
    height: auto;
    min-height: 100vh;
    flex-direction: column;
  }
  .sider {
    width: 100% !important;
    max-width: none !important;
    min-width: 0 !important;
    flex: 0 0 auto !important;
    height: auto;
  }
  .sider :deep(.arco-layout-sider-children) {
    min-height: 0;
  }
  .logo {
    height: 56px;
  }
  .sider-menu {
    flex: 0 0 auto;
    padding: 8px 10px;
  }
  .sider-menu :deep(.arco-menu-inner) {
    flex-direction: row !important;
    gap: 8px;
  }
  .sider-menu :deep(.arco-menu-item) {
    width: auto !important;
    flex: 1 1 0;
    margin: 0;
    text-align: center;
  }
  .sider-footer {
    display: none;
  }
  .header {
    min-height: 64px;
    height: auto;
    padding: 12px;
    gap: 10px;
    flex-wrap: wrap;
  }
  .content {
    padding: 14px 12px 28px;
  }
}
</style>

