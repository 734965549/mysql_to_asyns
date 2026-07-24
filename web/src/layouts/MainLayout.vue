<script setup>
import { computed } from "vue";
import { useRoute, useRouter } from "vue-router";
import {
  formHeaderActions,
  clearFormHeaderActions,
} from "../composables/useFormHeaderActions.js";
import { useUiTheme } from "../composables/useUiTheme.js";

const route = useRoute();
const router = useRouter();
const { appThemeClass } = useUiTheme();

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
  <a-layout :class="['layout-container', appThemeClass]">
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
/* MainLayout 基础样式，其他样式在 themes.css 中 */
.layout-container {
  height: 100vh;
}

.sider {
  display: flex;
  flex-direction: column;
}

.logo {
  height: 64px;
  display: flex;
  align-items: center;
  padding: 0 20px;
  color: #fff;
}

.logo-icon {
  width: 32px;
  height: 32px;
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

.sider-menu:not(.arco-menu-collapsed) {
  width: 100% !important;
}

.sider-menu :deep(.arco-menu-inner) {
  display: flex !important;
  flex-direction: column !important;
  opacity: 1 !important;
  animation: none !important;
  transition: none !important;
}

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

.sider-menu :deep(.arco-menu-item),
.sider-menu :deep(.arco-menu-item.arco-menu-selected),
.sider-menu :deep(.arco-menu-item:hover),
.sider-menu :deep(.arco-menu-item:focus),
.sider-menu :deep(.arco-menu-item:active) {
  background: transparent !important;
  background-color: transparent !important;
}

.sider-menu :deep(.arco-menu-item .arco-menu-icon) {
  color: #fff !important;
  opacity: 1 !important;
  visibility: visible !important;
  display: inline-flex !important;
  margin-right: 12px !important;
  animation: none !important;
  transition: none !important;
}

.sider-menu :deep(.arco-menu-item .arco-menu-title) {
  color: #fff !important;
  opacity: 1 !important;
  visibility: visible !important;
  display: inline !important;
  animation: none !important;
  transition: none !important;
  transform: none !important;
}

.sider-menu :deep(.arco-menu-item *) {
  color: #fff !important;
  opacity: 1 !important;
  visibility: visible !important;
  animation: none !important;
  transition: none !important;
}

.sider-menu :deep(.arco-menu-collapse-icon) {
  display: none !important;
}

.sider-menu :deep(.arco-menu) {
  transition: none !important;
  animation: none !important;
}

.sider-menu :deep(.arco-menu-item:hover) {
  background: rgba(255, 255, 255, 0.15) !important;
  color: #fff !important;
}

.sider-menu :deep(.arco-menu-item.arco-menu-selected) {
  background: rgba(255, 255, 255, 0.25) !important;
  color: #fff !important;
}

.sider-menu :deep(.arco-menu-item::before),
.sider-menu :deep(.arco-menu-item::after) {
  animation: none !important;
  transition: none !important;
}

.sider-menu :deep(.arco-menu-item svg) {
  opacity: 1 !important;
  visibility: visible !important;
  color: #fff !important;
}

.sider-footer {
  padding: 16px 20px;
  border-top: 1px solid rgba(255, 255, 255, 0.1);
  margin-top: auto;
}

.header {
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

.header-left,
.header-right {
  display: flex;
  align-items: center;
}
</style>

