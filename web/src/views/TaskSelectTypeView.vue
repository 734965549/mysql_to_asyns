<script setup>
import { useRouter } from "vue-router";
const router = useRouter();
function proceedToCreateTask(type) {
  router.push({ path: "/tasks/new/config", query: { type } });
}
</script>

<template>
        <div class="select-type-page">
          <div class="select-type-header">
            <a-typography-title :heading="3" style="margin-bottom: 8px"
              >选择目标端类型</a-typography-title
            >
            <a-typography-text type="secondary"
              >请选择数据同步的目标存储系统</a-typography-text
            >
          </div>

          <div class="type-cards-container">
            <a-card
              hoverable
              class="type-card"
              @click="proceedToCreateTask('MYSQL')"
            >
              <div class="type-icon mysql-icon">
                <icon-storage />
              </div>
              <div class="type-content">
                <a-typography-title :heading="5"
                  >MySQL 数据库</a-typography-title
                >
                <a-typography-text type="secondary"
                  >将数据实时同步到另一个 MySQL
                  或兼容数据库，支持全量+增量</a-typography-text
                >
              </div>
            </a-card>

            <a-card
              hoverable
              class="type-card"
              @click="proceedToCreateTask('KAFKA')"
            >
              <div class="type-icon kafka-icon">
                <icon-send />
              </div>
              <div class="type-content">
                <a-typography-title :heading="5"
                  >Kafka 消息队列</a-typography-title
                >
                <a-typography-text type="secondary"
                  >将变更流以 JSON 格式投递到 Kafka
                  Topic，适合下游流式处理</a-typography-text
                >
              </div>
            </a-card>

            <a-card
              hoverable
              class="type-card"
              @click="proceedToCreateTask('WEBHOOK')"
            >
              <div class="type-icon webhook-icon">
                <icon-link />
              </div>
              <div class="type-content">
                <a-typography-title :heading="5"
                  >HTTP Webhook</a-typography-title
                >
                <a-typography-text type="secondary"
                  >发生数据变更时主动回调指定的 HTTP
                  接口，灵活集成业务系统</a-typography-text
                >
              </div>
            </a-card>

            <a-card
              hoverable
              class="type-card"
              @click="proceedToCreateTask('MULTI')"
            >
              <div class="type-icon multi-icon">
                <icon-branch />
              </div>
              <div class="type-content">
                <a-typography-title :heading="5"
                  >高级: 多目标 (Multi-Sink)</a-typography-title
                >
                <a-typography-text type="secondary"
                  >同时将数据分发到多个不同类型的目标端</a-typography-text
                >
              </div>
            </a-card>
          </div>
        </div>
</template>

<style scoped>
.select-type-page {
  max-width: 800px;
  margin: 40px auto;
  padding: 0 24px;
}
.select-type-header {
  text-align: center;
  margin-bottom: 40px;
}
.type-cards-container {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 24px;
}
.type-card {
  border-radius: 8px;
  transition: all 0.3s ease;
  cursor: pointer;
  border: 1px solid transparent;
}
.type-card:hover {
  transform: translateY(-4px);
  box-shadow: 0 8px 24px rgba(0, 0, 0, 0.08);
  border-color: rgb(var(--primary-6));
}
.type-card :deep(.arco-card-body) {
  display: flex;
  align-items: flex-start;
  gap: 16px;
  padding: 24px;
}
.type-icon {
  width: 48px;
  height: 48px;
  border-radius: 12px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 24px;
  flex-shrink: 0;
}
.mysql-icon {
  background: #e8f3ff;
  color: rgb(var(--primary-6));
}
.kafka-icon {
  background: #fff3e8;
  color: rgb(var(--orange-6));
}
.multi-icon {
  background: #f2f3f5;
  color: var(--color-text-1);
}
.type-content h5 {
  margin: 0 0 8px 0;
  line-height: 1.4;
}
.type-content span {
  font-size: 13px;
  line-height: 1.5;
  display: block;
}
.layout-container:not(.theme-default) .select-type-header :deep(.arco-typography),
.layout-container:not(.theme-default) .type-content :deep(.arco-typography) {
  color: var(--app-text);
}
.layout-container:not(.theme-default) .select-type-header :deep(.arco-typography-secondary),
.layout-container:not(.theme-default) .type-content :deep(.arco-typography-secondary),
.layout-container:not(.theme-default) .type-content span {
  color: var(--app-muted);
}
/* Clear target-type selection and filter panel typography */
.select-type-page {
  max-width: 860px;
  margin: 56px auto 72px;
  padding: 0 28px;
}
.select-type-header {
  margin-bottom: 34px;
  padding: 18px 0 6px;
  text-align: center;
}
.select-type-header :deep(.arco-typography) {
  color: var(--color-text-1);
}
.select-type-header :deep(h1.arco-typography),
.select-type-header :deep(h2.arco-typography),
.select-type-header :deep(h3.arco-typography) {
  margin: 0 0 10px !important;
  color: var(--color-text-1);
  font-size: 30px;
  font-weight: 700;
  line-height: 1.25;
}
.select-type-header :deep(.arco-typography-secondary) {
  color: var(--color-text-3);
  font-size: 14px;
}
.type-cards-container {
  gap: 22px;
}
.type-card {
  min-height: 126px;
  border: 1px solid var(--color-border-2) !important;
  background: var(--color-bg-2) !important;
  box-shadow: 0 4px 10px rgba(0, 0, 0, 0.05);
  color: var(--color-text-1);
  overflow: hidden;
}
.type-card:hover {
  transform: translateY(-3px);
  border-color: rgb(var(--primary-6)) !important;
  background: var(--color-bg-2) !important;
  box-shadow: 0 8px 20px rgba(0, 0, 0, 0.08);
}
.type-card :deep(.arco-card-body) {
  align-items: center;
  padding: 24px;
  color: var(--color-text-1);
}
.type-content {
  min-width: 0;
}
.type-content :deep(.arco-typography),
.type-content :deep(h5.arco-typography) {
  color: var(--color-text-1);
}
.type-content :deep(h5.arco-typography) {
  margin-bottom: 8px !important;
  font-size: 20px;
  line-height: 1.35;
}
.type-content :deep(.arco-typography-secondary) {
  color: var(--color-text-3);
  font-size: 13px;
  line-height: 1.65;
}
.type-icon {
  border: 1px solid transparent;
  background: #e8f3ff !important;
  color: rgb(var(--primary-6)) !important;
}
.kafka-icon {
  background: #fff3e8 !important;
  color: rgb(var(--orange-6)) !important;
}
.webhook-icon {
  background: #e8fffb !important;
  color: rgb(var(--green-6)) !important;
}
.multi-icon {
  background: #f2f3f5 !important;
  color: var(--color-text-1) !important;
}

/* Sci-fi / Dark Theme overrides when layout is not default */
.layout-container:not(.theme-default) .select-type-header :deep(.arco-typography) {
  color: var(--app-text) !important;
}
.layout-container:not(.theme-default) .select-type-header :deep(h1.arco-typography),
.layout-container:not(.theme-default) .select-type-header :deep(h2.arco-typography),
.layout-container:not(.theme-default) .select-type-header :deep(h3.arco-typography) {
  color: #f3fbff !important;
  text-shadow: 0 0 18px rgba(32, 199, 232, 0.18);
}
.layout-container:not(.theme-default) .select-type-header :deep(.arco-typography-secondary) {
  color: #b9cbe0 !important;
}
.layout-container:not(.theme-default) .type-card {
  border: 1px solid var(--app-border) !important;
  background: linear-gradient(180deg, var(--app-surface), var(--app-surface-soft)) !important;
  box-shadow: 0 16px 34px rgba(0, 0, 0, 0.22);
  color: var(--app-text);
}
.layout-container:not(.theme-default) .type-card:hover {
  border-color: var(--app-accent) !important;
  background: linear-gradient(180deg, var(--app-surface-soft), var(--app-surface-strong)) !important;
  box-shadow: 0 18px 38px var(--app-glow);
}
.layout-container:not(.theme-default) .type-card :deep(.arco-card-body) {
  color: var(--app-text);
}
.layout-container:not(.theme-default) .type-content :deep(.arco-typography),
.layout-container:not(.theme-default) .type-content :deep(h5.arco-typography) {
  color: #f3fbff !important;
}
.layout-container:not(.theme-default) .type-content :deep(.arco-typography-secondary) {
  color: var(--app-muted) !important;
}
.layout-container:not(.theme-default) .type-icon {
  border: 1px solid var(--app-border-soft);
  background: color-mix(in srgb, var(--app-accent) 12%, transparent) !important;
  color: var(--app-accent) !important;
}
.layout-container:not(.theme-default) .kafka-icon {
  background: color-mix(in srgb, var(--app-accent-2) 12%, transparent) !important;
  color: var(--app-accent-2) !important;
}
.layout-container:not(.theme-default) .multi-icon {
  background: color-mix(in srgb, var(--app-muted) 12%, transparent) !important;
  color: var(--app-muted) !important;
}

.theme-blue .type-card,
.theme-gray .type-card,
.theme-black .type-card,
.theme-dark .type-card,
.theme-default .type-card,
.theme-default .select-type-header :deep(.arco-typography),
.theme-default .type-content :deep(.arco-typography),
.theme-default .select-type-header :deep(.arco-typography-secondary),
.theme-default .type-content :deep(.arco-typography-secondary),
.theme-default .type-icon {
  border: 0;
}
.theme-default .mysql-icon { background: #e8f3ff !important; color: rgb(var(--primary-6)) !important; }
.theme-default .kafka-icon { background: #fff3e8 !important; color: rgb(var(--orange-6)) !important; }
.theme-default .multi-icon { background: #f2f3f5 !important; color: var(--color-text-1) !important; }

</style>
