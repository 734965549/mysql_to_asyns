<script setup>
import { ref, onMounted } from "vue";
import { Message } from "@arco-design/web-vue";
import { API_BASE } from "../composables/useApi.js";
import { useDefaultConfig } from "../composables/useDefaultConfig.js";
import { useUiTheme } from "../composables/useUiTheme.js";

const { configForm, configLoading, fetchDefaultConfig, ensureDefaultConfig } =
  useDefaultConfig();
const { uiTheme, uiThemeOptions, setUiTheme } = useUiTheme();
const logApplying = ref(false);

async function saveConfig() {
  configLoading.value = true;
  try {
    const res = await fetch(`${API_BASE}/config/update`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(configForm.value),
    });
    if (res.ok) {
      Message.success("系统配置已更新，配置文件已同步");
      await fetchDefaultConfig();
    } else {
      const text = await res.text();
      try {
        const err = JSON.parse(text);
        Message.error("更新配置失败: " + err.error);
      } catch {
        Message.error("更新配置失败: " + text);
      }
    }
  } catch (e) {
    Message.error("更新配置失败: " + e.message);
  } finally {
    configLoading.value = false;
  }
}

async function applyLogConfig() {
  logApplying.value = true;
  try {
    const res = await fetch(`${API_BASE}/config/log`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        level: configForm.value.log.level,
        console: configForm.value.log.console,
        file: configForm.value.log.file,
      }),
    });
    if (res.ok) {
      const data = await res.json();
      Message.success(
        `日志配置已热加载生效 — 级别: ${data.level?.toUpperCase()}, 控制台: ${data.console ? "开" : "关"}, 文件: ${data.file ? "开" : "关"}`,
      );
    } else {
      const text = await res.text();
      try {
        const err = JSON.parse(text);
        Message.error("日志热加载失败: " + err.error);
      } catch {
        Message.error("日志热加载失败: " + text);
      }
    }
  } catch (e) {
    Message.error("日志热加载失败: " + e.message);
  } finally {
    logApplying.value = false;
  }
}

onMounted(() => {
  ensureDefaultConfig();
});
</script>

<template>
        <div class="config-page-shell">
          <div class="config-hero">
            <div>
              <a-typography-title :heading="4" style="margin: 0 0 8px 0">系统配置</a-typography-title>
              <a-typography-text type="secondary">统一管理服务监听、日志、默认数据库与任务持久化配置。</a-typography-text>
            </div>
            <a-tag color="arcoblue" bordered>配置文件：etc/application.toml</a-tag>
          </div>

          <a-card class="config-section-card theme-config-card" :bordered="false">
            <template #title>界面主题</template>
            <div class="theme-option-grid">
              <button
                v-for="theme in uiThemeOptions"
                :key="theme.value"
                type="button"
                class="theme-option"
                :class="[`theme-option--${theme.value}`, { 'is-active': uiTheme === theme.value }]"
                @click="setUiTheme(theme.value)"
              >
                <span class="theme-option__swatch">
                  <span></span>
                  <span></span>
                  <span></span>
                </span>
                <span class="theme-option__content">
                  <span class="theme-option__title">{{ theme.label }}</span>
                  <span class="theme-option__desc">{{ theme.desc }}</span>
                </span>
                <span v-if="uiTheme === theme.value" class="theme-option__checked">已启用</span>
              </button>
            </div>
            <div class="config-hint">主题仅保存在当前浏览器本地，不会改写服务端配置文件。</div>
          </a-card>

          <a-row :gutter="16" class="config-summary-row">
            <a-col :span="8"><a-card class="config-summary-card" :bordered="false"><a-statistic title="HTTP 端口" :value="configForm.http.port" /></a-card></a-col>
            <a-col :span="8"><a-card class="config-summary-card" :bordered="false"><a-statistic title="Redis DB" :value="configForm.redis.db" /></a-card></a-col>
            <a-col :span="8"><a-card class="config-summary-card" :bordered="false"><a-statistic title="日志级别"><template #value>{{ configForm.log.level?.toUpperCase() || '-' }}</template></a-statistic></a-card></a-col>
          </a-row>

          <a-card class="config-page-card" :bordered="false">
            <a-form :model="configForm" layout="vertical" @submit="saveConfig">
              <a-row :gutter="20">
                <a-col :span="12">
                  <a-card class="config-section-card" :bordered="false">
                    <template #title>基础连接</template>

                    <a-form-item label="HTTP 监听地址">
                      <a-input v-model="configForm.http.host" placeholder="0.0.0.0" />
                    </a-form-item>

                    <a-form-item label="HTTP 监听端口">
                      <a-input-number v-model="configForm.http.port" :min="1" :max="65535" style="width: 100%" />
                    </a-form-item>

                    <a-divider orientation="left" class="config-section-divider">Redis 状态持久化</a-divider>

                    <a-form-item label="Redis 主机">
                      <a-input v-model="configForm.redis.host" placeholder="127.0.0.1" />
                    </a-form-item>

                    <a-row :gutter="12">
                      <a-col :span="12">
                        <a-form-item label="Redis 端口">
                          <a-input-number v-model="configForm.redis.port" :min="1" :max="65535" style="width: 100%" />
                        </a-form-item>
                      </a-col>
                      <a-col :span="12">
                        <a-form-item label="数据库索引 (DB)">
                          <a-input-number v-model="configForm.redis.db" :min="0" :max="15" style="width: 100%" />
                        </a-form-item>
                      </a-col>
                    </a-row>

                    <a-form-item label="Redis 密码">
                      <a-input-password v-model="configForm.redis.password" placeholder="留空表示无密码" />
                    </a-form-item>
                  </a-card>
                </a-col>

                <a-col :span="12">
                  <a-card class="config-section-card" :bordered="false">
                    <template #title>
                      <span>日志与默认环境</span>
                      <a-tag color="green" size="small" style="margin-left: 8px">热加载</a-tag>
                    </template>

                    <a-form-item label="日志级别">
                      <a-select v-model="configForm.log.level">
                        <a-option value="debug">Debug</a-option>
                        <a-option value="info">Info</a-option>
                        <a-option value="warn">Warn</a-option>
                        <a-option value="error">Error</a-option>
                      </a-select>
                    </a-form-item>

                    <a-form-item label="输出开关">
                      <a-space direction="vertical" style="width: 100%">
                        <a-checkbox v-model="configForm.log.console.enable">开启控制台标准输出 (Stdout)</a-checkbox>
                        <a-checkbox v-model="configForm.log.file.enable">开启文件持久化输出 (File)</a-checkbox>
                      </a-space>
                    </a-form-item>

                    <a-form-item>
                      <a-button type="primary" status="success" :loading="logApplying" @click="applyLogConfig" style="width: 100%">
                        <template #icon><icon-sync /></template>
                        立即应用日志配置（无需重启）
                      </a-button>
                      <div class="config-hint">修改日志级别或输出开关后点击此按钮，配置即刻生效并持久化到配置文件。</div>
                    </a-form-item>

                    <a-divider orientation="left" class="config-section-divider">默认数据库环境</a-divider>

                    <a-form-item label="默认源库地址">
                      <a-input v-model="configForm.datasource.host" />
                    </a-form-item>

                    <a-form-item label="默认目标库地址">
                      <a-input v-model="configForm.target.host" />
                    </a-form-item>

                    <a-form-item label="调试模式 (Debug)">
                      <a-switch v-model="configForm.datasource.debug" />
                    </a-form-item>
                  </a-card>
                </a-col>
              </a-row>

              <a-card class="config-section-card config-storage-card" :bordered="false">
                <template #title>
                  <span>任务数据持久化</span>
                </template>

                <a-form-item label="持久化模式">
                  <a-radio-group v-model="configForm.storage.mode" type="button">
                    <a-radio value="file">本地文件</a-radio>
                    <a-radio value="mysql">MySQL 数据库</a-radio>
                  </a-radio-group>
                </a-form-item>

                <template v-if="configForm.storage.mode === 'file'">
                  <a-form-item label="数据目录">
                    <a-input v-model="configForm.storage.data_dir" placeholder="data" />
                  </a-form-item>
                </template>

                <template v-if="configForm.storage.mode === 'mysql'">
                  <a-row :gutter="16">
                    <a-col :span="8">
                      <a-form-item label="MySQL 主机">
                        <a-input v-model="configForm.storage.host" placeholder="127.0.0.1" />
                      </a-form-item>
                    </a-col>
                    <a-col :span="4">
                      <a-form-item label="端口">
                        <a-input-number v-model="configForm.storage.port" :min="1" :max="65535" style="width: 100%" />
                      </a-form-item>
                    </a-col>
                    <a-col :span="6">
                      <a-form-item label="数据库">
                        <a-input v-model="configForm.storage.database" />
                      </a-form-item>
                    </a-col>
                    <a-col :span="6">
                      <a-form-item label="用户名">
                        <a-input v-model="configForm.storage.username" />
                      </a-form-item>
                    </a-col>
                  </a-row>

                  <a-form-item label="密码">
                    <a-input-password v-model="configForm.storage.password" />
                  </a-form-item>
                </template>
              </a-card>

              <div class="config-actions-bar">
                <a-button type="primary" size="large" :loading="configLoading" @click="saveConfig">
                  保存并同步到 application.toml
                </a-button>
                <a-typography-text type="secondary">
                  <icon-info-circle /> 修改配置后将直接改写服务器磁盘文件，部分底层服务（如端口监听）需重启 Go 程序生效。
                </a-typography-text>
              </div>
            </a-form>
          </a-card>
        </div>
</template>

<style scoped>
/* Theme selector in system config */
.config-page-shell {
  display: flex;
  flex-direction: column;
  gap: 16px;
}
.config-hero {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  flex-wrap: wrap;
}
.config-hint {
  margin-top: 10px;
  font-size: 12px;
  line-height: 1.6;
  color: var(--app-muted, #86909c);
}
.config-summary-row {
  margin-bottom: 4px;
}
.config-page-card :deep(.arco-card-body) {
  padding: 20px 24px 24px;
}
.config-section-card :deep(.arco-card-header) {
  padding: 16px 20px 12px;
  border-bottom: 1px solid var(--app-border-soft, #edf2f7);
}
.config-section-card :deep(.arco-card-header-title) {
  color: var(--app-text, #1d2129);
  font-weight: 600;
  font-size: 15px;
  line-height: 22px;
}
.config-section-card :deep(.arco-card-body) {
  padding: 16px 20px 20px;
}
.config-section-divider {
  margin: 22px 0 16px !important;
  border-color: var(--app-border-soft, #e5e8ef) !important;
}
.config-section-divider :deep(.arco-divider-text) {
  padding: 0 12px 0 0;
  font-size: 13px;
  font-weight: 600;
  line-height: 20px;
  color: var(--app-text, #1d2129);
  background: var(--app-surface-soft, #fbfcff);
}
.theme-config-card {
  margin-bottom: 16px;
}
.theme-option-grid {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  gap: 12px;
}
.theme-option {
  min-height: 86px;
  padding: 12px;
  border: 1px solid rgba(120, 144, 166, 0.24);
  border-radius: 8px;
  background: rgba(255, 255, 255, 0.04);
  color: inherit;
  cursor: pointer;
  display: flex;
  flex-direction: column;
  gap: 8px;
  text-align: left;
  transition: border-color 0.16s ease, box-shadow 0.16s ease, transform 0.16s ease;
}
.theme-option:hover,
.theme-option.is-active {
  border-color: var(--app-accent, #165dff);
  box-shadow: 0 0 0 3px rgba(32, 199, 232, 0.12);
  transform: translateY(-1px);
}
.theme-option__swatch {
  display: grid;
  grid-template-columns: 1.2fr 1fr 1fr;
  gap: 4px;
  height: 18px;
}
.theme-option__swatch span {
  border-radius: 4px;
}
.theme-option__content {
  display: flex;
  flex-direction: column;
  gap: 2px;
}
.theme-option__title {
  font-weight: 600;
  font-size: 13px;
}
.theme-option__desc,
.theme-option__checked {
  color: var(--app-muted, #86909c);
  font-size: 12px;
  line-height: 18px;
}
.theme-option__checked {
  color: var(--app-accent, #165dff);
}
.theme-option--default .theme-option__swatch span:nth-child(1) { background: #ffffff; border: 1px solid #e5e6eb; }
.theme-option--default .theme-option__swatch span:nth-child(2) { background: #f5f7fa; }
.theme-option--default .theme-option__swatch span:nth-child(3) { background: #165dff; }
.theme-option--blue .theme-option__swatch span:nth-child(1) { background: #07111f; }
.theme-option--blue .theme-option__swatch span:nth-child(2) { background: #12233a; }
.theme-option--blue .theme-option__swatch span:nth-child(3) { background: #20c7e8; }
.theme-option--gray .theme-option__swatch span:nth-child(1) { background: #14181f; }
.theme-option--gray .theme-option__swatch span:nth-child(2) { background: #252c36; }
.theme-option--gray .theme-option__swatch span:nth-child(3) { background: #94a3b8; }
.theme-option--black .theme-option__swatch span:nth-child(1) { background: #000000; }
.theme-option--black .theme-option__swatch span:nth-child(2) { background: #0a0a0a; }
.theme-option--black .theme-option__swatch span:nth-child(3) { background: #38bdf8; }
.theme-option--dark .theme-option__swatch span:nth-child(1) { background: #111827; }
.theme-option--dark .theme-option__swatch span:nth-child(2) { background: #1f2937; }
.theme-option--dark .theme-option__swatch span:nth-child(3) { background: #60a5fa; }

@media (max-width: 920px) {
  .theme-option-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

</style>
