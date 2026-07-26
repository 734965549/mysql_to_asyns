#!/usr/bin/env node
/**
 * Generate TaskFormView.vue from App.vue by keeping form logic + form template.
 * Run: node web/scripts/gen-form.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const WEB = path.resolve(__dirname, "..");
const SRC = path.join(WEB, "src");
const EXT = path.join(SRC, "_extract");

const app = fs.readFileSync(path.join(SRC, "App.vue"), "utf8");
const lines = app.split(/\r?\n/);

// script: lines 2 .. </script> exclusive (inside script setup)
const scriptEnd = lines.findIndex((l) => l.trim() === "</script>");
let script = lines.slice(1, scriptEnd).join("\n"); // content inside <script setup>

// Remove detail-drawer-only state usage is fine to leave unused refs
// Replace imports at top
script = script.replace(
  /import \{ ref, onMounted, onUnmounted, watch, computed \} from "vue";/,
  `import { ref, onMounted, onUnmounted, watch, computed } from "vue";
import { useRoute, useRouter } from "vue-router";
import { API_BASE, handleApiError } from "../composables/useApi.js";
import { useDefaultConfig } from "../composables/useDefaultConfig.js";
import {
  setFormHeaderActions,
  clearFormHeaderActions,
} from "../composables/useFormHeaderActions.js";`,
);

script = script.replace(
  /import \{\s*buildTargetDatabasesPayload as buildDatabaseMappingsPayload,\s*getTaskDatabaseMappings,\s*\} from "\.\/utils\/databaseMappings\.js";/,
  `import {
  buildTargetDatabasesPayload as buildDatabaseMappingsPayload,
  getTaskDatabaseMappings,
} from "../utils/databaseMappings.js";`,
);

script = script.replace(
  /import \{\s*hasExplicitSinkConfigs,\s*isMaskedSecret,\s*isSingleExplicitMySQLSink,\s*resolveMySQLSinkConnectionDisplay,\s*resolveTaskTargetMySQLDisplay,\s*unmaskSecret,\s*\} from "\.\/utils\/taskTargetDisplay\.js";/,
  `import {
  hasExplicitSinkConfigs,
  isMaskedSecret,
  isSingleExplicitMySQLSink,
  resolveMySQLSinkConnectionDisplay,
  resolveTaskTargetMySQLDisplay,
  unmaskSecret,
} from "../utils/taskTargetDisplay.js";`,
);

// Remove local API_BASE and handleApiError definitions
script = script.replace(/const API_BASE = "\/api";\s*/, "");
script = script.replace(
  /\/\/ 统一错误处理函数\s*\n\s*async function handleApiError\(response, defaultMsg = "操作失败"\) \{[\s\S]*?\n\}\s*/,
  "",
);

// Inject router + default config after imports block — append near top after last import
script = script.replace(
  /(from "\.\.\/utils\/taskTargetDisplay\.js";\s*)/,
  `$1
const route = useRoute();
const router = useRouter();
const { configForm, fetchDefaultConfig, ensureDefaultConfig } = useDefaultConfig();
const isEditMode = computed(() => route.meta?.mode === "edit" || route.name === "task-edit");
`,
);

// Remove local configForm definition and fetchDefaultConfig/saveConfig/applyLog — keep saveSource which uses config
// Actually form uses configForm from useDefaultConfig now — remove local const configForm = ref({...}) through fetchDefaultConfig function
script = script.replace(
  /\/\/ 系统配置状态\s*\n\s*const configForm = ref\(\{[\s\S]*?\}\);\s*\n\s*const configLoading = ref\(false\);\s*\n\s*\/\/ 获取默认配置\s*\n\s*async function fetchDefaultConfig\(\) \{[\s\S]*?\n\}\s*\n\s*\/\/ 保存系统配置\s*\n\s*async function saveConfig\(\) \{[\s\S]*?\n\}\s*\n\s*const logApplying = ref\(false\);\s*\n\s*async function applyLogConfig\(\) \{[\s\S]*?\n\}\s*/,
  "",
);

// Remove theme stuff
script = script.replace(
  /const UI_THEME_STORAGE_KEY = "mysql_to_async_ui_theme";\s*\n\s*const uiThemeOptions = \[[\s\S]*?\];\s*\n\s*const uiTheme = ref\("default"\);\s*\n\s*const appThemeClass = computed\(\(\) => \`theme-\$\{uiTheme\.value\}\`\);\s*\n\s*function setUiTheme\(theme\) \{[\s\S]*?\n\}\s*\n\s*function syncUiThemeToDocument\(theme\) \{[\s\S]*?\n\}\s*/,
  "",
);
script = script.replace(
  /watch\(uiTheme, \(theme\) => syncUiThemeToDocument\(theme\), \{ immediate: true \}\);\s*/,
  "",
);

// Replace navigation helpers
script = script.replace(
  /function closeTaskForm\(\) \{[\s\S]*?\n\}/,
  `function closeTaskForm() {
  resetForm();
  clearFormHeaderActions();
  router.push("/tasks");
}

function goSelectType() {
  router.push("/tasks/new/select");
}`,
);

script = script.replace(
  /function openCreateDialog\(\) \{[\s\S]*?\n\}/,
  `function openCreateDialog() {
  resetForm();
  router.push("/tasks/new/select");
}`,
);

script = script.replace(
  /function proceedToCreateTask\(type\) \{[\s\S]*?\n\}/,
  `function proceedToCreateTask(type) {
  targetType.value = type;
}`,
);

script = script.replace(
  /window\.history\.pushState\(\{\s*taskForm: "edit"\s*\},\s*"",\s*\`#\/tasks\/\$\{editingTaskId\.value\}\/edit\`,\s*\);/,
  `router.push(\`/tasks/\${editingTaskId.value}/edit\`);`,
);

script = script.replace(
  /window\.history\.pushState\(\{\s*taskForm: "create"\s*\},\s*"",\s*"#\/tasks\/new"\);/,
  `router.push("/tasks/new");`,
);

script = script.replace(
  /window\.history\.pushState\(\{\s*taskForm: "create"\s*\},\s*"",\s*"#\/tasks\/new\/config"\);/,
  `router.push({ path: "/tasks/new/config", query: { type: targetType.value } });`,
);

script = script.replace(/window\.history\.pushState\(\{\}, "", "#\/tasks"\);/g, `router.push("/tasks");`);
script = script.replace(
  /window\.history\.pushState\(\{\}, "", "#\/" \+ key\);/g,
  `router.push("/" + key);`,
);

// After successful create/update, ensure router.push tasks — look for closeTaskForm calls which already push

// Replace onMounted / onUnmounted / handlePopState
script = script.replace(
  /function handlePopState\(\) \{[\s\S]*?\n\}\s*\n\s*let refreshInterval;\s*\n\s*onMounted\(async \(\) => \{[\s\S]*?\n\}\);\s*\n\s*onUnmounted\(\(\) => \{[\s\S]*?\n\}\);/,
  `onMounted(async () => {
  await ensureDefaultConfig();
  await fetchDatabases();

  // edit mode: load task
  if (isEditMode.value && route.params.id) {
    editMode.value = true;
    editingTaskId.value = String(route.params.id);
    try {
      const res = await fetch(\`\${API_BASE}/tasks/\${editingTaskId.value}\`);
      if (res.ok) {
        const task = await res.json();
        fillTaskFormFromTask(task);
      } else {
        Message.error("加载任务失败");
        router.push("/tasks");
        return;
      }
    } catch (e) {
      Message.error("加载任务失败: " + e.message);
      router.push("/tasks");
      return;
    }
  } else {
    editMode.value = false;
    editingTaskId.value = null;
    const type = String(route.query.type || "").toUpperCase();
    if (["MYSQL", "KAFKA", "WEBHOOK", "MULTI"].includes(type)) {
      targetType.value = type === "WEBHOOK" ? "WEBHOOK" : type;
      if (type === "WEBHOOK") targetType.value = "WEBHOOK";
    }
    const cloneFrom = route.query.clone_from;
    if (cloneFrom) {
      try {
        const res = await fetch(\`\${API_BASE}/tasks/\${cloneFrom}\`);
        if (res.ok) {
          const task = await res.json();
          openDuplicateFromTask(task);
        }
      } catch (e) {
        console.error(e);
      }
    }
  }

  setFormHeaderActions({
    loading,
    editMode,
    submit: createTask,
    cancel: closeTaskForm,
  });
});

onUnmounted(() => {
  clearFormHeaderActions();
});`,
);

// Fix createTask success path to use router — closeTaskForm already does
// Also patch editMode references: keep editMode ref in sync with isEditMode for template
// Template will use editMode still from original — keep it

// Remove list-only watchers that call fetchTasks on filter — keep form watches
// Remove syncTaskFilters / loadTaskFilters if they remain — OK if unused

// Fix openDuplicateFromTask to not navigate (already on create page when called from onMounted)
script = script.replace(
  /function openDuplicateFromTask\(task\) \{[\s\S]*?Message\.success\("已载入该任务配置，请检查后点击「创建」"\);\n\}/,
  `function openDuplicateFromTask(task) {
  resetForm();
  editMode.value = false;
  editingTaskId.value = null;
  cloneFromTaskId.value = task.config.id;
  fillTaskFormFromTask(task);
  const base = (task.config.name || "同步任务").trim();
  const suffix = "（副本）";
  taskForm.value.name = base.endsWith(suffix)
    ? \`\${base}_\${Date.now()}\`
    : \`\${base}\${suffix}\`;
  Message.success("已载入该任务配置，请检查后点击「创建」");
}`,
);

// Template
let formTpl = readTpl();
function readTpl() {
  let t = fs.readFileSync(path.join(EXT, "tpl-form.html"), "utf8");
  t = t.replace(
    /\s+v-if="taskFormPage === 'create' \|\| taskFormPage === 'edit'"/g,
    "",
  );
  t = t.replace(/taskFormPage === 'edit'/g, "editMode");
  t = t.replace(/taskFormPage === \"edit\"/g, "editMode");
  t = t.replace(
    /@click="taskFormPage = 'select_type'"/g,
    `@click="goSelectType"`,
  );
  return t;
}

const style = fs.readFileSync(path.join(EXT, "style-form.css"), "utf8");

const out = `<script setup>
${script}
</script>

<template>
${formTpl}
</template>

<style scoped>
${style}
</style>
`;

fs.writeFileSync(path.join(SRC, "views/TaskFormView.vue"), out.replace(/\r\n/g, "\n"), "utf8");
console.log("OK TaskFormView.vue", out.split("\n").length);
