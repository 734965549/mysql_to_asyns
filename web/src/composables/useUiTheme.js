import { ref, computed, watch } from "vue";

export const UI_THEME_STORAGE_KEY = "mysql_to_async_ui_theme";

export const uiThemeOptions = [
  { value: "default", label: "默认白色", desc: "接近最初的浅色后台风格" },
  { value: "blue", label: "深蓝科技", desc: "当前青蓝深色主题" },
  { value: "gray", label: "高级灰", desc: "低饱和灰蓝工作台" },
  { value: "black", label: "纯黑", desc: "高对比黑色主题" },
  { value: "dark", label: "暗色", desc: "柔和暗色主题" },
];

const uiTheme = ref("default");
const appThemeClass = computed(() => `theme-${uiTheme.value}`);
let themeWatchStarted = false;

function syncUiThemeToDocument(theme) {
  if (typeof document !== "undefined") {
    document.documentElement.dataset.uiTheme = theme;
  }
}

function ensureThemeWatch() {
  if (themeWatchStarted) return;
  themeWatchStarted = true;
  watch(uiTheme, (theme) => syncUiThemeToDocument(theme), { immediate: true });
}

export function useUiTheme() {
  ensureThemeWatch();

  function setUiTheme(theme) {
    uiTheme.value = theme;
    localStorage.setItem(UI_THEME_STORAGE_KEY, theme);
  }

  function initUiThemeFromStorage() {
    const savedTheme = localStorage.getItem(UI_THEME_STORAGE_KEY);
    if (uiThemeOptions.some((option) => option.value === savedTheme)) {
      uiTheme.value = savedTheme;
    }
    syncUiThemeToDocument(uiTheme.value);
  }

  return {
    UI_THEME_STORAGE_KEY,
    uiThemeOptions,
    uiTheme,
    appThemeClass,
    setUiTheme,
    syncUiThemeToDocument,
    initUiThemeFromStorage,
  };
}
