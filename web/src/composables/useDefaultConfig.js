import { ref } from "vue";
import { API_BASE } from "./useApi.js";

const configForm = ref({
  http: { host: "", port: 8080 },

  datasource: {
    host: "",
    port: 3306,
    database: "",
    username: "",
    password: "",
    debug: false,
  },

  target: { host: "", port: 3306, database: "", username: "", password: "" },

  redis: { host: "", port: 6379, password: "", db: 0 },

  storage: {
    mode: "file",
    data_dir: "data",
    host: "",
    port: 3306,
    database: "",
    username: "",
    password: "",
  },

  log: {
    level: "info",
    console: { enable: true, no_color: false },
    file: { enable: true },
  },
});

const configLoading = ref(false);
let defaultConfigPromise = null;

async function fetchDefaultConfig() {
  try {
    const res = await fetch(`${API_BASE}/config/default`);

    if (res.ok) {
      const data = await res.json();

      if (data.http) Object.assign(configForm.value.http, data.http);
      if (data.redis) Object.assign(configForm.value.redis, data.redis);

      if (data.log) {
        if (data.log.level) configForm.value.log.level = data.log.level;
        if (data.log.console)
          Object.assign(configForm.value.log.console, data.log.console);
        if (data.log.file)
          Object.assign(configForm.value.log.file, data.log.file);
      }

      if (data.datasource)
        Object.assign(configForm.value.datasource, data.datasource);
      if (data.target) Object.assign(configForm.value.target, data.target);
      if (data.storage) Object.assign(configForm.value.storage, data.storage);
    }
  } catch (e) {
    console.error("获取默认配置失败:", e);
  }
}

export function useDefaultConfig() {
  function ensureDefaultConfig() {
    if (!defaultConfigPromise) {
      defaultConfigPromise = fetchDefaultConfig().finally(() => {
        // allow refresh via explicit fetchDefaultConfig()
      });
    }
    return defaultConfigPromise;
  }

  return {
    configForm,
    configLoading,
    fetchDefaultConfig,
    ensureDefaultConfig,
  };
}
