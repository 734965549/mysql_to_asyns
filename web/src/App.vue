<script setup>
import { onMounted } from "vue";
import { useUiTheme } from "./composables/useUiTheme.js";

const { appThemeClass, initUiThemeFromStorage } = useUiTheme();

onMounted(() => {
  initUiThemeFromStorage();
});
</script>

<template>
  <div class="app-root" :class="appThemeClass">
    <router-view />
  </div>
</template>

<style scoped>
.app-root {
  height: 100%;
  min-height: 100vh;
}
</style>
