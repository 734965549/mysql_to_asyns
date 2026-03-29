<script setup> // 使用Vue 3的Composition API语法
import { ref } from 'vue' // 从vue包导入ref函数，用于创建响应式变量
import viteLogo from '../assets/vite.svg' // 导入Vite logo图片
import heroImg from '../assets/hero.png' // 导入hero图片
import vueLogo from '../assets/vue.svg' // 导入Vue logo图片

const count = ref(0) // 创建一个响应式计数器变量，初始值为0
</script>

<template> <!-- 模板部分开始 -->
  <section id="center"> <!-- 中央区域容器 -->
    <div class="hero"> <!-- Hero图片容器 -->
      <img :src="heroImg" class="base" width="170" height="179" alt="" /> <!-- Hero主图片 -->
      <img :src="vueLogo" class="framework" alt="Vue logo" /> <!-- Vue框架logo -->
      <img :src="viteLogo" class="vite" alt="Vite logo" /> <!-- Vite构建工具logo -->
    </div> <!-- Hero图片容器结束 -->
    <div> <!-- 文本容器 -->
      <h1>Get started</h1> <!-- 主标题 -->
      <p>Edit <code>src/App.vue</code> and save to test <code>HMR</code></p> <!-- 提示文本：编辑App.vue文件以测试热模块替换 -->
    </div> <!-- 文本容器结束 -->
    <button class="counter" @click="count++">Count is {{ count }}</button> <!-- 计数器按钮，点击时计数加1 -->
  </section> <!-- 中央区域结束 -->

  <div class="ticks"></div> <!-- 装饰性分割线 -->

  <section id="next-steps"> <!-- 下一步指南区域 -->
    <div id="docs"> <!-- 文档部分 -->
      <svg class="icon" role="presentation" aria-hidden="true"> <!-- 文档图标SVG -->
        <use href="/icons.svg#documentation-icon"></use> <!-- 引用图标符号 -->
      </svg> <!-- 图标结束 -->
      <h2>Documentation</h2> <!-- 文档标题 -->
      <p>Your questions, answered</p> <!-- 文档描述 -->
      <ul> <!-- 链接列表开始 -->
        <li> <!-- 列表项：Vite文档 -->
          <a href="https://vite.dev/" target="_blank"> <!-- Vite官方文档链接 -->
            <img class="logo" :src="viteLogo" alt="" /> <!-- Vite logo -->
            Explore Vite <!-- 链接文本 -->
          </a> <!-- 链接结束 -->
        </li> <!-- 列表项结束 -->
        <li> <!-- 列表项：Vue文档 -->
          <a href="https://vuejs.org/" target="_blank"> <!-- Vue官方文档链接 -->
            <img class="button-icon" :src="vueLogo" alt="" /> <!-- Vue logo -->
            Learn more <!-- 链接文本 -->
          </a> <!-- 链接结束 -->
        </li> <!-- 列表项结束 -->
      </ul> <!-- 链接列表结束 -->
    </div> <!-- 文档部分结束 -->
    <div id="social"> <!-- 社交媒体部分 -->
      <svg class="icon" role="presentation" aria-hidden="true"> <!-- 社交图标SVG -->
        <use href="/icons.svg#social-icon"></use> <!-- 引用图标符号 -->
      </svg> <!-- 图标结束 -->
      <h2>Connect with us</h2> <!-- 社交标题 -->
      <p>Join the Vite community</p> <!-- 社交描述 -->
      <ul> <!-- 社交链接列表开始 -->
        <li> <!-- 列表项：GitHub -->
          <a href="https://github.com/vitejs/vite" target="_blank"> <!-- GitHub链接 -->
            <svg class="button-icon" role="presentation" aria-hidden="true"> <!-- GitHub图标SVG -->
              <use href="/icons.svg#github-icon"></use> <!-- 引用图标符号 -->
            </svg> <!-- 图标结束 -->
            GitHub <!-- 链接文本 -->
          </a> <!-- 链接结束 -->
        </li> <!-- 列表项结束 -->
        <li> <!-- 列表项：Discord -->
          <a href="https://chat.vite.dev/" target="_blank"> <!-- Discord链接 -->
            <svg class="button-icon" role="presentation" aria-hidden="true"> <!-- Discord图标SVG -->
              <use href="/icons.svg#discord-icon"></use> <!-- 引用图标符号 -->
            </svg> <!-- 图标结束 -->
            Discord <!-- 链接文本 -->
          </a> <!-- 链接结束 -->
        </li> <!-- 列表项结束 -->
        <li> <!-- 列表项：X.com -->
          <a href="https://x.com/vite_js" target="_blank"> <!-- X.com链接 -->
            <svg class="button-icon" role="presentation" aria-hidden="true"> <!-- X.com图标SVG -->
              <use href="/icons.svg#x-icon"></use> <!-- 引用图标符号 -->
            </svg> <!-- 图标结束 -->
            X.com <!-- 链接文本 -->
          </a> <!-- 链接结束 -->
        </li> <!-- 列表项结束 -->
        <li> <!-- 列表项：Bluesky -->
          <a href="https://bsky.app/profile/vite.dev" target="_blank"> <!-- Bluesky链接 -->
            <svg class="button-icon" role="presentation" aria-hidden="true"> <!-- Bluesky图标SVG -->
              <use href="/icons.svg#bluesky-icon"></use> <!-- 引用图标符号 -->
            </svg> <!-- 图标结束 -->
            Bluesky <!-- 链接文本 -->
          </a> <!-- 链接结束 -->
        </li> <!-- 列表项结束 -->
      </ul> <!-- 社交链接列表结束 -->
    </div> <!-- 社交媒体部分结束 -->
  </section> <!-- 下一步指南结束 -->

  <div class="ticks"></div> <!-- 装饰性分割线 -->
  <section id="spacer"></section> <!-- 间距占位符 -->
</template> <!-- 模板部分结束 -->
