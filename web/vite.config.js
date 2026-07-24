import { defineConfig } from 'vite' // 从vite包导入defineConfig函数，用于定义配置
import vue from '@vitejs/plugin-vue' // 导入Vite的Vue插件

// https://vite.dev/config/ - Vite配置文档
export default defineConfig({ // 导出默认配置
  root: '.', // 设置项目根目录为当前目录
  plugins: [vue()], // 使用Vue插件，支持.vue文件
  server: { // 开发服务器配置
    host: '0.0.0.0', // 设置服务器监听地址为所有网络接口
    port: 5173, // 设置开发服务器端口为5173
    proxy: { // 代理配置，用于API请求
      '/api': { // 匹配/api开头的请求
        target: 'http://localhost:8080', // 将请求代理到本地8080端口
        changeOrigin: true // 改变请求源头的origin，避免CORS问题
      }
    }
  },
  build: { // 构建配置
    // 提高 chunk 大小警告限制（单位：kB）
    chunkSizeWarningLimit: 1500 // 将chunk大小警告限制提高到1500KB
  }
})
