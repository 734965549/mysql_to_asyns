import { createApp } from 'vue' // 从vue包导入createApp函数，用于创建Vue应用实例
import ArcoVue from '@arco-design/web-vue' // 导入Arco Design Vue组件库
import '@arco-design/web-vue/dist/arco.css' // 导入Arco Design的CSS样式文件
import App from './App.vue' // 导入根组件App.vue
import './style.css' // 导入全局样式文件

const app = createApp(App) // 创建Vue应用实例
app.use(ArcoVue) // 使用Arco Design Vue组件库
app.mount('#app') // 将应用挂载到DOM元素#app上
