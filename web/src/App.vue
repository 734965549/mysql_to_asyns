<script setup>
import { ref, onMounted, onUnmounted, watch, computed } from 'vue'
import { 
  Message, 
  Modal
} from '@arco-design/web-vue'

const API_BASE = '/api'

// 统一错误处理函数
async function handleApiError(response, defaultMsg = '操作失败') {
  try {
    const errData = await response.json()
    if (errData.error) {
      // 解析错误信息
      const errorMsg = errData.error
      // 如果是详细错误信息，显示完整信息
      if (errorMsg.includes(':')) {
        return `${defaultMsg}: ${errorMsg}`
      }
      return `${defaultMsg}: ${errorMsg}`
    }
    return defaultMsg
  } catch (e) {
    return defaultMsg
  }
}

// 导航状态
const selectedKey = ref(['tasks'])

// 状态
const tasks = ref([])
const databases = ref([])
const tables = ref([])
const loading = ref(false)
const taskFormPage = ref('none') // 'none' | 'create' | 'edit'

// 搜索框状态
const databaseSearchText = ref('')
const tableSearchText = ref('')

const selectedSyncLevel = ref('database')
const selectedDatabases = ref([])        // 库级别同步时选中的源数据库列表
const targetDatabaseMappings = ref([])   // [{source, target}] 源->目标库映射
const selectedTables = ref([])
const editMode = ref(false)
const editingTaskId = ref(null)

// 任务详情抽屉
const detailDrawerVisible = ref(false)
const selectedTaskForDetail = ref(null)

// 自定义数据库配置开关
const useCustomSourceDB = ref(false)
const useCustomTargetDB = ref(false)

// 自定义数据库配置
const customSourceDB = ref({
  host: '',
  port: 3306,
  database: '',
  username: '',
  password: ''
})

const customTargetDB = ref({
  host: '',
  port: 3306,
  database: '',
  username: '',
  password: ''
})

const taskForm = ref({
  name: '',
  source_schema: '',
  target_schema: '',
  tables: [],
  mode: 'FULL',
  batch_size: 1000,
  worker_count: 4,
  enable_limit_one: false,
  optimize_index: false
})

// 刷新状态
const refreshingDatabases = ref(false)
const refreshingTables = ref(false)

// 刷新数据库列表
async function refreshDatabases() {
  refreshingDatabases.value = true
  try {
    await fetch(`${API_BASE}/metadata/refresh`, { method: 'POST' })
    const res = await fetch(`${API_BASE}/metadata/databases`)
    if (res.ok) {
      databases.value = await res.json()
    }
  } catch (e) {
    Message.error('刷新数据库列表失败')
    console.error('刷新数据库列表失败:', e)
  } finally {
    refreshingDatabases.value = false
  }
}

// 获取数据库列表
async function fetchDatabases() {
  try {
    // 确定使用哪个数据库配置
    let dbConfig = null
    
    if (useCustomSourceDB.value) {
      // 开启自定义源数据库：使用自定义配置
      if (customSourceDB.value.host) {
        dbConfig = {
          host: customSourceDB.value.host,
          port: customSourceDB.value.port,
          username: customSourceDB.value.username,
          password: customSourceDB.value.password,
          database: customSourceDB.value.database || 'mysql'
        }
      }
    } else {
      // 未开启自定义源数据库：使用配置文件中的源数据库配置
      if (configForm.value.datasource && configForm.value.datasource.host) {
        dbConfig = {
          host: configForm.value.datasource.host,
          port: configForm.value.datasource.port,
          username: configForm.value.datasource.username,
          password: configForm.value.datasource.password,
          database: configForm.value.datasource.database || 'mysql'
        }
      }
    }
    
    // 首先尝试使用默认连接（后端可能在启动时已经根据配置文件建立了连接）
    const defaultRes = await fetch(`${API_BASE}/metadata/databases`)
    if (defaultRes.ok) {
      databases.value = await defaultRes.json()
      return
    }
    
    // 如果默认连接失败，且有自定义配置，尝试使用自定义配置连接
    if (dbConfig && dbConfig.host) {
      const res = await fetch(`${API_BASE}/metadata/databases-with-config`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(dbConfig)
      })
      if (res.ok) {
        databases.value = await res.json()
        return
      } else {
        const errData = await res.json()
        console.error('获取数据库列表失败:', errData.error)
        Message.warning('获取数据库列表失败: ' + errData.error)
      }
    } else {
      // 没有配置，提示用户
      const errData = await defaultRes.json()
      console.error('获取数据库列表失败:', errData.error)
      Message.info('请先在系统配置中配置源数据库连接信息，或在高级配置中指定自定义数据库连接')
    }
  } catch (e) {
    console.error('获取数据库列表失败:', e)
    Message.error('获取数据库列表失败: ' + e.message)
  }
}

// 测试数据库连接
async function testConnection(dbConfig, type) {
  try {
    const res = await fetch(`${API_BASE}/config/test-connection`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(dbConfig)
    })
    const data = await res.json()
    if (data.success) {
      Message.success(`${type}连接成功: ${data.message}`)
    } else {
      Message.error(`${type}连接失败: ${data.message}`)
    }
    return data.success
  } catch (e) {
    Message.error(`${type}连接测试失败: ${e.message}`)
    return false
  }
}

// 测试源数据库连接
async function testSourceConnection() {
  return await testConnection(customSourceDB.value, '源数据库')
}

// 测试目标数据库连接
async function testTargetConnection() {
  return await testConnection(customTargetDB.value, '目标数据库')
}

// 保存源数据库配置到配置文件
async function saveSourceConfig() {
  if (!customSourceDB.value.host) {
    Message.warning('请先填写源数据库配置')
    return
  }
  configForm.value.datasource = {
    ...configForm.value.datasource,
    host: customSourceDB.value.host,
    port: customSourceDB.value.port,
    database: customSourceDB.value.database,
    username: customSourceDB.value.username,
    password: customSourceDB.value.password
  }
  await saveConfig()
  // 重新获取数据库列表
  fetchDatabases()
}

// 保存目标数据库配置到配置文件
async function saveTargetConfig() {
  if (!customTargetDB.value.host) {
    Message.warning('请先填写目标数据库配置')
    return
  }
  configForm.value.target = {
    ...configForm.value.target,
    host: customTargetDB.value.host,
    port: customTargetDB.value.port,
    database: customTargetDB.value.database,
    username: customTargetDB.value.username,
    password: customTargetDB.value.password
  }
  await saveConfig()
}

// 刷新表列表
async function refreshTables() {
  if (!taskForm.value.source_schema) {
    Message.warning('请先选择源数据库')
    return
  }
  refreshingTables.value = true
  try {
    await fetch(`${API_BASE}/metadata/refresh`, { method: 'POST' })
    const res = await fetch(`${API_BASE}/metadata/tables?schema=${taskForm.value.source_schema}`)
    if (res.ok) {
      tables.value = await res.json()
    }
  } catch (e) {
    Message.error('刷新表列表失败')
    console.error('刷新表列表失败:', e)
  } finally {
    refreshingTables.value = false
  }
}

// 获取表列表
async function fetchTables() {
  if (!taskForm.value.source_schema) {
    return
  }
  loading.value = true
  try {
    // 确定使用哪个数据库配置
    let dbConfig = null
    
    if (useCustomSourceDB.value) {
      // 开启自定义源数据库：使用自定义配置
      if (customSourceDB.value.host) {
        dbConfig = {
          host: customSourceDB.value.host,
          port: customSourceDB.value.port,
          username: customSourceDB.value.username,
          password: customSourceDB.value.password,
          database: customSourceDB.value.database || taskForm.value.source_schema
        }
      }
    } else {
      // 未开启自定义源数据库：使用配置文件中的源数据库配置
      if (configForm.value.datasource && configForm.value.datasource.host) {
        dbConfig = {
          host: configForm.value.datasource.host,
          port: configForm.value.datasource.port,
          username: configForm.value.datasource.username,
          password: configForm.value.datasource.password,
          database: configForm.value.datasource.database || taskForm.value.source_schema
        }
      }
    }
    
    let res
    if (dbConfig && dbConfig.host) {
      // 使用自定义配置获取表列表
      res = await fetch(`${API_BASE}/metadata/tables-with-config`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ...dbConfig,
          schema: taskForm.value.source_schema
        })
      })
    } else {
      // 使用默认连接（后端启动时建立的连接）
      res = await fetch(`${API_BASE}/metadata/tables?schema=${taskForm.value.source_schema}`)
    }
    
    if (res.ok) {
      tables.value = await res.json()
    } else {
      // 解析错误信息并显示给用户
      const errText = await res.text()
      try {
        const errData = JSON.parse(errText)
        Message.error(`获取表列表失败: ${errData.error || errText}`)
      } catch {
        Message.error(`获取表列表失败: ${errText}`)
      }
    }
  } catch (e) {
    console.error('获取表列表失败:', e)
    Message.error(`获取表列表失败: ${e.message}`)
  } finally {
    loading.value = false
  }
}

// 获取任务列表
async function fetchTasks() {
  try {
    const res = await fetch(`${API_BASE}/tasks`)
    if (res.ok) {
      tasks.value = await res.json()
    }
  } catch (e) {
    console.error('获取任务列表失败:', e)
  }
}

// 关闭任务表单页
function closeTaskForm() {
  taskFormPage.value = 'none'
  resetForm()
  window.history.pushState({}, '', '#/tasks')
}

// 打开创建页
function openCreateDialog() {
  resetForm()
  taskFormPage.value = 'create'
  window.history.pushState({ taskForm: 'create' }, '', '#/tasks/new')
}

// 重置表单
function resetForm() {
  taskForm.value = {
    name: '',
    source_schema: '',
    target_schema: '',
    tables: [],
    mode: 'FULL',
    batch_size: 1000,
    worker_count: 4,
    enable_limit_one: false,
    optimize_index: false
  }
  selectedSyncLevel.value = 'database'
  selectedDatabases.value = []
  targetDatabaseMappings.value = []
  selectedTables.value = []
  editMode.value = false
  editingTaskId.value = null
  useCustomSourceDB.value = false
  useCustomTargetDB.value = false
  customSourceDB.value = { host: '', port: 3306, database: '', username: '', password: '' }
  customTargetDB.value = { host: '', port: 3306, database: '', username: '', password: '' }
}

// 全选/取消全选表
function toggleAllTables() {
  if (selectedTables.value.length === tables.value.length) {
    selectedTables.value = []
  } else {
    selectedTables.value = tables.value.map(t => t.table_name)
  }
}

// 创建任务
async function createTask() {
  if (!taskForm.value.name) {
    Message.warning('请输入任务名称')
    return
  }
  
  if (selectedSyncLevel.value === 'database') {
    if (selectedDatabases.value.length === 0) {
      Message.warning('请至少选择一个源数据库')
      return
    }
  } else {
    if (!taskForm.value.source_schema) {
      Message.warning('请选择源数据库')
      return
    }
    if (selectedTables.value.length === 0) {
      Message.warning('请至少选择一个表')
      return
    }
  }

  // 构建 payload
  let tablesPayload = []
  let sourceDatabasesPayload = []
  let targetDatabasesPayload = []
  let sourceSchemaPayload = taskForm.value.source_schema
  let targetSchemaPayload = taskForm.value.target_schema

  if (selectedSyncLevel.value === 'database') {
    // 库级别：使用多选的库列表
    sourceDatabasesPayload = selectedDatabases.value
    targetDatabasesPayload = targetDatabaseMappings.value.map(m => m.target)
    // source_schema / target_schema 留空（由 source_databases 驱动）
    sourceSchemaPayload = ''
    targetSchemaPayload = ''
  } else {
    tablesPayload = selectedTables.value
  }

  const payload = {
    ...taskForm.value,
    source_schema: sourceSchemaPayload,
    target_schema: targetSchemaPayload,
    sync_level: selectedSyncLevel.value,
    tables: tablesPayload,
    source_databases: sourceDatabasesPayload,
    target_databases: targetDatabasesPayload,
    source_db: useCustomSourceDB.value ? customSourceDB.value : null,
    target_db: useCustomTargetDB.value ? customTargetDB.value : null
  }

  loading.value = true
  try {
    const url = editMode.value 
      ? `${API_BASE}/tasks/${editingTaskId.value}` 
      : `${API_BASE}/tasks`
    const method = editMode.value ? 'PUT' : 'POST'
    
    const res = await fetch(url, {
      method,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    })
    
    if (res.ok) {
      closeTaskForm()
      fetchTasks()
      Message.success(editMode.value ? '更新成功' : '创建成功')
    } else {
      // 尝试解析错误信息
      try {
        const text = await res.text()
        if (text) {
          try {
            const err = JSON.parse(text)
            Message.error((editMode.value ? '更新' : '创建') + '失败: ' + (err.error || text))
          } catch {
            // 不是JSON，直接显示文本
            Message.error((editMode.value ? '更新' : '创建') + '失败: ' + text)
          }
        } else {
          Message.error((editMode.value ? '更新' : '创建') + '失败: 服务器返回空响应')
        }
      } catch (e) {
        Message.error((editMode.value ? '更新' : '创建') + '失败: ' + e.message)
      }
    }
  } catch (e) {
    Message.error((editMode.value ? '更新' : '创建') + '失败: ' + e.message)
  } finally {
    loading.value = false
  }
}

// 启动任务
async function startTask(taskId) {
  try {
    const res = await fetch(`${API_BASE}/tasks/${taskId}/start`, { method: 'POST' })
    if (res.ok) {
      fetchTasks()
      Message.success('任务已启动')
    } else {
      const errorMsg = await handleApiError(res, '启动失败')
      Message.error(errorMsg)
      // 刷新任务列表以获取最新状态
      fetchTasks()
    }
  } catch (e) {
    Message.error('启动失败: ' + e.message)
  }
}

// 暂停任务
async function pauseTask(taskId) {
  try {
    const res = await fetch(`${API_BASE}/tasks/${taskId}/pause`, { method: 'POST' })
    if (res.ok) {
      fetchTasks()
      Message.success('任务已暂停')
    } else {
      const errorMsg = await handleApiError(res, '暂停失败')
      Message.error(errorMsg)
    }
  } catch (e) {
    Message.error('暂停失败: ' + e.message)
  }
}

// 删除任务
async function deleteTask(taskId) {
  Modal.confirm({
    title: '确认删除',
    content: '确定要删除这个任务吗？',
    okText: '删除',
    cancelText: '取消',
    onOk: async () => {
      try {
        const res = await fetch(`${API_BASE}/tasks/${taskId}`, { method: 'DELETE' })
        if (res.ok) {
          fetchTasks()
          Message.success('删除成功')
        } else {
          const errorMsg = await handleApiError(res, '删除失败')
          Message.error(errorMsg)
        }
      } catch (e) {
        Message.error('删除失败: ' + e.message)
      }
    }
  })
}

// 显示任务详情
function showTaskDetail(task) {
  selectedTaskForDetail.value = task
  detailDrawerVisible.value = true
}

// 打开编辑对话框
function openEditDialog(task) {
  resetForm()
  editMode.value = true
  editingTaskId.value = task.config.id
  
  // 填充表单数据
  taskForm.value = {
    name: task.config.name,
    source_schema: task.config.source_schema,
    target_schema: task.config.target_schema,
    tables: task.config.tables || [],
    mode: task.config.mode,
    batch_size: task.config.batch_size,
    worker_count: task.config.worker_count,
    enable_limit_one: task.config.enable_limit_one,
    optimize_index: task.config.optimize_index || false
  }
  
  // 设置同步级别
  if (task.config.sync_level === 'DATABASE') {
    selectedSyncLevel.value = 'database'
    // 恢复多库选择状态
    const srcDbs = task.config.source_databases || []
    const dstDbs = task.config.target_databases || []
    selectedDatabases.value = srcDbs
    targetDatabaseMappings.value = srcDbs.map((db, i) => ({
      source: db,
      target: dstDbs[i] || db
    }))
  } else {
    selectedSyncLevel.value = 'table'
    // 加载表列表并设置选中的表
    fetchTables().then(() => {
      selectedTables.value = task.config.tables || []
    })
  }
  
  // 设置自定义数据库配置
  if (task.config.source_db) {
    useCustomSourceDB.value = true
    customSourceDB.value = {
      host: task.config.source_db.host,
      port: task.config.source_db.port,
      database: task.config.source_db.database,
      username: task.config.source_db.username,
      password: task.config.source_db.password
    }
  }
  
  if (task.config.target_db) {
    useCustomTargetDB.value = true
    customTargetDB.value = {
      host: task.config.target_db.host,
      port: task.config.target_db.port,
      database: task.config.target_db.database,
      username: task.config.target_db.username,
      password: task.config.target_db.password
    }
  }
  
  taskFormPage.value = 'edit'
  window.history.pushState({ taskForm: 'edit' }, '', `#/tasks/${editingTaskId.value}/edit`)
}

// 格式化时间
function formatTime(time) {
  if (!time) return '-'
  return new Date(time).toLocaleString('zh-CN')
}

// 计算运行时长
function calculateDuration(startTime, endTime) {
  if (!startTime) return '-'
  const start = new Date(startTime)
  const end = endTime ? new Date(endTime) : new Date()
  const diff = Math.floor((end - start) / 1000)
  
  if (diff < 60) return `${diff}秒`
  if (diff < 3600) return `${Math.floor(diff / 60)}分${diff % 60}秒`
  const hours = Math.floor(diff / 3600)
  const minutes = Math.floor((diff % 3600) / 60)
  return `${hours}小时${minutes}分`
}

// 获取状态颜色
function getStatusColor(status) {
  const colors = {
    'PENDING': 'gray',
    'RUNNING': 'blue',
    'PAUSED': 'orange',
    'COMPLETED': 'green',
    'FAILED': 'red'
  }
  return colors[status] || 'gray'
}

// 获取状态文本
function getStatusText(status) {
  const texts = {
    'PENDING': '待执行',
    'RUNNING': '执行中',
    'PAUSED': '已暂停',
    'COMPLETED': '已完成',
    'FAILED': '失败'
  }
  return texts[status] || status
}

// 计算进度
function getProgress(task) {
  if (!task.context.total_rows) return 0
  return Math.round((task.context.processed_rows / task.context.total_rows) * 100)
}

// 监听同步级别变化
function onSyncLevelChange() {
  selectedDatabases.value = []
  targetDatabaseMappings.value = []
  selectedTables.value = []
  // 注意：不在这里调用 fetchTables()，因为此时 source_schema 可能还没有值
  // 表列表会在用户选择源数据库后通过 onSourceSchemaChange() 加载
}

// 监听源数据库变化
function onSourceSchemaChange() {
  selectedTables.value = []
  if (selectedSyncLevel.value === 'table') {
    fetchTables()
  }
}

// 系统配置状态
const configForm = ref({
  http: { host: '', port: 8080 },
  datasource: { host: '', port: 3306, database: '', username: '', password: '', debug: false },
  target: { host: '', port: 3306, database: '', username: '', password: '' },
  redis: { host: '', port: 6379, password: '', db: 0 },
  storage: { mode: 'file', data_dir: 'data', host: '', port: 3306, database: '', username: '', password: '' },
  log: { level: 'info', console: { enable: true, no_color: false }, file: { enable: true } }
})
const configLoading = ref(false)

// 获取默认配置
async function fetchDefaultConfig() {
  try {
    const res = await fetch(`${API_BASE}/config/default`)
    if (res.ok) {
      const data = await res.json()
      // 深度合并配置，确保响应性且不丢失结构
      if (data.http) Object.assign(configForm.value.http, data.http)
      if (data.redis) Object.assign(configForm.value.redis, data.redis)
      if (data.log) {
        if (data.log.level) configForm.value.log.level = data.log.level
        if (data.log.console) Object.assign(configForm.value.log.console, data.log.console)
        if (data.log.file) Object.assign(configForm.value.log.file, data.log.file)
      }
      if (data.datasource) Object.assign(configForm.value.datasource, data.datasource)
      if (data.target) Object.assign(configForm.value.target, data.target)
      if (data.storage) Object.assign(configForm.value.storage, data.storage)
    }
  } catch (e) {
    console.error('获取默认配置失败:', e)
  }
}

// 保存系统配置
async function saveConfig() {
  configLoading.value = true
  try {
    const res = await fetch(`${API_BASE}/config/update`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(configForm.value)
    })
    if (res.ok) {
      Message.success('系统配置已更新，配置文件已同步')
      // 重新获取最新配置以同步页面数据
      await fetchDefaultConfig()
      // 刷新元数据，因为数据库连接可能变了
      await refreshDatabases()
    } else {
      const err = await res.json()
      Message.error('更新配置失败: ' + err.error)
    }
  } catch (e) {
    Message.error('更新配置失败: ' + e.message)
  } finally {
    configLoading.value = false
  }
}

// 处理浏览器返回按钒
function handlePopState() {
  if (taskFormPage.value !== 'none') {
    taskFormPage.value = 'none'
    resetForm()
  }
}

let refreshInterval
onMounted(async () => {
  window.addEventListener('popstate', handlePopState)
  await fetchDefaultConfig()
  fetchDatabases()
  fetchTasks()
  refreshInterval = setInterval(fetchTasks, 3000)
})

onUnmounted(() => {
  window.removeEventListener('popstate', handlePopState)
  if (refreshInterval) clearInterval(refreshInterval)
})

// 菜单点击处理
function onMenuClick(key) {
  console.log('Menu item clicked:', key)
  const prevKey = selectedKey.value[0]
  selectedKey.value = [key]
  
  if (key === 'tasks') {
    fetchTasks()
  } else if (key === 'config') {
    // 只有在从非配置页面切换过来时才获取配置，避免在配置页面内操作时被覆盖
    if (prevKey !== 'config') {
      fetchDefaultConfig()
    }
  }
}

// 使用计算属性来确保页面正确渲染
const currentPage = computed(() => selectedKey.value[0])

// 计算属性：对任务列表进行稳定排序（避免列表闪烁）
const sortedTasks = computed(() => {
  return [...tasks.value].sort((a, b) => {
    // 按任务ID排序（ID包含时间戳，可以保持稳定）
    return a.config.id.localeCompare(b.config.id)
  })
})

// 计算属性：过滤后的数据库列表
const filteredDatabases = computed(() => {
  if (!databaseSearchText.value) {
    return databases.value
  }
  const searchText = databaseSearchText.value.toLowerCase()
  return databases.value.filter(db => 
    db.toLowerCase().includes(searchText)
  )
})

// 计算属性：过滤后的表列表
const filteredTables = computed(() => {
  if (!tableSearchText.value) {
    return tables.value
  }
  const searchText = tableSearchText.value.toLowerCase()
  return tables.value.filter(table => 
    table.table_name.toLowerCase().includes(searchText)
  )
})

// 监听自定义源数据库开关变化，自动刷新数据库列表
watch(useCustomSourceDB, (newVal) => {
  fetchDatabases()
})

// 监听选中的源数据库变化，自动同步目标数据库映射
watch(selectedDatabases, (newDbs) => {
  const newMappings = newDbs.map(db => {
    const existing = targetDatabaseMappings.value.find(m => m.source === db)
    return existing || { source: db, target: db }
  })
  targetDatabaseMappings.value = newMappings
}, { deep: true })
</script>

<template>
  <a-layout class="layout-container">
    <!-- 左侧导航栏 -->
    <a-layout-sider 
      :width="220" 
      :collapsible="false"
      class="sider"
    >
      <div class="logo">
        <div class="logo-icon">
          <icon-storage />
        </div>
        <span class="logo-text">MySQL 数据同步</span>
      </div>
      
      <a-menu
        v-model:selected-keys="selectedKey"
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
    
    <!-- 主内容区 -->
    <a-layout>
      <a-layout-header class="header">
        <div class="header-left">
          <a-button 
            v-if="taskFormPage !== 'none'" 
            type="text" 
            style="margin-right: 8px"
            @click="closeTaskForm"
          >
            <template #icon><icon-arrow-left /></template>
            返回
          </a-button>
          <a-typography-title :heading="5" style="margin: 0">
            {{ taskFormPage !== 'none' ? (editMode ? '编辑任务' : '创建同步任务') : (selectedKey[0] === 'tasks' ? '任务管理' : '系统配置') }}
          </a-typography-title>
        </div>
        <div class="header-right" v-if="selectedKey[0] === 'tasks' && taskFormPage === 'none'">
          <a-button type="primary" @click="openCreateDialog">
            <template #icon><icon-plus /></template>
            创建同步任务
          </a-button>
        </div>
        <div class="header-right" v-if="taskFormPage !== 'none'">
          <a-space>
            <a-button @click="closeTaskForm">取消</a-button>
            <a-button type="primary" :loading="loading" @click="createTask">
              {{ editMode ? '更新' : '创建' }}
            </a-button>
          </a-space>
        </div>
      </a-layout-header>
      
      <a-layout-content class="content">
        <!-- 任务表单全屏页 -->
        <div v-if="taskFormPage !== 'none'" class="task-form-full-page">
          <a-form :model="taskForm" layout="vertical">
            <a-row :gutter="32">
              <a-col :span="12">
                <a-form-item label="任务名称" required>
                  <a-input v-model="taskForm.name" placeholder="请输入任务名称" />
                </a-form-item>

                <a-form-item label="同步级别">
                  <a-radio-group v-model="selectedSyncLevel" @change="onSyncLevelChange">
                    <a-radio value="database">
                      <a-space><icon-storage />库级别同步（全库）</a-space>
                    </a-radio>
                    <a-radio value="table">
                      <a-space><icon-file />表级别同步（指定表）</a-space>
                    </a-radio>
                  </a-radio-group>
                </a-form-item>

                <!-- 库级别：多选源数据库 -->
                <a-form-item v-if="selectedSyncLevel === 'database'" label="源数据库（可多选）" required>
                  <div style="display: flex; align-items: flex-start; gap: 8px">
                    <div style="flex: 1">
                      <!-- 搜索框 -->
                      <a-input-search
                        v-model="databaseSearchText"
                        placeholder="搜索数据库名..."
                        style="margin-bottom: 8px"
                        allow-clear
                      />
                      <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px">
                        <a-button type="text" size="small" @click="selectedDatabases = selectedDatabases.length === filteredDatabases.length ? [] : [...filteredDatabases]">
                          {{ selectedDatabases.length === filteredDatabases.length && filteredDatabases.length > 0 ? '取消全选' : '全选' }}
                        </a-button>
                        <a-typography-text type="secondary" style="font-size: 12px">已选 {{ selectedDatabases.length }} / {{ filteredDatabases.length }}</a-typography-text>
                      </div>
                      <div style="max-height: 180px; overflow-y: auto; border: 1px solid #e5e6eb; border-radius: 4px; padding: 8px; background: #fafafa">
                        <a-checkbox-group v-model="selectedDatabases" style="width: 100%">
                          <a-row :gutter="[8, 8]">
                            <a-col :span="8" v-for="db in filteredDatabases" :key="db">
                              <a-checkbox :value="db">{{ db }}</a-checkbox>
                            </a-col>
                          </a-row>
                        </a-checkbox-group>
                        <a-empty v-if="filteredDatabases.length === 0" description="暂无匹配的数据库" :style="{ padding: '8px 0' }" />
                      </div>
                    </div>
                    <a-button type="text" size="small" :loading="refreshingDatabases" @click="refreshDatabases">
                      <template #icon><icon-refresh /></template>
                    </a-button>
                  </div>
                </a-form-item>

                <!-- 表级别：单选源数据库 -->
                <a-form-item v-if="selectedSyncLevel === 'table'" label="源数据库" required>
                  <a-space>
                    <a-select v-model="taskForm.source_schema" placeholder="请选择源数据库" style="width: 260px" @change="onSourceSchemaChange">
                      <a-option value="">请选择源数据库</a-option>
                      <a-option v-for="db in databases" :key="db" :value="db">{{ db }}</a-option>
                    </a-select>
                    <a-button type="text" size="small" :loading="refreshingDatabases" @click="refreshDatabases">
                      <template #icon><icon-refresh /></template>
                    </a-button>
                  </a-space>
                </a-form-item>

                <!-- 库级别：目标数据库映射表 -->
                <a-form-item v-if="selectedSyncLevel === 'database'" label="目标数据库映射">
                  <div v-if="targetDatabaseMappings.length > 0">
                    <div v-for="(mapping, i) in targetDatabaseMappings" :key="mapping.source"
                      style="display: flex; align-items: center; gap: 8px; margin-bottom: 8px">
                      <a-tag color="arcoblue" style="min-width: 130px; text-align: center">{{ mapping.source }}</a-tag>
                      <icon-arrow-right style="color: #86909c" />
                      <a-input v-model="targetDatabaseMappings[i].target" style="width: 200px" placeholder="目标库名" />
                    </div>
                  </div>
                  <a-typography-text v-else type="secondary">请先选择源数据库</a-typography-text>
                </a-form-item>

                <!-- 表级别：单个目标库输入 + 同步模式 -->
                <a-row :gutter="16">
                  <a-col v-if="selectedSyncLevel === 'table'" :span="12">
                    <a-form-item label="目标数据库" required>
                      <a-input v-model="taskForm.target_schema" placeholder="请输入目标数据库名" />
                    </a-form-item>
                  </a-col>
                  <a-col :span="selectedSyncLevel === 'table' ? 12 : 24">
                    <a-form-item label="同步模式">
                      <a-select v-model="taskForm.mode">
                        <a-option value="FULL">全量同步</a-option>
                        <a-option value="INCREMENTAL">增量同步</a-option>
                        <a-option value="ALL">全量+增量</a-option>
                      </a-select>
                    </a-form-item>
                  </a-col>
                </a-row>
              </a-col>

              <a-col :span="12">
                <!-- 表级别：选择表 -->
                <a-form-item v-if="selectedSyncLevel === 'table'" label="选择要同步的表">
                  <template #extra>
                    <a-space>
                      <a-button type="text" size="small" @click="toggleAllTables">
                        {{ selectedTables.length === filteredTables.length ? '取消全选' : '全选' }}
                      </a-button>
                      <a-button type="text" size="small" :loading="refreshingTables" @click="refreshTables">
                        <template #icon><icon-refresh /></template>刷新
                      </a-button>
                    </a-space>
                  </template>
                  <!-- 表搜索框 -->
                  <a-input-search
                    v-model="tableSearchText"
                    placeholder="搜索表名..."
                    style="margin-bottom: 8px"
                    allow-clear
                  />
                  <div style="max-height: 300px; overflow-y: auto; border: 1px solid #e5e6eb; border-radius: 4px; padding: 8px; background: #fafafa">
                    <a-checkbox-group v-model="selectedTables" v-if="filteredTables.length > 0">
                      <a-row :gutter="[8, 8]">
                        <a-col :span="12" v-for="table in filteredTables" :key="table.table_name">
                          <a-checkbox :value="table.table_name">
                            {{ table.table_name }}
                            <a-tag size="small" color="gray">{{ table.table_row_count }} 行</a-tag>
                          </a-checkbox>
                        </a-col>
                      </a-row>
                    </a-checkbox-group>
                    <a-empty v-else description="暂无匹配的表" :style="{ padding: '20px 0' }" />
                  </div>
                  <div style="margin-top: 8px">
                    <a-typography-text type="secondary">已选择 {{ selectedTables.length }} / {{ filteredTables.length }} 个表</a-typography-text>
                  </div>
                </a-form-item>

                <!-- 高级配置 -->
                <a-typography-title :heading="6" style="margin-bottom: 12px">高级配置</a-typography-title>

                <a-row :gutter="16">
                  <a-col :span="12">
                    <a-form-item label="批量大小">
                      <a-input-number v-model="taskForm.batch_size" :min="1" style="width: 100%" />
                    </a-form-item>
                  </a-col>
                  <a-col :span="12">
                    <a-form-item label="表并发数">
                      <a-input-number v-model="taskForm.worker_count" :min="1" :max="32" style="width: 100%" />
                    </a-form-item>
                  </a-col>
                </a-row>

                <a-form-item>
                  <a-checkbox v-model="taskForm.optimize_index">
                    <a-space direction="vertical" :size="4">
                      <span style="font-weight: 500">启用索引优化</span>
                      <a-typography-text type="secondary" style="font-size: 12px">
                        同步前删除非主键索引以提高写入性能，同步完成后自动重建
                      </a-typography-text>
                    </a-space>
                  </a-checkbox>
                </a-form-item>

                <!-- 自定义数据库连接 -->
                <a-collapse :default-active-key="[]">
                  <a-collapse-item key="source" header="自定义源数据库连接">
                    <template #extra><a-switch v-model="useCustomSourceDB" size="small" @click.stop /></template>
                    <div v-if="useCustomSourceDB">
                      <a-row :gutter="16">
                        <a-col :span="12"><a-form-item label="主机"><a-input v-model="customSourceDB.host" placeholder="如: 192.168.1.100" /></a-form-item></a-col>
                        <a-col :span="12"><a-form-item label="端口"><a-input-number v-model="customSourceDB.port" :min="1" :max="65535" /></a-form-item></a-col>
                      </a-row>
                      <a-row :gutter="16">
                        <a-col :span="12"><a-form-item label="数据库"><a-input v-model="customSourceDB.database" /></a-form-item></a-col>
                        <a-col :span="12"><a-form-item label="用户名"><a-input v-model="customSourceDB.username" /></a-form-item></a-col>
                      </a-row>
                      <a-row :gutter="16">
                        <a-col :span="12"><a-form-item label="密码"><a-input-password v-model="customSourceDB.password" /></a-form-item></a-col>
                        <a-col :span="12">
                          <a-form-item label="操作">
                            <a-space>
                              <a-button type="outline" size="small" @click="testSourceConnection"><template #icon><icon-check /></template>测试连接</a-button>
                              <a-button type="primary" size="small" @click="saveSourceConfig"><template #icon><icon-save /></template>保存配置</a-button>
                            </a-space>
                          </a-form-item>
                        </a-col>
                      </a-row>
                    </div>
                  </a-collapse-item>
                  <a-collapse-item key="target" header="自定义目标数据库连接">
                    <template #extra><a-switch v-model="useCustomTargetDB" size="small" @click.stop /></template>
                    <div v-if="useCustomTargetDB">
                      <a-row :gutter="16">
                        <a-col :span="12"><a-form-item label="主机"><a-input v-model="customTargetDB.host" placeholder="如: 192.168.1.101" /></a-form-item></a-col>
                        <a-col :span="12"><a-form-item label="端口"><a-input-number v-model="customTargetDB.port" :min="1" :max="65535" /></a-form-item></a-col>
                      </a-row>
                      <a-row :gutter="16">
                        <a-col :span="12"><a-form-item label="数据库"><a-input v-model="customTargetDB.database" /></a-form-item></a-col>
                        <a-col :span="12"><a-form-item label="用户名"><a-input v-model="customTargetDB.username" /></a-form-item></a-col>
                      </a-row>
                      <a-row :gutter="16">
                        <a-col :span="12"><a-form-item label="密码"><a-input-password v-model="customTargetDB.password" /></a-form-item></a-col>
                        <a-col :span="12">
                          <a-form-item label="操作">
                            <a-space>
                              <a-button type="outline" size="small" @click="testTargetConnection"><template #icon><icon-check /></template>测试连接</a-button>
                              <a-button type="primary" size="small" @click="saveTargetConfig"><template #icon><icon-save /></template>保存配置</a-button>
                            </a-space>
                          </a-form-item>
                        </a-col>
                      </a-row>
                    </div>
                  </a-collapse-item>
                </a-collapse>
              </a-col>
            </a-row>
          </a-form>
        </div>

        <!-- 任务管理页面 -->
        <div v-show="taskFormPage === 'none' && currentPage === 'tasks'">
          <!-- 统计卡片 -->
          <a-row :gutter="16" class="stat-cards">
            <a-col :span="6">
              <a-card class="stat-card">
                <a-statistic title="总任务数" :value="tasks.length">
                  <template #prefix>
                    <icon-branch class="stat-icon blue" />
                  </template>
                </a-statistic>
              </a-card>
            </a-col>
            <a-col :span="6">
              <a-card class="stat-card">
                <a-statistic title="执行中" :value="tasks.filter(t => t.context.status === 'RUNNING').length">
                  <template #prefix>
                    <icon-play-arrow class="stat-icon green" />
                  </template>
                </a-statistic>
              </a-card>
            </a-col>
            <a-col :span="6">
              <a-card class="stat-card">
                <a-statistic title="已完成" :value="tasks.filter(t => t.context.status === 'COMPLETED').length">
                  <template #prefix>
                    <icon-check class="stat-icon blue" />
                  </template>
                </a-statistic>
              </a-card>
            </a-col>
            <a-col :span="6">
              <a-card class="stat-card">
                <a-statistic title="失败" :value="tasks.filter(t => t.context.status === 'FAILED').length">
                  <template #prefix>
                    <icon-close class="stat-icon red" />
                  </template>
                </a-statistic>
              </a-card>
            </a-col>
          </a-row>
          
          <!-- 任务列表 -->
          <a-card class="task-list-card">
            <template #title>
              <span>任务列表</span>
            </template>
            
            <div v-if="sortedTasks.length === 0" class="empty-state">
              <a-empty description="暂无同步任务">
                <a-button type="primary" @click="openCreateDialog">创建任务</a-button>
              </a-empty>
            </div>
            
            <a-list v-else :bordered="false">
              <a-list-item v-for="task in sortedTasks" :key="task.config.id" class="task-item">
                <a-card :bordered="false" class="task-card-inner">
                  <div class="task-header">
                    <div class="task-title">
                      <a-typography-title :heading="6" style="margin: 0">
                        {{ task.config.name }}
                      </a-typography-title>
                      <a-tag :color="getStatusColor(task.context.status)" size="small">
                        {{ getStatusText(task.context.status) }}
                      </a-tag>
                    </div>
                  </div>
                  
                  <a-descriptions :column="4" size="small" class="task-desc">
                    <a-descriptions-item label="同步级别">
                      {{ task.config.sync_level === 'DATABASE' ? '库级别' : '表级别' }}
                    </a-descriptions-item>
                    <a-descriptions-item label="源库">
                      <template v-if="task.config.sync_level === 'DATABASE' && task.config.source_databases?.length">
                        <a-tag v-for="db in task.config.source_databases" :key="db" size="small" color="arcoblue" style="margin-right: 4px">{{ db }}</a-tag>
                      </template>
                      <template v-else>{{ task.config.source_schema || '-' }}</template>
                    </a-descriptions-item>
                    <a-descriptions-item label="目标库">
                      <template v-if="task.config.sync_level === 'DATABASE' && task.config.target_databases?.length">
                        <a-tag v-for="db in task.config.target_databases" :key="db" size="small" color="green" style="margin-right: 4px">{{ db }}</a-tag>
                      </template>
                      <template v-else>{{ task.config.target_schema || '-' }}</template>
                    </a-descriptions-item>
                    <a-descriptions-item label="表数量">
                      {{ task.config.sync_level === 'DATABASE' ? '全库' : (task.config.tables?.length || 0) }}
                    </a-descriptions-item>
                  </a-descriptions>
                  
                  <!-- 进度条 -->
                  <div v-if="task.context.status === 'RUNNING'" class="task-progress">
                    <a-progress 
                      :percent="getProgress(task)" 
                      :stroke-width="8"
                      status="normal"
                      style="flex: 1"
                    />
                    <span class="progress-text">
                      {{ task.context.processed_rows || 0 }} / {{ task.context.total_rows || 0 }}
                    </span>
                  </div>
                  
                  <!-- 操作按钮 -->
                  <div class="task-actions">
                    <a-button 
                      size="small"
                      @click="showTaskDetail(task)"
                    >
                      <template #icon><icon-eye /></template>
                      详情
                    </a-button>
                    <a-button 
                      v-if="task.context.status === 'PENDING' || task.context.status === 'PAUSED'" 
                      size="small"
                      @click="openEditDialog(task)"
                    >
                      <template #icon><icon-edit /></template>
                      编辑
                    </a-button>
                    <a-button 
                      v-if="task.context.status === 'PENDING' || task.context.status === 'PAUSED'" 
                      type="primary"
                      size="small"
                      status="success"
                      @click="startTask(task.config.id)"
                    >
                      <template #icon><icon-play-arrow /></template>
                      启动
                    </a-button>
                    <a-button 
                      v-if="task.context.status === 'RUNNING'" 
                      size="small"
                      status="warning"
                      @click="pauseTask(task.config.id)"
                    >
                      <template #icon><icon-pause /></template>
                      暂停
                    </a-button>
                    <a-button 
                      v-if="task.context.status !== 'RUNNING'" 
                      size="small"
                      status="danger"
                      @click="deleteTask(task.config.id)"
                    >
                      <template #icon><icon-delete /></template>
                      删除
                    </a-button>
                  </div>
                </a-card>
              </a-list-item>
            </a-list>
          </a-card>
        </div>

        <!-- 系统配置页面 -->
        <div v-show="taskFormPage === 'none' && currentPage === 'config'">
          <a-card title="系统配置 (etc/application.toml)">
            <a-form :model="configForm" layout="vertical" @submit="saveConfig">
              <a-row :gutter="32">
                <!-- 基础配置 -->
                <a-col :span="12">
                  <a-typography-title :heading="6">HTTP 服务配置</a-typography-title>
                  <a-form-item label="监听地址">
                    <a-input v-model="configForm.http.host" placeholder="0.0.0.0" />
                  </a-form-item>
                  <a-form-item label="监听端口">
                    <a-input-number v-model="configForm.http.port" :min="1" :max="65535" />
                  </a-form-item>

                  <a-typography-title :heading="6" style="margin-top: 20px">Redis 状态持久化配置</a-typography-title>
                  <a-form-item label="主机">
                    <a-input v-model="configForm.redis.host" placeholder="127.0.0.1" />
                  </a-form-item>
                  <a-form-item label="端口">
                    <a-input-number v-model="configForm.redis.port" :min="1" :max="65535" />
                  </a-form-item>
                  <a-form-item label="密码">
                    <a-input-password v-model="configForm.redis.password" placeholder="留空表示无密码" />
                  </a-form-item>
                  <a-form-item label="数据库索引 (DB)">
                    <a-input-number v-model="configForm.redis.db" :min="0" :max="15" />
                  </a-form-item>
                </a-col>

                <!-- 日志配置 -->
                <a-col :span="12">
                  <a-typography-title :heading="6">日志配置</a-typography-title>
                  <a-form-item label="日志级别">
                    <a-select v-model="configForm.log.level">
                      <a-option value="debug">Debug</a-option>
                      <a-option value="info">Info</a-option>
                      <a-option value="warn">Warn</a-option>
                      <a-option value="error">Error</a-option>
                    </a-select>
                  </a-form-item>
                  
                  <a-form-item label="输出开关">
                    <a-space direction="vertical">
                      <a-checkbox v-model="configForm.log.console.enable">
                        开启控制台标准输出 (Stdout)
                      </a-checkbox>
                      <a-checkbox v-model="configForm.log.file.enable">
                        开启文件持久化输出 (File)
                      </a-checkbox>
                    </a-space>
                  </a-form-item>

                  <a-typography-title :heading="6" style="margin-top: 20px">默认数据库环境 (用于元数据浏览)</a-typography-title>
                  <a-form-item label="默认源库地址">
                    <a-input v-model="configForm.datasource.host" />
                  </a-form-item>
                  <a-form-item label="默认目标库地址">
                    <a-input v-model="configForm.target.host" />
                  </a-form-item>
                  <a-form-item label="调试模式 (Debug)">
                    <a-switch v-model="configForm.datasource.debug" />
                  </a-form-item>

                  <a-typography-title :heading="6" style="margin-top: 20px">任务数据持久化配置</a-typography-title>
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
                    <a-form-item label="MySQL 主机">
                      <a-input v-model="configForm.storage.host" placeholder="127.0.0.1" />
                    </a-form-item>
                    <a-row :gutter="16">
                      <a-col :span="12">
                        <a-form-item label="端口">
                          <a-input-number v-model="configForm.storage.port" :min="1" :max="65535" />
                        </a-form-item>
                      </a-col>
                      <a-col :span="12">
                        <a-form-item label="数据库">
                          <a-input v-model="configForm.storage.database" />
                        </a-form-item>
                      </a-col>
                    </a-row>
                    <a-form-item label="用户名">
                      <a-input v-model="configForm.storage.username" />
                    </a-form-item>
                    <a-form-item label="密码">
                      <a-input-password v-model="configForm.storage.password" />
                    </a-form-item>
                  </template>
                </a-col>
              </a-row>

              <div style="margin-top: 30px; text-align: center; border-top: 1px solid #f0f0f0; padding-top: 20px">
                <a-button type="primary" size="large" :loading="configLoading" @click="saveConfig">
                  保存并同步到 application.toml
                </a-button>
                <div style="margin-top: 12px">
                  <a-typography-text type="secondary">
                    <icon-info-circle /> 注意：修改配置后将直接改写服务器磁盘文件，部分底层服务（如端口监听）需重启 Go 程序生效。
                  </a-typography-text>
                </div>
              </div>
            </a-form>
          </a-card>
        </div>
      </a-layout-content>
    </a-layout>
    
    
    <!-- 任务详情抽屉 -->
    <a-drawer
      v-model:visible="detailDrawerVisible"
      title="任务详情"
      :width="600"
      :footer="false"
    >
      <div v-if="selectedTaskForDetail" class="task-detail">
        <!-- 基本信息 -->
        <a-descriptions title="基本信息" :column="2" bordered>
          <a-descriptions-item label="任务ID">
            {{ selectedTaskForDetail.config.id }}
          </a-descriptions-item>
          <a-descriptions-item label="任务名称">
            {{ selectedTaskForDetail.config.name }}
          </a-descriptions-item>
          <a-descriptions-item label="同步级别">
            {{ selectedTaskForDetail.config.sync_level === 'DATABASE' ? '库级别' : '表级别' }}
          </a-descriptions-item>
          <a-descriptions-item label="同步模式">
            <a-tag v-if="selectedTaskForDetail.config.mode === 'FULL'" color="blue">全量同步</a-tag>
            <a-tag v-else-if="selectedTaskForDetail.config.mode === 'INCREMENTAL'" color="green">增量同步</a-tag>
            <a-tag v-else color="purple">全量+增量</a-tag>
          </a-descriptions-item>
          <a-descriptions-item label="源数据库">
            {{ selectedTaskForDetail.config.source_schema }}
          </a-descriptions-item>
          <a-descriptions-item label="目标数据库">
            {{ selectedTaskForDetail.config.target_schema }}
          </a-descriptions-item>
          <a-descriptions-item label="批量大小">
            {{ selectedTaskForDetail.config.batch_size }}
          </a-descriptions-item>
          <a-descriptions-item label="工作线程">
            {{ selectedTaskForDetail.config.worker_count }}
          </a-descriptions-item>
        </a-descriptions>
        
        <!-- 执行状态 -->
        <a-descriptions title="执行状态" :column="2" bordered style="margin-top: 20px">
          <a-descriptions-item label="状态">
            <a-tag :color="getStatusColor(selectedTaskForDetail.context.status)">
              {{ getStatusText(selectedTaskForDetail.context.status) }}
            </a-tag>
          </a-descriptions-item>
          <a-descriptions-item label="进度">
            {{ selectedTaskForDetail.context.progress_percent.toFixed(1) }}%
          </a-descriptions-item>
          <a-descriptions-item label="已处理行数">
            {{ selectedTaskForDetail.context.processed_rows || 0 }}
          </a-descriptions-item>
          <a-descriptions-item label="总行数">
            {{ selectedTaskForDetail.context.total_rows || 0 }}
          </a-descriptions-item>
          <a-descriptions-item label="当前位置">
            {{ selectedTaskForDetail.context.current_position || '-' }}
          </a-descriptions-item>
          <a-descriptions-item label="运行时长">
            {{ calculateDuration(selectedTaskForDetail.context.start_time, selectedTaskForDetail.context.end_time) }}
          </a-descriptions-item>
          <a-descriptions-item label="开始时间">
            {{ formatTime(selectedTaskForDetail.context.start_time) }}
          </a-descriptions-item>
          <a-descriptions-item label="结束时间">
            {{ formatTime(selectedTaskForDetail.context.end_time) }}
          </a-descriptions-item>
        </a-descriptions>
        
        <!-- 错误信息 -->
        <a-descriptions 
          v-if="selectedTaskForDetail.context.error_stack" 
          title="错误信息" 
          :column="1" 
          bordered 
          style="margin-top: 20px"
        >
          <a-descriptions-item label="错误详情">
            <a-alert type="error" style="margin: 0">
              <pre style="margin: 0; white-space: pre-wrap; word-break: break-word;">{{ selectedTaskForDetail.context.error_stack }}</pre>
            </a-alert>
          </a-descriptions-item>
        </a-descriptions>
        
        <!-- 同步表列表 -->
        <a-descriptions title="同步表" :column="1" bordered style="margin-top: 20px">
          <a-descriptions-item label="表列表">
            <a-tag v-for="table in selectedTaskForDetail.config.tables" :key="table" style="margin: 4px">
              {{ table }}
            </a-tag>
            <span v-if="!selectedTaskForDetail.config.tables || selectedTaskForDetail.config.tables.length === 0">
              全库同步
            </span>
          </a-descriptions-item>
        </a-descriptions>
        
        <!-- 操作按钮 -->
        <div style="margin-top: 20px; text-align: right">
          <a-space>
            <a-button 
              v-if="selectedTaskForDetail.context.status === 'PENDING' || selectedTaskForDetail.context.status === 'PAUSED'" 
              type="primary"
              status="success"
              @click="startTask(selectedTaskForDetail.config.id); detailDrawerVisible = false"
            >
              <template #icon><icon-play-arrow /></template>
              启动
            </a-button>
            <a-button 
              v-if="selectedTaskForDetail.context.status === 'RUNNING'" 
              status="warning"
              @click="pauseTask(selectedTaskForDetail.config.id); detailDrawerVisible = false"
            >
              <template #icon><icon-pause /></template>
              暂停
            </a-button>
          </a-space>
        </div>
      </div>
    </a-drawer>
  </a-layout>
</template>

<style scoped>
.layout-container {
  height: 100vh;
  background: #f5f7fa;
}

.task-form-full-page {
  max-width: 1100px;
  margin: 0 auto;
  padding: 8px 0 40px;
}

.sider {
  background: linear-gradient(180deg, #1d2129 0%, #165dff 100%);
  display: flex;
  flex-direction: column;
}

.logo {
  height: 64px;
  display: flex;
  align-items: center;
  padding: 0 20px;
  color: #fff;
  border-bottom: 1px solid rgba(255, 255, 255, 0.1);
}

.logo-icon {
  width: 32px;
  height: 32px;
  background: rgba(255, 255, 255, 0.2);
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

/* 禁止菜单收缩 */
.sider-menu:not(.arco-menu-collapsed) {
  width: 100% !important;
}

/* 菜单inner容器 - 禁止动画 */
.sider-menu :deep(.arco-menu-inner) {
  display: flex !important;
  flex-direction: column !important;
  opacity: 1 !important;
  animation: none !important;
  transition: none !important;
}

/* 菜单项 - 常亮显示，禁止所有动画和过渡 */
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

/* 强制覆盖Arco Design菜单项的所有状态背景 */
.sider-menu :deep(.arco-menu-item),
.sider-menu :deep(.arco-menu-item.arco-menu-selected),
.sider-menu :deep(.arco-menu-item:hover),
.sider-menu :deep(.arco-menu-item:focus),
.sider-menu :deep(.arco-menu-item:active),
.sider-menu :deep(.arco-menu-item[data-key="tasks"]),
.sider-menu :deep(.arco-menu-item[data-key="config"]) {
  background: transparent !important;
  background-color: transparent !important;
}

/* 菜单项图标 - 常亮显示，禁止动画 */
.sider-menu :deep(.arco-menu-item .arco-menu-icon) {
  color: #fff !important;
  opacity: 1 !important;
  visibility: visible !important;
  display: inline-flex !important;
  margin-right: 12px !important;
  animation: none !important;
  transition: none !important;
}

/* 菜单项文字 - 常亮显示，禁止动画 */
.sider-menu :deep(.arco-menu-item .arco-menu-title) {
  color: #fff !important;
  opacity: 1 !important;
  visibility: visible !important;
  display: inline !important;
  animation: none !important;
  transition: none !important;
  transform: none !important;
}

/* 菜单项内所有内容 */
.sider-menu :deep(.arco-menu-item *) {
  color: #fff !important;
  opacity: 1 !important;
  visibility: visible !important;
  animation: none !important;
  transition: none !important;
}

/* 禁止Arco Design菜单的所有动画效果 */
.sider-menu :deep(.arco-menu-collapse-icon) {
  display: none !important;
}

/* 禁止菜单展开/折叠的宽度动画 */
.sider-menu :deep(.arco-menu) {
  transition: none !important;
  animation: none !important;
}

/* 悬停状态 - 只改变背景，不改变透明度 */
.sider-menu :deep(.arco-menu-item:hover) {
  background: rgba(255, 255, 255, 0.15) !important;
  color: #fff !important;
}

/* 选中状态 */
.sider-menu :deep(.arco-menu-item.arco-menu-selected) {
  background: rgba(255, 255, 255, 0.25) !important;
  color: #fff !important;
}

/* 禁止所有伪元素动画 */
.sider-menu :deep(.arco-menu-item::before),
.sider-menu :deep(.arco-menu-item::after) {
  animation: none !important;
  transition: none !important;
}

/* 确保菜单图标svg也常亮 */
.sider-menu :deep(.arco-menu-item svg) {
  opacity: 1 !important;
  visibility: visible !important;
  color: #fff !important;
}

.sider-footer {
  padding: 16px 20px;
  border-top: 1px solid rgba(255, 255, 255, 0.1);
}

.header {
  background: #fff;
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

.stat-cards {
  margin-bottom: 24px;
}

.stat-card {
  border-radius: 8px;
}

.stat-icon {
  font-size: 20px;
  margin-right: 8px;
}

.stat-icon.blue {
  color: #165dff;
}

.stat-icon.green {
  color: #00b42a;
}

.stat-icon.red {
  color: #f53f3f;
}

.task-list-card {
  border-radius: 8px;
}

.empty-state {
  padding: 60px 0;
  text-align: center;
}

.task-item {
  padding: 0 !important;
  margin-bottom: 16px;
  border: none !important;
}

.task-item:last-child {
  margin-bottom: 0;
}

.task-card-inner {
  background: #fafbfc;
  border-radius: 8px;
  width: 100%;
}

.task-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 12px;
}

.task-title {
  display: flex;
  align-items: center;
  gap: 12px;
}

.task-desc {
  margin-bottom: 12px;
}

.task-progress {
  display: flex;
  align-items: center;
  gap: 16px;
  margin-bottom: 12px;
}

.progress-text {
  color: #86909c;
  font-size: 13px;
  white-space: nowrap;
}

.task-actions {
  display: flex;
  gap: 8px;
  justify-content: flex-end;
  padding-top: 12px;
  border-top: 1px solid #e5e6eb;
}
</style>