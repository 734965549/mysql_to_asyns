import re # 导入正则表达式模块

with open('web/src/App.vue', 'r', encoding='utf-8') as f: # 打开Vue文件
    content = f.read() # 读取文件内容

# Extract template block content only
tmpl_start = content.index('<template>') + len('<template>') # 找到template标签开始位置
depth = 1 # 设置深度为1
pos = tmpl_start # 设置当前位置
tmpl_end = len(content) # 设置结束位置
while depth > 0 and pos < len(content): # 当深度大于0且位置小于内容长度时循环
    m_open = content.find('<template', pos) # 查找template标签
    m_close = content.find('</template>', pos) # 查找template结束标签
    if m_close == -1: # 如果找不到结束标签
        break # 跳出循环
    if m_open != -1 and m_open < m_close: # 如果找到开始标签且在结束标签之前
        depth += 1 # 深度加1
        pos = m_open + 1 # 更新位置
    else: # 否则
        depth -= 1 # 深度减1
        if depth == 0: # 如果深度为0
            tmpl_end = m_close # 设置结束位置
        pos = m_close + 1 # 更新位置

template = content[tmpl_start:tmpl_end] # 提取template内容
base_line = content[:tmpl_start].count('\n') + 1 # 计算起始行号

def tokenize_tags(html): # 标签分词函数
    i = 0 # 初始化位置
    tags = [] # 创建标签列表
    while i < len(html): # 遍历HTML内容
        if html[i] != '<': # 如果不是标签开始字符
            i += 1 # 位置加1
            continue # 继续循环
        j = i + 1 # 设置下一个位置
        # Skip comments
        if html[j:j+3] == '!--': # 如果是注释开始
            end = html.find('-->', j) # 查找注释结束
            i = end + 3 if end != -1 else len(html) # 更新位置
            continue # 继续循环
        is_close = j < len(html) and html[j] == '/' # 判断是否是关闭标签
        if is_close: # 如果是关闭标签
            j += 1 # 位置加1
        # Tag name
        name_start = j # 记录标签名开始位置
        while j < len(html) and (html[j].isalnum() or html[j] in '-_:'): # 获取标签名
            j += 1 # 位置加1
        tag_name = html[name_start:j] # 提取标签名
        if not tag_name: # 如果标签名为空
            i += 1 # 位置加1
            continue # 继续循环
        # Skip attributes, handling quotes
        is_self_close = False # 初始化自关闭标志
        while j < len(html) and html[j] != '>': # 跳过属性
            if html[j] in ('"', "'"): # 如果遇到引号
                q = html[j] # 记录引号类型
                j += 1 # 位置加1
                while j < len(html) and html[j] != q: # 跳过引号内容
                    j += 1 # 位置加1
            j += 1 # 位置加1
        # Check for self-close: the char before > is /
        tag_text = html[i:j+1] # 获取标签文本
        if tag_text.rstrip().endswith('/>'): # 如果以/>结尾
            is_self_close = True # 设置自关闭标志
        line_num = base_line + html[:i].count('\n') # 计算行号
        tags.append((tag_name, is_close, is_self_close, line_num)) # 添加标签到列表
        i = j + 1 # 更新位置
    return tags # 返回标签列表

VOID = {'br','hr','img','input','meta','link','area','base','col','embed','param','source','track','wbr'} # 空元素集合

tags = tokenize_tags(template) # 分词标签
stack = [] # 创建栈
for tag_name, is_close, is_self_close, line_num in tags: # 遍历标签
    if is_self_close or tag_name.lower() in VOID: # 如果是自关闭或空元素
        continue # 跳过
    if is_close: # 如果是关闭标签
        if stack and stack[-1][0] == tag_name: # 如果栈不为空且栈顶标签匹配
            stack.pop() # 弹出栈顶
        else: # 否则
            top = stack[-1] if stack else None # 获取栈顶
            print(f'MISMATCH close </{tag_name}> at line {line_num}, stack top: {top}') # 输出不匹配信息
    else: # 如果是开始标签
        stack.append((tag_name, line_num)) # 压入栈

print(f'Remaining open elements ({len(stack)}):') # 输出剩余未关闭元素数量
for tag, line in reversed(stack): # 反向遍历栈
    print(f'  Line {line}: <{tag}>') # 输出未关闭元素
