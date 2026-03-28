import re

with open('web/src/App.vue', 'r', encoding='utf-8') as f:
    content = f.read()

# Extract template block content only
tmpl_start = content.index('<template>') + len('<template>')
depth = 1
pos = tmpl_start
tmpl_end = len(content)
while depth > 0 and pos < len(content):
    m_open = content.find('<template', pos)
    m_close = content.find('</template>', pos)
    if m_close == -1:
        break
    if m_open != -1 and m_open < m_close:
        depth += 1
        pos = m_open + 1
    else:
        depth -= 1
        if depth == 0:
            tmpl_end = m_close
        pos = m_close + 1

template = content[tmpl_start:tmpl_end]
base_line = content[:tmpl_start].count('\n') + 1

def tokenize_tags(html):
    i = 0
    tags = []
    while i < len(html):
        if html[i] != '<':
            i += 1
            continue
        j = i + 1
        # Skip comments
        if html[j:j+3] == '!--':
            end = html.find('-->', j)
            i = end + 3 if end != -1 else len(html)
            continue
        is_close = j < len(html) and html[j] == '/'
        if is_close:
            j += 1
        # Tag name
        name_start = j
        while j < len(html) and (html[j].isalnum() or html[j] in '-_:'):
            j += 1
        tag_name = html[name_start:j]
        if not tag_name:
            i += 1
            continue
        # Skip attributes, handling quotes
        is_self_close = False
        while j < len(html) and html[j] != '>':
            if html[j] in ('"', "'"):
                q = html[j]
                j += 1
                while j < len(html) and html[j] != q:
                    j += 1
            j += 1
        # Check for self-close: the char before > is /
        tag_text = html[i:j+1]
        if tag_text.rstrip().endswith('/>'):
            is_self_close = True
        line_num = base_line + html[:i].count('\n')
        tags.append((tag_name, is_close, is_self_close, line_num))
        i = j + 1
    return tags

VOID = {'br','hr','img','input','meta','link','area','base','col','embed','param','source','track','wbr'}

tags = tokenize_tags(template)
stack = []
for tag_name, is_close, is_self_close, line_num in tags:
    if is_self_close or tag_name.lower() in VOID:
        continue
    if is_close:
        if stack and stack[-1][0] == tag_name:
            stack.pop()
        else:
            top = stack[-1] if stack else None
            print(f'MISMATCH close </{tag_name}> at line {line_num}, stack top: {top}')
    else:
        stack.append((tag_name, line_num))

print(f'Remaining open elements ({len(stack)}):')
for tag, line in reversed(stack):
    print(f'  Line {line}: <{tag}>')
