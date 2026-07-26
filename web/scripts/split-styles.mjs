import fs from "fs";
import path from "path";

const root = "web/src";
const extractDir = path.join(root, "_extract");
const styles = fs
  .readFileSync(path.join(extractDir, "all-styles.css"), "utf8")
  .split(/\r?\n/);

function extractStyleRules(predicates) {
  const out = [];
  let i = 0;
  while (i < styles.length) {
    const line = styles[i];
    const trimmed = line.trim();
    if (!trimmed) {
      i++;
      continue;
    }
    if (trimmed.startsWith("/*")) {
      let j = i;
      const commentBlock = [];
      while (j < styles.length) {
        commentBlock.push(styles[j]);
        if (styles[j].includes("*/")) {
          j++;
          break;
        }
        j++;
        if (j - i > 40) break;
      }
      while (j < styles.length && styles[j].trim() === "") {
        commentBlock.push(styles[j]);
        j++;
      }
      const sel = styles[j] || "";
      if (predicates.some((p) => p(sel))) {
        out.push(...commentBlock);
        i = j;
        continue;
      }
      i = j > i ? j : i + 1;
      continue;
    }
    if (predicates.some((p) => p(line))) {
      const buf = [styles[i]];
      let bal =
        (styles[i].match(/{/g) || []).length -
        (styles[i].match(/}/g) || []).length;
      i++;
      while (i < styles.length && bal > 0) {
        buf.push(styles[i]);
        bal +=
          (styles[i].match(/{/g) || []).length -
          (styles[i].match(/}/g) || []).length;
        i++;
      }
      if (i < styles.length && styles[i].trim() === "") {
        buf.push(styles[i]);
        i++;
      }
      out.push(...buf);
      continue;
    }
    i++;
  }
  return out.join("\n");
}

const parts = {
  "style-detail.css": extractStyleRules([
    (s) =>
      /\.task-detail-page|\.detail-page|\.detail-header|\.detail-overview|\.detail-tabs|\.overview-|\.runtime-/.test(
        s,
      ),
  ]),
  "style-layout.css": extractStyleRules([
    (s) =>
      /^\.(layout-container|sider|logo|header|content|sider-menu|sider-footer)/.test(
        s.trim(),
      ) &&
      !s.includes("theme-") &&
      !s.includes(":not(.theme"),
  ]),
  "style-select.css": extractStyleRules([
    (s) =>
      /\.select-type|\.type-card|\.type-icon|\.type-content|\.type-cards|\.mysql-icon|\.kafka-icon|\.multi-icon/.test(
        s,
      ),
  ]),
  "style-list.css": extractStyleRules([
    (s) =>
      /\.stat-|\.task-list|\.task-filter|\.task-card|\.task-info|\.task-title|\.task-status|\.filter-chip|\.empty-state|\.task-actions|\.advanced-filter|\.pagination-wrap/.test(
        s,
      ),
  ]),
  "style-config.css": extractStyleRules([
    (s) =>
      /\.config-|\.theme-option|\.theme-config/.test(s) &&
      !/\.theme-(blue|gray|black|dark|default)\b/.test(s),
  ]),
  "style-form.css": extractStyleRules([
    (s) =>
      /\.task-form|\.task-create|\.task-base|\.transfer|\.table-selector|\.table-mapping|\.db-transfer|\.advanced-config|\.mapped-item|\.full-load|\.sink-/.test(
        s,
      ),
  ]),
};

for (const [name, content] of Object.entries(parts)) {
  fs.writeFileSync(path.join(extractDir, name), content + "\n");
  console.log(name, content.split("\n").length);
}
