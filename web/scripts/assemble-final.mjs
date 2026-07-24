#!/usr/bin/env node
/**
 * Final assembler: extract from current App.vue and write all views/layout/themes.
 * Run from repo root: node web/scripts/assemble-final.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const WEB = path.resolve(__dirname, "..");
const SRC = path.join(WEB, "src");
const APP = path.join(SRC, "App.vue");
const EXT = path.join(SRC, "_extract");

const raw = fs.readFileSync(APP, "utf8");
const lines = raw.split(/\r?\n/);
const slice = (a, b) => lines.slice(a - 1, b).join("\n");
const write = (rel, content) => {
  const p = path.join(WEB, rel);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, content, "utf8");
  console.log("wrote", rel, content.split("\n").length, "lines");
};

// --- ranges for current App.vue (10748 lines) ---
const R = {
  detail: [2958, 3776],
  sider: [3781, 3816],
  select: [3871, 3957],
  form: [3960, 5995],
  list: [5999, 6507],
  config: [6511, 6698],
  modal: [7205, 7255],
  styles: [7259, 10746],
};

fs.mkdirSync(EXT, { recursive: true });
for (const [k, [a, b]] of Object.entries(R)) {
  write(`src/_extract/tpl-${k}.html`.replace("tpl-styles.html", "all-styles.css").replace("tpl-modal.html", "tpl-modal.html"), slice(a, b));
}
// fix styles key naming
write("src/_extract/all-styles.css", slice(...R.styles));
write("src/_extract/tpl-detail.html", slice(...R.detail));
write("src/_extract/tpl-sider.html", slice(...R.sider));
write("src/_extract/tpl-select-type.html", slice(...R.select));
write("src/_extract/tpl-form.html", slice(...R.form));
write("src/_extract/tpl-list.html", slice(...R.list));
write("src/_extract/tpl-config.html", slice(...R.config));
write("src/_extract/tpl-modal.html", slice(...R.modal));

// Split styles (reuse logic inline)
const styles = slice(...R.styles).split("\n");
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
      out.push(...buf);
      continue;
    }
    i++;
  }
  return out.join("\n");
}

const styleParts = {
  detail: extractStyleRules([
    (s) =>
      /\.task-detail-page|\.detail-page|\.detail-header|\.detail-overview|\.detail-tabs|\.overview-|\.runtime-/.test(
        s,
      ),
  ]),
  layout: extractStyleRules([
    (s) =>
      /^\.(layout-container|sider|logo|header|content|sider-menu|sider-footer)/.test(
        s.trim(),
      ) &&
      !s.includes("theme-") &&
      !s.includes(":not(.theme"),
  ]),
  select: extractStyleRules([
    (s) =>
      /\.select-type|\.type-card|\.type-icon|\.type-content|\.type-cards|\.mysql-icon|\.kafka-icon|\.multi-icon/.test(
        s,
      ),
  ]),
  list: extractStyleRules([
    (s) =>
      /\.stat-|\.task-list|\.task-filter|\.task-card|\.task-info|\.task-title|\.task-status|\.filter-chip|\.empty-state|\.task-actions|\.advanced-filter|\.pagination-wrap/.test(
        s,
      ),
  ]),
  config: extractStyleRules([
    (s) =>
      /\.config-|\.theme-option|\.theme-config/.test(s) &&
      !/\.theme-(blue|gray|black|dark|default)\b/.test(s),
  ]),
  form: extractStyleRules([
    (s) =>
      /\.task-form|\.task-create|\.task-base|\.transfer|\.table-selector|\.table-mapping|\.db-transfer|\.advanced-config|\.mapped-item|\.full-load|\.sink-/.test(
        s,
      ),
  ]),
};

for (const [k, v] of Object.entries(styleParts)) {
  write(`src/_extract/style-${k}.css`, v + "\n");
}

// themes.css from Sci-fi block onward
let themeStart = styles.findIndex((l) => l.includes("Sci-fi create task surface"));
if (themeStart < 0) {
  themeStart = styles.findIndex((l) => /^\.theme-blue\s*\{/.test(l));
}
write("src/styles/themes.css", styles.slice(Math.max(0, themeStart)).join("\n") + "\n");

console.log("extracts ready");
