import fs from "node:fs";

const vuePath = "src/views/TaskFormView.vue";
const vue = fs.readFileSync(vuePath, "utf8");
const styleCss = fs.readFileSync("src/_extract/style-form.css", "utf8");
const lines = styleCss.split(/\r?\n/);

// Keep only clean form styles before theme / layout-container:not fragments
let end = lines.findIndex((l) =>
  l.includes(".layout-container:not(.theme-default)"),
);
if (end < 0) end = lines.length;

// Also drop orphaned indented media-body fragments before that
let cssLines = lines.slice(0, end);

// Remove trailing orphaned indented rules that look like media-query bodies
// without @media wrapper: lines starting with two spaces at end after a closed rule
while (cssLines.length) {
  const last = cssLines[cssLines.length - 1].trim();
  if (last === "" || last === "}") break;
  // if file ends mid-selector with comma, drop
  if (last.endsWith(",")) {
    cssLines.pop();
    continue;
  }
  break;
}

// Fix: remove block of indented rules that appear without @media
// (from extractor breaking @media)
const fixed = [];
let i = 0;
while (i < cssLines.length) {
  const line = cssLines[i];
  // Detect start of orphaned indented selector after a closed rule
  if (
    /^\s{2}\.[a-z]/.test(line) &&
    fixed.length &&
    fixed[fixed.length - 1].trim() === "}"
  ) {
    // skip until we leave indented block (back to column 0 selector or end)
    while (i < cssLines.length && (cssLines[i].startsWith("  ") || cssLines[i].trim() === "")) {
      i++;
    }
    continue;
  }
  fixed.push(line);
  i++;
}

const scriptEnd = vue.indexOf("</script>");
const script = vue.slice(0, scriptEnd + "</script>".length);
let tpl = fs.readFileSync("src/_extract/tpl-form.html", "utf8");
tpl = tpl
  .replace(/\s+v-if="taskFormPage === 'create' \|\| taskFormPage === 'edit'"/g, "")
  .replace(/taskFormPage === 'edit'/g, "editMode")
  .replace(/taskFormPage === \"edit\"/g, "editMode")
  .replace(
    /@click="taskFormPage = 'select_type'"/g,
    `@click="clearFormHeaderActions(); router.push('/tasks/new/select')"`,
  );

const out = `${script}

<template>
${tpl}
</template>

<style scoped>
${fixed.join("\n").trim()}
</style>
`;
fs.writeFileSync(vuePath, out.replace(/\r\n/g, "\n"));
console.log("lines", out.split("\n").length, "css lines", fixed.length);
