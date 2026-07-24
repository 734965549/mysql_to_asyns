import fs from "node:fs";

const vue = fs.readFileSync("src/views/TaskFormView.vue", "utf8");
const script = vue.slice(0, vue.indexOf("</script>"));
const tpl = vue.slice(vue.indexOf("<template>"), vue.indexOf("</template>"));

const defined = new Set();
for (const m of script.matchAll(
  /(?:function|const|async function)\s+([A-Za-z_][\w]*)/g,
)) {
  defined.add(m[1]);
}
// also from destructuring unlikely

const used = new Set();
for (const m of tpl.matchAll(/\b([A-Za-z_][\w]*)\b/g)) {
  used.add(m[1]);
}

const skip = new Set([
  "div", "span", "template", "class", "style", "true", "false", "null", "undefined",
  "a", "key", "type", "size", "label", "value", "icon", "row", "col", "model",
  "length", "map", "filter", "includes", "push", "trim", "String", "Number", "Array",
  "console", "window", "document", "JSON", "Date", "Math", "Intl", "Object",
  "primary", "secondary", "mini", "small", "large", "vertical", "horizontal",
  "text", "password", "number", "checkbox", "radio", "button", "input", "select",
  "MYSQL", "KAFKA", "WEBHOOK", "MULTI", "HTTP_WEBHOOK", "FULL", "INCREMENTAL", "ALL",
  "database", "table", "v1", "v2", "POST", "GET",
]);

const missing = [...used]
  .filter((n) => /^[a-z]/.test(n) && !skip.has(n) && !defined.has(n))
  .filter((n) => !n.startsWith("arco") && !n.startsWith("icon"))
  .sort();

console.log("maybe missing:", missing.join(", "));
