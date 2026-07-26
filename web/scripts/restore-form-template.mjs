import fs from "node:fs";

const path = "src/views/TaskFormView.vue";
const vue = fs.readFileSync(path, "utf8");
let tpl = fs.readFileSync("src/_extract/tpl-form.html", "utf8");
tpl = tpl.replace(
  /\s+v-if="taskFormPage === 'create' \|\| taskFormPage === 'edit'"/g,
  "",
);
tpl = tpl.replace(/taskFormPage === 'edit'/g, "editMode");
tpl = tpl.replace(/taskFormPage === \"edit\"/g, "editMode");
tpl = tpl.replace(
  /@click="taskFormPage = 'select_type'"/g,
  `@click="clearFormHeaderActions(); router.push('/tasks/new/select')"`,
);
const style = fs.readFileSync("src/_extract/style-form.css", "utf8");
const scriptEnd = vue.indexOf("</script>");
if (scriptEnd < 0) throw new Error("no script");
const script = vue.slice(0, scriptEnd + "</script>".length);
const out = `${script}

<template>
${tpl}
</template>

<style scoped>
${style}
</style>
`;
fs.writeFileSync(path, out.replace(/\r\n/g, "\n"));
console.log("restored TaskFormView.vue lines", out.split("\n").length);
