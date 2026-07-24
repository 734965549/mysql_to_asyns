import fs from "fs";
const tpl = fs.readFileSync("web/src/_extract/tpl-detail.html", "utf8");
const ids = new Set();
for (const m of tpl.matchAll(/[a-zA-Z_][\w]*/g)) {
  // too noisy
}
const interesting = new Set();
const re =
  /\b(detailPage\w+|get\w+|format\w+|can\w+|is\w+|open\w+|close\w+|refresh\w+|confirm\w+|sync\w+|runtime\w+|row\w+|has\w+|resume\w+)\b/g;
for (const m of tpl.matchAll(re)) interesting.add(m[1]);
console.log([...interesting].sort().join("\n"));
