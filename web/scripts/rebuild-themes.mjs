import fs from "node:fs";

const styles = fs.readFileSync("src/_extract/all-styles.css", "utf8").split(/\r?\n/);
let themeStart = styles.findIndex((l) => l.includes("Sci-fi create task surface"));
if (themeStart < 0) {
  themeStart = styles.findIndex((l) => /^\.theme-blue\s*\{/.test(l));
}
let css = styles.slice(Math.max(0, themeStart)).join("\n");
// Global CSS: unwrap :deep(x) -> x
css = css.replace(/:deep\(([^)]+)\)/g, (_, inner) => inner);
fs.writeFileSync("src/styles/themes.css", css + "\n");
console.log(
  "themes.css lines",
  css.split("\n").length,
  "dangling",
  (css.match(/>\s*\{/g) || []).length,
  "deep left",
  (css.match(/:deep\(/g) || []).length,
);
