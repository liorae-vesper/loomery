// SPDX-License-Identifier: MPL-2.0

//! Checks that every Mermaid diagram in the documentation parses.
//!
//! A diagram that does not parse is worse than no diagram: it renders as an error
//! box for whoever opens the file. Markdown has no compiler, so this is the
//! closest thing — `mermaid.parse` is the same parser a renderer uses.
//!
//! Usage:
//!
//! ```sh
//! node check.mjs [path ...]        # default: docs/
//! node check.mjs --render docs     # also render each diagram to SVG
//! ```
//!
//! `--render` exercises the layout engine as well as the parser, and writes the
//! SVGs to `tools/mermaid-check/out/` for a human to look at. Continuous
//! integration runs the parse check only: parsing is what a broken diagram fails,
//! and rendering adds seconds and a DOM for no extra signal.

import { globSync, mkdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { join, relative, resolve } from "node:path";

const args = process.argv.slice(2);
const render = args.includes("--render");
const roots = args.filter((arg) => !arg.startsWith("--"));

// The parser wants a DOM. jsdom provides one; the shims below are the pieces
// mermaid's *render* path touches that jsdom does not implement (they are inert
// for parsing).
const { JSDOM } = await import("jsdom");
const dom = new JSDOM("<!DOCTYPE html><body></body>", { pretendToBeVisual: true });
globalThis.window = dom.window;
globalThis.document = dom.window.document;
Object.defineProperty(globalThis, "navigator", {
  value: dom.window.navigator,
  configurable: true,
});
class StyleSheetShim {
  constructor() {
    this.cssRules = [];
  }
  insertRule(rule) {
    this.cssRules.push(rule);
    return this.cssRules.length - 1;
  }
  replaceSync() {}
}
globalThis.CSSStyleSheet = StyleSheetShim;
dom.window.CSSStyleSheet = StyleSheetShim;
globalThis.CSS = { supports: () => false, escape: (value) => value };
dom.window.CSS = globalThis.CSS;
dom.window.SVGElement.prototype.getBBox = () => ({ x: 0, y: 0, width: 100, height: 20 });
dom.window.Element.prototype.getComputedTextLength = () => 100;

const mermaid = (await import("mermaid")).default;
mermaid.initialize({ startOnLoad: false, securityLevel: "loose" });

/** Every markdown file under a root — or the root itself, when it is a file. */
function markdownFiles(root) {
  const absolute = resolve(root);
  if (statSync(absolute, { throwIfNoEntry: false })?.isFile()) {
    return [absolute];
  }
  return globSync(join(absolute, "**/*.md"), { exclude: ["**/node_modules/**"] }).sort();
}

/** The `mermaid` code blocks of a document, with the line each starts on. */
function diagrams(text) {
  const blocks = [];
  let start = null;
  let body = [];
  for (const [index, line] of text.split("\n").entries()) {
    if (start === null && line.trim() === "```mermaid") {
      start = index + 1;
      body = [];
      continue;
    }
    if (start === null) {
      continue;
    }
    if (line.trim() === "```") {
      blocks.push({ line: start, text: body.join("\n") });
      start = null;
    } else {
      body.push(line);
    }
  }
  return blocks;
}

const files = (roots.length > 0 ? roots : ["docs"]).flatMap(markdownFiles);
const out = join("tools", "mermaid-check", "out");
let checked = 0;
let failed = 0;

for (const file of files) {
  for (const diagram of diagrams(readFileSync(file, "utf8"))) {
    checked += 1;
    const where = `${relative(process.cwd(), file)}:${diagram.line}`;
    try {
      await mermaid.parse(diagram.text);
    } catch (error) {
      failed += 1;
      console.error(`✗ ${where}: ${reason(error)}`);
      continue;
    }
    if (!render) {
      console.log(`✓ ${where}`);
      continue;
    }
    try {
      const { svg } = await mermaid.render(`diagram-${checked}`, diagram.text);
      mkdirSync(out, { recursive: true });
      const target = join(out, `${where.replaceAll(/[/:.]/g, "-")}.svg`);
      writeFileSync(target, svg);
      console.log(`✓ ${where} → ${target}`);
    } catch (error) {
      failed += 1;
      console.error(`✗ ${where}: ${reason(error)}`);
    }
  }
}

console.log(`\n${checked} diagram(s) in ${files.length} file(s), ${failed} failing`);
process.exit(failed === 0 ? 0 : 1);

/** The first few lines of an error, as one line. */
function reason(error) {
  return String(error?.message ?? error).split("\n").slice(0, 4).join(" | ");
}
