// Recomputes /perfect coverage from scratch. Not inherited — run it.
// RULE (three clauses, all required): a map context is COVERED when it has
//   (a) >=1 direction note pointing at it (`context: "[[<name>]]"`), OR
//   (b) an explicit nothing-clears-the-bar verdict in its context note, OR
//   (c) `last_proposed != never` in its context-note frontmatter.
// CRLF MUST be normalized before any frontmatter regex — a naive
// /^---\n([\s\S]*?)\n---/ silently skips every CRLF file in this repo and
// scored 18/46 in r17.
import fs from "node:fs";
import path from "node:path";

const root = process.argv[2] || ".";
const vault = path.join(root, ".perfect", "Perfect");
const map = JSON.parse(fs.readFileSync(path.join(root, "context-map.json"), "utf8"));

const text = (p) => fs.readFileSync(p, "utf8").replace(/\r\n/g, "\n");
const fm = (p) => {
  const m = text(p).match(/^---\n([\s\S]*?)\n---/);
  return m ? m[1] : "";
};
const field = (block, key) => {
  const m = block.match(new RegExp(`^${key}:\\s*(.*)$`, "m"));
  return m ? m[1].trim().replace(/^["']|["']$/g, "") : null;
};

const mapNames = (map.contexts || []).map((c) => c.name);
const ctxDir = path.join(vault, "contexts");
const dirDir = path.join(vault, "directions");
const ctxFiles = fs.readdirSync(ctxDir).filter((f) => f.endsWith(".md"));
const dirFiles = fs.readdirSync(dirDir).filter((f) => f.endsWith(".md"));

// (a) contexts pointed at by >=1 direction note
const pointedAt = new Set();
for (const f of dirFiles) {
  const c = field(fm(path.join(dirDir, f)), "context");
  if (c) pointedAt.add(c.replace(/^\[\[|\]\]$/g, ""));
}

const noteByName = new Map();
for (const f of ctxFiles) noteByName.set(f.replace(/\.md$/, ""), path.join(ctxDir, f));

const covered = [];
const never = [];
for (const name of mapNames) {
  const p = noteByName.get(name);
  const block = p ? fm(p) : "";
  const body = p ? text(p) : "";
  const a = pointedAt.has(name);
  const b = /nothing[- ]clears[- ]the[- ]bar/i.test(body);
  const c = p && (field(block, "last_proposed") || "never") !== "never";
  (a || b || c ? covered : never).push(name);
}

const orphanNotes = [...noteByName.keys()].filter((n) => !mapNames.includes(n));
const missingNotes = mapNames.filter((n) => !noteByName.has(n));
const aliased = orphanNotes.filter((n) => /^(superseded_by|alias):/m.test(fm(noteByName.get(n))));

console.log(`map contexts           ${mapNames.length}`);
console.log(`vault context notes    ${ctxFiles.length}`);
console.log(`map contexts NO note   ${missingNotes.length}  ${JSON.stringify(missingNotes)}`);
console.log(`vault-only notes       ${orphanNotes.length}  (aliased: ${aliased.length})`);
console.log(`  un-aliased           ${JSON.stringify(orphanNotes.filter((n) => !aliased.includes(n)))}`);
console.log(`direction notes        ${dirFiles.length}`);
console.log(`\nCOVERAGE  ${covered.length}/${mapNames.length}   never-proposed ${never.length}`);
console.log(`never-proposed: ${never.sort().join(" · ")}`);
