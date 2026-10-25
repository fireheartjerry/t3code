// @effect-diagnostics nodeBuiltinImport:off globalConsole:off
// Holds each file to the number of React effect hooks it calls today, so the
// count only goes down while we remove them.
//
//   node scripts/effect-budget.ts            fail if a file calls more effects than its budget
//   node scripts/effect-budget.ts --update   lower budgets to current counts (never raises)
//
// Raising a budget is a deliberate edit to effect-budget.json, made in the PR
// that needs the effect so the reviewer sees it.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
// The parser keeps comments, strings, and names like `matchCauseEffect(` out of
// the count. TypeScript 7 no longer exports the compiler API.
import ts from "typescript-legacy";

export type Budget = Readonly<Record<string, number>>;

const HOOKS = new Set(["useEffect", "useLayoutEffect", "useInsertionEffect"]);
const HOOK_TEXT = /use(?:Layout|Insertion)?Effect/;
const CODE = /\.[cm]?[jt]sx?$/;
const EXCLUDED = /\.test\.[cm]?[jt]sx?$|(?:^|\/)(?:test|__tests__)\//;

const root = NodePath.resolve(import.meta.dirname, "..");
const budgetPath = NodePath.join(import.meta.dirname, "effect-budget.json");

/** Counts `useEffect(...)` and member calls such as `React.useEffect(...)`. */
export function countEffects(fileName: string, source: string): number {
  if (!HOOK_TEXT.test(source)) return 0;
  const kind = /\.[cm]?[jt]sx$/.test(fileName)
    ? ts.ScriptKind.TSX
    : /\.[cm]?js$/.test(fileName)
      ? ts.ScriptKind.JS
      : ts.ScriptKind.TS;
  const file = ts.createSourceFile(fileName, source, ts.ScriptTarget.Latest, false, kind);
  let count = 0;
  const visit = (node: ts.Node): void => {
    if (ts.isCallExpression(node)) {
      const callee = node.expression;
      const name = ts.isIdentifier(callee)
        ? callee.text
        : ts.isPropertyAccessExpression(callee)
          ? callee.name.text
          : undefined;
      if (name !== undefined && HOOKS.has(name)) count += 1;
    }
    ts.forEachChild(node, visit);
  };
  visit(file);
  return count;
}

export function sortBudget(entries: Iterable<readonly [string, number]>): Budget {
  return Object.fromEntries(
    [...entries].filter(([, count]) => count > 0).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)),
  );
}

/** Files over budget (a missing entry is a budget of 0) and files under it. */
export function compareBudget(budget: Budget, counts: Budget) {
  const over: Array<{ file: string; budget: number; count: number }> = [];
  const under: Array<{ file: string; budget: number; count: number }> = [];
  for (const file of new Set([...Object.keys(budget), ...Object.keys(counts)])) {
    const allowed = budget[file] ?? 0;
    const count = counts[file] ?? 0;
    if (count > allowed) over.push({ file, budget: allowed, count });
    if (count < allowed) under.push({ file, budget: allowed, count });
  }
  const byFile = (a: { file: string }, b: { file: string }) => (a.file < b.file ? -1 : 1);
  return { over: over.sort(byFile), under: under.sort(byFile) };
}

/** Lowers each entry to its current count and drops entries that reach 0. Never adds or raises. */
export function lowerBudget(budget: Budget, counts: Budget): Budget {
  return sortBudget(
    Object.entries(budget).map(([file, allowed]) => [file, Math.min(allowed, counts[file] ?? 0)]),
  );
}

export function formatBudget(budget: Budget): string {
  return `${JSON.stringify(sortBudget(Object.entries(budget)), null, 2)}\n`;
}

export function overBudgetMessage(over: ReturnType<typeof compareBudget>["over"]): string {
  return [
    "effect-budget: these files call more React effect hooks than their budget:",
    ...over.map(({ file, budget, count }) => `  ${file}  ${budget} -> ${count}`),
    "",
    "Prefer deriving in render, event handlers, a `key` reset, a ref callback for",
    "element-owned work, or a store. If an effect is genuinely the right model,",
    "raise that file's entry in scripts/effect-budget.json in the same PR so",
    "reviewers see it. A moved file takes its entry with it.",
  ].join("\n");
}

function sourceFiles(): Array<string> {
  return NodeChildProcess.execFileSync(
    "git",
    ["ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", "apps", "packages"],
    { cwd: root, encoding: "utf8", maxBuffer: 64 * 1024 * 1024 },
  )
    .split("\0")
    .filter((file) => CODE.test(file) && !EXCLUDED.test(file))
    .filter((file) => NodeFS.existsSync(NodePath.join(root, file)));
}

function currentCounts(): Budget {
  return sortBudget(
    sourceFiles().map((file) => [
      file,
      countEffects(file, NodeFS.readFileSync(NodePath.join(root, file), "utf8")),
    ]),
  );
}

if (import.meta.main) {
  const budget: Budget = JSON.parse(NodeFS.readFileSync(budgetPath, "utf8"));
  const counts = currentCounts();
  const update = process.argv.includes("--update");
  if (update) {
    const lowered = lowerBudget(budget, counts);
    NodeFS.writeFileSync(budgetPath, formatBudget(lowered));
    const removed = Object.values(budget).reduce((sum, n) => sum + n, 0);
    const kept = Object.values(lowered).reduce((sum, n) => sum + n, 0);
    console.log(`effect-budget: lowered the budget by ${removed - kept} to ${kept}.`);
  }
  const { over, under } = compareBudget(update ? lowerBudget(budget, counts) : budget, counts);
  if (over.length > 0) {
    console.error(overBudgetMessage(over));
    process.exitCode = 1;
  } else if (under.length > 0) {
    console.log(
      `effect-budget: ${under.length} files are below their budget. ` +
        "Run `vp run effect-budget:update` to lock that in.",
    );
  } else {
    const total = Object.values(counts).reduce((sum, n) => sum + n, 0);
    console.log(`effect-budget: ${total} effects, none over budget.`);
  }
}
