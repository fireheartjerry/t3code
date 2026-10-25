import { expect, it } from "vite-plus/test";

import { compareBudget, countEffects, formatBudget, lowerBudget } from "./effect-budget.ts";

it("counts effect hook calls, including member calls, and ignores comments, strings, and lookalikes", () => {
  const source = `
    import React, { useEffect, useLayoutEffect } from "react";
    // useEffect(() => {}) in a comment
    /* useLayoutEffect(() => {}) */
    const label = "useEffect(() => {})";
    const template = \`useInsertionEffect(\${label})\`;
    const pipeline = Effect.matchCauseEffect(effect, {});
    const ref = useEffect;
    export function Panel() {
      useEffect(() => {}, []);
      useLayoutEffect(() => {});
      React.useEffect(() => {});
      React.useInsertionEffect(() => {});
      return <div title="useEffect(x)" />;
    }
  `;
  expect(countEffects("Panel.tsx", source)).toBe(4);
  expect(countEffects("plain.ts", "export const value = 1;")).toBe(0);
});

it("fails a file that gains effects, including a new file, and lets counts drop", () => {
  const budget = { "a.tsx": 2, "b.tsx": 3 };
  expect(compareBudget(budget, { "a.tsx": 3, "b.tsx": 1, "c.tsx": 1 })).toEqual({
    over: [
      { file: "a.tsx", budget: 2, count: 3 },
      { file: "c.tsx", budget: 0, count: 1 },
    ],
    under: [{ file: "b.tsx", budget: 3, count: 1 }],
  });
  expect(compareBudget(budget, { "a.tsx": 2 }).over).toEqual([]);
});

it("update only lowers entries and drops files that reach zero", () => {
  const budget = { "a.tsx": 2, "b.tsx": 3, "deleted.tsx": 1 };
  expect(lowerBudget(budget, { "a.tsx": 5, "b.tsx": 1, "new.tsx": 2 })).toEqual({
    "a.tsx": 2,
    "b.tsx": 1,
  });
});

it("writes one sorted entry per line without zero counts", () => {
  expect(formatBudget({ "z.tsx": 1, "a.tsx": 2, "gone.tsx": 0 })).toBe(
    '{\n  "a.tsx": 2,\n  "z.tsx": 1\n}\n',
  );
});
