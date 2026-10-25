import { assert, expect, it } from "@effect/vitest";
import { RunId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { type ThreadTools, waitForRun, workspaceThreadOperation } from "./thread.ts";

const threadId = ThreadId.make("thread-1");
const runId = RunId.make("run-1");

it.effect("reading verification evidence never asks the server to execute a check", () =>
  Effect.gen(function* () {
    const evidence = {
      verificationId: "attempt",
      execution: "failed",
      currentness: "Stale",
      endSource: { digest: "tested-source" },
    };
    const { tools, calls } = scriptedTools({ get_thread_verification: [evidence] });
    expect(yield* workspaceThreadOperation(tools, { operation: "verification", threadId })).toEqual(
      evidence,
    );
    assert.strictEqual(calls.length, 1);
    assert.strictEqual(calls[0]?.tool, "get_thread_verification");
  }),
);

it.effect("explicit CLI verification carries an idempotency ID without supplying a command", () =>
  Effect.gen(function* () {
    const evidence = { verificationId: "attempt", execution: "queued", currentness: "Unknown" };
    const { tools, calls } = scriptedTools({ verify_thread: [evidence] });
    expect(
      yield* workspaceThreadOperation(tools, {
        operation: "verify",
        threadId,
        clientRequestId: "requested-check",
      }),
    ).toEqual(evidence);
    assert.deepStrictEqual(calls[0]?.args, { threadId, clientRequestId: "requested-check" });
  }),
);

/** Answers each tool call with the next scripted response for that tool. */
const scriptedTools = (script: Record<string, Array<unknown>>) => {
  const calls: Array<{ readonly tool: string; readonly args: unknown }> = [];
  const call = ((tool: string, args: unknown) => {
    calls.push({ tool, args });
    const next = script[tool]?.shift();
    return next === undefined ? Effect.die(`unscripted ${tool} call`) : Effect.succeed(next);
  }) as ThreadTools["call"];
  return { tools: { call } satisfies ThreadTools, calls };
};

it.effect("--wait follows the sent run to its continuation's terminal status", () =>
  Effect.gen(function* () {
    const continuation = RunId.make("run-2");
    const { tools, calls } = scriptedTools({
      t3_thread_wait: [
        { threadId, runId, status: "running", timedOut: true },
        { threadId, runId: continuation, status: "failed", timedOut: false },
      ],
      t3_thread_read: [{ thread: { pendingRequestCount: 0 } }],
    });

    const outcome = yield* waitForRun(tools, threadId, runId);

    assert.deepStrictEqual(outcome, { type: "ended", runId: continuation, status: "failed" });
    assert.deepStrictEqual(
      calls.filter((entry) => entry.tool === "t3_thread_wait").map((entry) => entry.args),
      [
        { threadId, runId, timeoutMs: 15_000 },
        { threadId, runId, timeoutMs: 15_000 },
      ],
    );
  }),
);

it.effect("--wait stops when the thread waits for an answer instead of polling forever", () =>
  Effect.gen(function* () {
    const { tools } = scriptedTools({
      t3_thread_wait: [
        { threadId, runId, status: "queued", timedOut: true },
        { threadId, runId, status: "queued", timedOut: true },
      ],
      t3_thread_read: [{ thread: { pendingRequestCount: 1 } }],
    });

    assert.deepStrictEqual(yield* waitForRun(tools, threadId, runId), { type: "needs_input" });
  }),
);

it.effect("--wait reports its run's result when a later run is the one waiting", () =>
  Effect.gen(function* () {
    const { tools } = scriptedTools({
      t3_thread_wait: [
        { threadId, runId, status: "running", timedOut: true },
        { threadId, runId, status: "completed", timedOut: false },
      ],
      t3_thread_read: [{ thread: { pendingRequestCount: 1 } }],
    });

    assert.deepStrictEqual(yield* waitForRun(tools, threadId, runId), {
      type: "ended",
      runId,
      status: "completed",
    });
  }),
);
