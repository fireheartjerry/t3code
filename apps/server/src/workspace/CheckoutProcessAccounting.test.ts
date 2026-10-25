import { assert, describe, it } from "@effect/vitest";
import { RunId, ThreadId, type OwnershipHandle } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";

import * as Accounting from "./CheckoutProcessAccounting.ts";

const handle: OwnershipHandle = {
  ownershipId: "claim",
  checkoutKey: "checkout",
  generation: 1,
  serverInstanceId: "server",
  operation: "provider",
  owner: { kind: "run", threadId: ThreadId.make("thread"), runId: RunId.make("run") },
};
const parent = { id: "parent", creationMarker: "parent-start", pid: 1 };
const child = { id: "child", creationMarker: "child-start", pid: 2 };

describe("checkout process accounting", () => {
  it.effect("does not merge owners whose identifiers contain delimiters", () =>
    Effect.gen(function* () {
      const accounting = yield* Accounting.CheckoutProcessAccounting;
      const first = {
        ...handle,
        owner: { kind: "run" as const, threadId: ThreadId.make("a:b"), runId: RunId.make("c") },
      };
      const other = {
        kind: "run" as const,
        threadId: ThreadId.make("a"),
        runId: RunId.make("b:c"),
      };
      yield* accounting.register({ handle: first, processRef: parent, kind: "provider" });
      yield* accounting.mark({ owner: first.owner, processRef: parent, state: "stopped" });
      assert.strictEqual((yield* accounting.observe(other)).state, "unknown");
    }).pipe(Effect.provide(Accounting.layer)),
  );
  it.effect("retains a live descendant after its parent finishes", () =>
    Effect.gen(function* () {
      const accounting = yield* Accounting.CheckoutProcessAccounting;
      yield* accounting.register({ handle, processRef: parent, kind: "provider" });
      yield* accounting.register({ handle, processRef: child, kind: "terminal" });
      yield* accounting.mark({ owner: handle.owner, processRef: parent, state: "stopped" });
      yield* accounting.mark({ owner: handle.owner, processRef: child, state: "live" });
      assert.strictEqual((yield* accounting.observe(handle.owner)).state, "live");
      yield* accounting.mark({ owner: handle.owner, processRef: child, state: "stopped" });
      assert.strictEqual((yield* accounting.observe(handle.owner)).state, "stopped");
    }).pipe(Effect.provide(Accounting.layer)),
  );

  it.effect("never treats untracked terminal activity or a reused PID as stopped", () =>
    Effect.gen(function* () {
      const accounting = yield* Accounting.CheckoutProcessAccounting;
      assert.strictEqual((yield* accounting.observe(handle.owner)).state, "unknown");
      yield* accounting.register({ handle, processRef: parent, kind: "terminal" });
      assert.strictEqual((yield* accounting.observe(handle.owner)).state, "unknown");
      const reused = yield* Effect.exit(
        accounting.mark({
          owner: handle.owner,
          processRef: { ...parent, creationMarker: "reused" },
          state: "stopped",
        }),
      );
      assert.isTrue(Exit.isFailure(reused));
      assert.strictEqual((yield* accounting.observe(handle.owner)).state, "unknown");
    }).pipe(Effect.provide(Accounting.layer)),
  );
});
