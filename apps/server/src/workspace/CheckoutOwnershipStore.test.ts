import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  RunId,
  ThreadId,
  type AcquireCheckoutOwnershipInput,
  type OwnershipHandle,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

import * as Sqlite from "../persistence/Sqlite.ts";
import * as Store from "./CheckoutOwnershipStore.ts";

const layer = Store.layer.pipe(Layer.provideMerge(Sqlite.layerMemory));
const input = (
  id: string,
): AcquireCheckoutOwnershipInput & { ownershipId: string; serverInstanceId: string } => ({
  commandId: CommandId.make(id),
  ownershipId: `ownership:${id}`,
  serverInstanceId: "server",
  identity: {
    environmentId: EnvironmentId.make("environment"),
    checkoutKey: "checkout",
    canonicalCwd: "/repo",
    filesystemIdentity: { device: "1", file: "2" },
    gitDir: "/repo/.git",
    gitCommonDir: "/repo/.git",
  },
  owner: { kind: "run", threadId: ThreadId.make(id), runId: RunId.make(id) },
  operation: "setup",
});
const acquire = Effect.fn("test.acquire")(function* (id: string) {
  const store = yield* Store.CheckoutOwnershipStore;
  const result = yield* store.acquire(input(id));
  assert.strictEqual(result._tag, "Acquired");
  if (result._tag !== "Acquired") throw new Error("expected admission");
  return result.handle;
});
const release = (handle: OwnershipHandle, id: string) =>
  Store.CheckoutOwnershipStore.use((store) =>
    store.transition({ type: "release", commandId: CommandId.make(id), handle }),
  );

describe("checkout ownership persistence", () => {
  it.effect("atomically admits exactly one simultaneous writer and reuses original receipts", () =>
    Effect.gen(function* () {
      const store = yield* Store.CheckoutOwnershipStore;
      const results = yield* Effect.all([store.acquire(input("a")), store.acquire(input("b"))], {
        concurrency: "unbounded",
      });
      assert.strictEqual(results.filter((r) => r._tag === "Acquired").length, 1);
      assert.deepStrictEqual(yield* store.acquire(input("a")), results[0]);
      const sql = yield* SqlClient.SqlClient;
      assert.strictEqual((yield* sql`SELECT * FROM orchestration_v2_checkout_events`).length, 1);
      assert.strictEqual((yield* sql`SELECT * FROM orchestration_command_receipts`).length, 2);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("retains fencing after release and refuses stale workers", () =>
    Effect.gen(function* () {
      const store = yield* Store.CheckoutOwnershipStore;
      const first = yield* acquire("a");
      yield* release(first, "release-a");
      const next = yield* acquire("b");
      assert.strictEqual(next.generation, first.generation + 1);
      const stale = yield* Effect.exit(release(first, "stale-release"));
      assert.isTrue(Exit.isFailure(stale));
      yield* release(first, "release-a");
      assert.strictEqual((yield* store.read("checkout"))?.handle.owner.threadId, "b");
    }).pipe(Effect.provide(layer)),
  );

  it.effect("rolls back claim and event when receipt persistence fails", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.CheckoutOwnershipStore;
      yield* sql`CREATE TRIGGER fail_workspace_receipt BEFORE INSERT ON orchestration_v2_workspace_receipts
      BEGIN SELECT RAISE(ABORT, 'receipt failure'); END`;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(store.acquire(input("rollback")))));
      assert.isNull(yield* store.read("checkout"));
      assert.strictEqual((yield* sql`SELECT * FROM orchestration_v2_checkout_events`).length, 0);
      assert.strictEqual((yield* sql`SELECT * FROM orchestration_command_receipts`).length, 0);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("keeps cancelled waiters cancelled and wakes the oldest eligible waiter", () =>
    Effect.gen(function* () {
      const store = yield* Store.CheckoutOwnershipStore;
      const owner = yield* acquire("owner");
      const cancelled = yield* store.enqueueWait({
        ...input("wait-cancelled"),
        waitId: "cancelled",
      });
      yield* store.enqueueWait({ ...input("wait-next"), waitId: "next" });
      yield* store.cancelWait({
        commandId: CommandId.make("cancel"),
        waitId: cancelled.waitId,
        threadId: cancelled.input.owner.threadId,
      });
      yield* release(owner, "release-owner");
      assert.strictEqual((yield* store.getWait("cancelled"))?.state, "cancelled");
      assert.strictEqual((yield* store.getWait("next"))?.state, "ready");
      const duplicate = yield* store.enqueueWait({
        ...input("wait-cancelled"),
        waitId: "cancelled",
      });
      assert.strictEqual(duplicate.waitId, "cancelled");
      assert.strictEqual((yield* store.getWait("cancelled"))?.state, "cancelled");
    }).pipe(Effect.provide(layer)),
  );

  it.effect("persists uncertain ownership rather than expiring it", () =>
    Effect.gen(function* () {
      const store = yield* Store.CheckoutOwnershipStore;
      const handle = yield* acquire("owner");
      yield* store.transition({
        type: "uncertain",
        commandId: CommandId.make("uncertain"),
        handle,
      });
      const records = yield* store.listRecoverable;
      assert.strictEqual(records[0]?.state, "uncertain");
      assert.strictEqual((yield* store.acquire(input("competitor")))._tag, "Conflict");
    }).pipe(Effect.provide(layer)),
  );
});
