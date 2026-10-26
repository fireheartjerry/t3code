import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  RunId,
  ThreadId,
  type CheckoutIdentity,
  type OwnershipHandle,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Identity from "./CheckoutIdentity.ts";
import * as Recovery from "./CheckoutOwnershipRecovery.ts";
import * as Ownership from "./CheckoutOwnershipService.ts";
import * as Store from "./CheckoutOwnershipStore.ts";
import * as Accounting from "./CheckoutProcessAccounting.ts";

const identity: CheckoutIdentity = {
  environmentId: EnvironmentId.make("environment"),
  checkoutKey: "checkout",
  canonicalCwd: "/repo",
  filesystemIdentity: { device: "1", file: "2" },
  gitDir: "/repo/.git",
  gitCommonDir: "/repo/.git",
};
const dependencies = Layer.mergeAll(
  Store.layer,
  Accounting.layer,
  Layer.succeed(ServerEnvironment.ServerEnvironmentIdentity, {
    getEnvironmentId: Effect.succeed(identity.environmentId),
  }),
  Layer.succeed(Identity.CheckoutIdentityResolver, {
    resolve: () => Effect.succeed(identity),
  }),
).pipe(Layer.provideMerge(Sqlite.layerMemory));

const fixture = Effect.gen(function* () {
  const store = yield* Store.CheckoutOwnershipStore;
  const accounting = yield* Accounting.CheckoutProcessAccounting;
  const sql = yield* SqlClient.SqlClient;
  const ownership = yield* Ownership.CheckoutOwnershipService.pipe(Effect.provide(Ownership.layer));
  const recovery = yield* Recovery.CheckoutOwnershipRecovery.pipe(
    Effect.provide(Recovery.layer),
    Effect.provideService(Ownership.CheckoutOwnershipService, ownership),
  );
  const acquire = Effect.fn("test.acquire")(function* (oldBoot = false) {
    const request = {
      commandId: CommandId.make("acquire-owner"),
      identity,
      owner: { kind: "run" as const, threadId: ThreadId.make("thread"), runId: RunId.make("run") },
      operation: "setup" as const,
    };
    const result = oldBoot
      ? yield* store.acquire({
          ...request,
          ownershipId: "old-claim",
          serverInstanceId: "previous-boot",
        })
      : yield* ownership.acquire(request);
    assert.strictEqual(result._tag, "Acquired");
    if (result._tag !== "Acquired") throw new Error("Expected checkout admission");
    return result.handle;
  });
  const mark = Effect.fn("test.mark")(function* (
    handle: OwnershipHandle,
    state: "live" | "stopped" | "unknown",
  ) {
    const processRef = { id: "process", creationMarker: "captured-creation", pid: 123 };
    yield* accounting.register({ handle, processRef, kind: "provider" });
    yield* accounting.mark({ handle, processRef, state });
  });
  return { store, sql, ownership, recovery, acquire, mark };
});

describe("checkout ownership recovery", () => {
  it.effect("fences an unknown current owner until accounting actually proves stopped", () =>
    Effect.gen(function* () {
      const { store, ownership, recovery, acquire, mark } = yield* fixture;
      const handle = yield* acquire();
      yield* recovery.reconcile();
      assert.strictEqual((yield* store.read(identity.checkoutKey))?.state, "uncertain");
      assert.strictEqual(
        (yield* ownership.assertCurrent(handle).pipe(Effect.flip))._tag,
        "StaleOwnershipHandle",
      );
      yield* mark(handle, "live");
      yield* recovery.reconcile();
      assert.strictEqual((yield* store.read(identity.checkoutKey))?.state, "uncertain");
      yield* mark(handle, "stopped");
      yield* recovery.reconcile();
      assert.strictEqual((yield* store.read(identity.checkoutKey))?.state, "released");
    }).pipe(Effect.provide(dependencies)),
  );

  it.effect("shares the start-boundary lock before observing an in-flight spawn", () =>
    Effect.gen(function* () {
      const { store, ownership, recovery, acquire, mark } = yield* fixture;
      const handle = yield* acquire();
      const entered = yield* Deferred.make<void>();
      const finish = yield* Deferred.make<void>();
      const spawn = yield* ownership
        .withCurrent(
          handle,
          Effect.gen(function* () {
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(finish);
            yield* mark(handle, "live");
          }),
        )
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(entered);
      const reconcile = yield* recovery
        .reconcile()
        .pipe(Effect.forkChild({ startImmediately: true }));
      assert.isUndefined(reconcile.pollUnsafe());
      yield* Deferred.succeed(finish, undefined);
      yield* Fiber.join(spawn);
      yield* Fiber.join(reconcile);
      assert.strictEqual((yield* store.read(identity.checkoutKey))?.state, "held");
      yield* ownership.assertCurrent(handle);
    }).pipe(Effect.provide(dependencies)),
  );

  it.effect("fences old-boot unknown owners and never admits a replacement on restart", () =>
    Effect.gen(function* () {
      const { store, ownership, recovery, acquire, sql } = yield* fixture;
      const handle = yield* acquire(true);
      yield* recovery.reconcile();
      yield* recovery.reconcile();
      const record = yield* store.read(identity.checkoutKey);
      assert.strictEqual(record?.state, "uncertain");
      assert.deepStrictEqual(record?.handle, handle);
      assert.strictEqual(
        (yield* ownership.assertCurrent(handle).pipe(Effect.flip))._tag,
        "StaleOwnershipHandle",
      );
      assert.strictEqual(
        (yield* ownership
          .releaseWhenQuiescent({ commandId: CommandId.make("old-worker-release"), handle })
          .pipe(Effect.flip))._tag,
        "StaleOwnershipHandle",
      );
      const competitor = yield* ownership.acquire({
        commandId: CommandId.make("competitor"),
        identity,
        owner: {
          kind: "run",
          threadId: ThreadId.make("competitor"),
          runId: RunId.make("competitor"),
        },
        operation: "provider",
      });
      assert.strictEqual(competitor._tag, "Conflict");
      assert.strictEqual(
        (yield* sql`SELECT * FROM orchestration_v2_checkout_events WHERE kind = 'uncertain'`)
          .length,
        1,
      );
      assert.strictEqual(
        (yield* sql`SELECT * FROM orchestration_v2_checkout_events WHERE kind = 'release'`).length,
        0,
      );
    }).pipe(Effect.provide(dependencies)),
  );

  it.effect(
    "retains a live current owner through reconciliation and fences a live prior boot",
    () =>
      Effect.gen(function* () {
        const { store, ownership, recovery, acquire, mark, sql } = yield* fixture;
        const handle = yield* acquire();
        yield* mark(handle, "live");
        yield* recovery.reconcile();
        yield* recovery.reconcile();
        assert.strictEqual((yield* store.read(identity.checkoutKey))?.state, "held");
        yield* ownership.assertCurrent(handle);
        // Construct a fresh service over the same durable claims and captured process observations.
        const restarted = yield* Ownership.CheckoutOwnershipService.pipe(
          Effect.provide(Ownership.layer),
        );
        assert.notStrictEqual(restarted.serverInstanceId, ownership.serverInstanceId);
        const afterRestart = yield* Recovery.CheckoutOwnershipRecovery.pipe(
          Effect.provide(Recovery.layer),
          Effect.provideService(Ownership.CheckoutOwnershipService, restarted),
        );
        yield* afterRestart.reconcile();
        assert.strictEqual((yield* store.read(identity.checkoutKey))?.state, "uncertain");
        assert.strictEqual(
          (yield* restarted.assertCurrent(handle).pipe(Effect.flip))._tag,
          "StaleOwnershipHandle",
        );
        assert.strictEqual(
          (yield* ownership.assertCurrent(handle).pipe(Effect.flip))._tag,
          "StaleOwnershipHandle",
        );
        assert.strictEqual(
          (yield* sql`SELECT * FROM orchestration_v2_checkout_events WHERE kind = 'release'`)
            .length,
          0,
        );
      }).pipe(Effect.provide(dependencies)),
  );

  it.effect(
    "releases once after a confirmed old-owner stop and keeps cancelled waiters cancelled",
    () =>
      Effect.gen(function* () {
        const { store, ownership, recovery, acquire, mark, sql } = yield* fixture;
        const handle = yield* acquire(true);
        const waitInput = {
          commandId: CommandId.make("wait-cancelled"),
          identity,
          owner: {
            kind: "run" as const,
            threadId: ThreadId.make("waiter"),
            runId: RunId.make("waiter"),
          },
          operation: "setup" as const,
        };
        const cancelled = yield* ownership.enqueueWait(waitInput);
        const next = yield* ownership.enqueueWait({
          ...waitInput,
          commandId: CommandId.make("wait-next"),
        });
        yield* ownership.cancelWait({
          commandId: CommandId.make("cancel-wait"),
          waitId: cancelled.waitId,
          threadId: waitInput.owner.threadId,
        });
        yield* recovery.reconcile();
        assert.strictEqual((yield* store.read(identity.checkoutKey))?.state, "uncertain");
        yield* mark(handle, "stopped");
        yield* recovery.reconcile();
        yield* recovery.reconcile();
        assert.strictEqual((yield* store.read(identity.checkoutKey))?.state, "released");
        assert.strictEqual((yield* store.getWait(cancelled.waitId))?.state, "cancelled");
        assert.strictEqual((yield* store.getWait(next.waitId))?.state, "ready");
        assert.strictEqual(
          (yield* sql`SELECT * FROM orchestration_v2_checkout_events WHERE kind = 'release'`)
            .length,
          1,
        );
      }).pipe(Effect.provide(dependencies)),
  );

  it.effect(
    "keeps failed setup owned while a writer is alive and releases after its observed exit",
    () =>
      Effect.gen(function* () {
        const { store, recovery, acquire, mark } = yield* fixture;
        const handle = yield* acquire();
        yield* mark(handle, "live");
        yield* recovery.reconcile();
        assert.strictEqual((yield* store.read(identity.checkoutKey))?.state, "held");
        yield* mark(handle, "stopped");
        yield* recovery.reconcile();
        assert.strictEqual((yield* store.read(identity.checkoutKey))?.state, "released");
      }).pipe(Effect.provide(dependencies)),
  );

  it.effect("fences changed physical identity rather than releasing or adopting it", () =>
    Effect.gen(function* () {
      const { store, ownership, acquire, mark } = yield* fixture;
      const handle = yield* acquire();
      yield* mark(handle, "stopped");
      const changed = yield* Ownership.CheckoutOwnershipService.pipe(
        Effect.provide(Ownership.layer),
        Effect.provideService(Identity.CheckoutIdentityResolver, {
          resolve: () =>
            Effect.succeed({
              ...identity,
              filesystemIdentity: { device: "1", file: "replacement" },
            }),
        }),
      );
      const recovery = yield* Recovery.CheckoutOwnershipRecovery.pipe(
        Effect.provide(Recovery.layer),
        Effect.provideService(Ownership.CheckoutOwnershipService, changed),
      );
      yield* recovery.reconcile();
      assert.strictEqual((yield* store.read(identity.checkoutKey))?.state, "uncertain");
      assert.strictEqual(
        (yield* ownership.assertCurrent(handle).pipe(Effect.flip))._tag,
        "StaleOwnershipHandle",
      );
    }).pipe(Effect.provide(dependencies)),
  );

  it.effect("ignores a recovery snapshot superseded by a committed transfer", () =>
    Effect.gen(function* () {
      const { store, ownership, acquire, mark } = yield* fixture;
      const handle = yield* acquire();
      yield* mark(handle, "stopped");
      const records = yield* store.listRecoverable;
      const snapshotTaken = yield* Deferred.make<void>();
      const continueRecovery = yield* Deferred.make<void>();
      const recovery = yield* Recovery.CheckoutOwnershipRecovery.pipe(
        Effect.provide(Recovery.layer),
        Effect.provideService(Ownership.CheckoutOwnershipService, ownership),
        Effect.provideService(Store.CheckoutOwnershipStore, {
          ...store,
          listRecoverable: Deferred.succeed(snapshotTaken, undefined).pipe(
            Effect.andThen(Deferred.await(continueRecovery)),
            Effect.as(records),
          ),
        }),
      );
      const reconciling = yield* recovery
        .reconcile()
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(snapshotTaken);
      const next = yield* ownership.transfer({
        commandId: CommandId.make("transfer"),
        handle,
        nextOwner: {
          kind: "verification",
          threadId: handle.owner.threadId,
          verificationId: "verify",
        },
        nextOperation: "verification",
      });
      yield* Deferred.succeed(continueRecovery, undefined);
      yield* Fiber.join(reconciling);
      assert.strictEqual((yield* store.read(identity.checkoutKey))?.state, "held");
      assert.deepStrictEqual((yield* store.read(identity.checkoutKey))?.handle, next);
      yield* ownership.assertCurrent(next);
    }).pipe(Effect.provide(dependencies)),
  );

  it.effect("wraps storage failures as typed recovery errors without changing ownership", () =>
    Effect.gen(function* () {
      const { store, recovery, acquire, sql } = yield* fixture;
      const handle = yield* acquire(true);
      yield* sql`CREATE TRIGGER reject_recovery BEFORE UPDATE ON orchestration_v2_checkout_claims
        BEGIN SELECT RAISE(ABORT, 'recovery rejected'); END`;
      const error = yield* recovery.reconcile().pipe(Effect.flip);
      assert.strictEqual(error._tag, "CheckoutOwnershipRecoveryError");
      assert.instanceOf(error.cause, Store.CheckoutOwnershipStoreError);
      assert.deepStrictEqual((yield* store.read(identity.checkoutKey))?.handle, handle);
      assert.strictEqual((yield* store.read(identity.checkoutKey))?.state, "held");
    }).pipe(Effect.provide(dependencies)),
  );
});
