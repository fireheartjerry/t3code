import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  RunId,
  ThreadId,
  type AcquireCheckoutOwnershipInput,
  type CheckoutIdentity,
  type OwnershipHandle,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as SqlClient from "effect/sql/SqlClient";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Identity from "./CheckoutIdentity.ts";
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
const input = (id: string): AcquireCheckoutOwnershipInput => ({
  commandId: CommandId.make(id),
  identity,
  owner: { kind: "run", threadId: ThreadId.make(id), runId: RunId.make(id) },
  operation: "setup",
});
const dependencies = Layer.mergeAll(
  Store.layer,
  Accounting.layer,
  Layer.succeed(ServerEnvironment.ServerEnvironmentIdentity, {
    getEnvironmentId: Effect.succeed(identity.environmentId),
  }),
).pipe(Layer.provideMerge(Sqlite.layerMemory));

const fixture = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const physical = yield* Ref.make(identity);
  const notifications = yield* Ref.make(0);
  const store = yield* Store.CheckoutOwnershipStore;
  const resolver = Identity.CheckoutIdentityResolver.of({
    resolve: () =>
      Effect.gen(function* () {
        assert.isTrue(Option.isNone(yield* Effect.serviceOption(sql.transactionService)));
        return yield* Ref.get(physical);
      }),
  });
  const accounting = yield* Accounting.CheckoutProcessAccounting;
  const service = yield* Ownership.CheckoutOwnershipService.pipe(
    Effect.provide(Ownership.layer),
    Effect.provideService(Identity.CheckoutIdentityResolver, resolver),
    Effect.provideService(Store.CheckoutOwnershipStore, {
      ...store,
      notify: (key) =>
        Effect.gen(function* () {
          assert.isTrue(Option.isNone(yield* Effect.serviceOption(sql.transactionService)));
          yield* Ref.update(notifications, (count) => count + 1);
          yield* store.notify(key);
        }),
    }),
    Effect.provideService(Accounting.CheckoutProcessAccounting, {
      ...accounting,
      observe: (owner) =>
        Effect.gen(function* () {
          assert.isTrue(Option.isNone(yield* Effect.serviceOption(sql.transactionService)));
          return yield* accounting.observe(owner);
        }),
    }),
  );
  const acquireWithIntent = service.acquireWithIntent;
  if (acquireWithIntent === undefined)
    throw new Error("Production acquisition intent hook missing");
  const acquire = Effect.fn("test.acquire")(function* (id: string) {
    const result = yield* service.acquire(input(id));
    assert.strictEqual(result._tag, "Acquired");
    if (result._tag !== "Acquired") throw new Error("Expected checkout admission");
    return result.handle;
  });
  const mark = Effect.fn("test.mark")(function* (
    handle: OwnershipHandle,
    state: "live" | "stopped" | "unknown",
  ) {
    const processRef = { id: handle.ownershipId, creationMarker: "created-once", pid: 123 };
    yield* accounting.register({ handle, processRef, kind: "provider" });
    yield* accounting.mark({ handle, processRef, state });
  });
  return { service, store, sql, physical, acquire, acquireWithIntent, mark, notifications };
});

describe("checkout ownership service", () => {
  it.effect("never reuses a prior generation's stopped proof for release or transfer", () =>
    Effect.gen(function* () {
      const { service, acquire, mark } = yield* fixture;
      const old = yield* acquire("generation-proof");
      yield* mark(old, "stopped");
      const next = yield* service.advanceOperation({
        commandId: CommandId.make("next-phase"),
        handle: old,
        nextOperation: "finalization",
      });
      assert.strictEqual(
        yield* service.releaseWhenQuiescent({
          commandId: CommandId.make("new-release"),
          handle: next,
        }),
        "uncertain",
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            service.transfer({
              commandId: CommandId.make("new-transfer"),
              handle: next,
              nextOwner: {
                kind: "run",
                threadId: ThreadId.make("next-thread"),
                runId: RunId.make("next-run"),
              },
              nextOperation: "provider",
            }),
          ),
        ),
      );
    }).pipe(Effect.provide(dependencies)),
  );
  it.effect(
    "rolls back an explicit verification acquisition and intent without a claim, event or receipt",
    () =>
      Effect.gen(function* () {
        const { acquireWithIntent, store, sql, notifications } = yield* fixture;
        yield* sql`CREATE TABLE explicit_verification_intents (ownership_id TEXT, generation INTEGER)`;
        const request = {
          ...input("explicit-verification"),
          operation: "verification" as const,
          owner: {
            kind: "verification" as const,
            threadId: ThreadId.make("thread"),
            verificationId: "verification",
          },
        };
        const failure = yield* acquireWithIntent({
          request,
          persistIntent: (handle) =>
            Effect.gen(function* () {
              assert.isTrue(Option.isSome(yield* Effect.serviceOption(sql.transactionService)));
              yield* sql`INSERT INTO explicit_verification_intents VALUES (${handle.ownershipId}, ${handle.generation})`;
              return yield* Effect.fail("intent-failed" as const);
            }),
        }).pipe(Effect.flip);
        assert.strictEqual(failure, "intent-failed");
        assert.isNull(yield* store.read(identity.checkoutKey));
        assert.strictEqual((yield* sql`SELECT * FROM explicit_verification_intents`).length, 0);
        assert.strictEqual((yield* sql`SELECT * FROM orchestration_v2_checkout_events`).length, 0);
        assert.strictEqual(
          (yield* sql`SELECT * FROM orchestration_v2_workspace_receipts`).length,
          0,
        );
        assert.strictEqual((yield* sql`SELECT * FROM orchestration_command_receipts`).length, 0);
        assert.strictEqual(yield* Ref.get(notifications), 0);
        // The same command remains eligible after rollback; it has no successful receipt to replay.
        const admitted = yield* acquireWithIntent({
          request,
          persistIntent: (handle) =>
            sql`INSERT INTO explicit_verification_intents VALUES (${handle.ownershipId}, ${handle.generation})`,
        });
        assert.strictEqual(admitted._tag, "Acquired");
        assert.strictEqual((yield* sql`SELECT * FROM explicit_verification_intents`).length, 1);
        assert.strictEqual(yield* Ref.get(notifications), 1);
      }).pipe(Effect.provide(dependencies)),
  );

  it.effect(
    "replays simultaneous and later explicit verification requests without persisting their intent again",
    () =>
      Effect.gen(function* () {
        const { acquireWithIntent, sql, notifications } = yield* fixture;
        yield* sql`CREATE TABLE explicit_verification_intents (ownership_id TEXT PRIMARY KEY, generation INTEGER)`;
        const request = input("duplicate-verification");
        const persistIntent = (handle: OwnershipHandle) =>
          Effect.gen(function* () {
            assert.isTrue(Option.isSome(yield* Effect.serviceOption(sql.transactionService)));
            yield* sql`INSERT INTO explicit_verification_intents VALUES (${handle.ownershipId}, ${handle.generation})`;
          });
        const [first, duplicate] = yield* Effect.all(
          [
            acquireWithIntent({ request, persistIntent }),
            acquireWithIntent({ request, persistIntent }),
          ],
          { concurrency: "unbounded" },
        );
        assert.strictEqual(first._tag, "Acquired");
        assert.deepStrictEqual(duplicate, first);
        assert.deepStrictEqual(
          yield* acquireWithIntent({
            request,
            persistIntent: () => Effect.die("Acquisition intent replayed"),
          }),
          first,
        );
        assert.strictEqual((yield* sql`SELECT * FROM explicit_verification_intents`).length, 1);
        assert.strictEqual((yield* sql`SELECT * FROM orchestration_v2_checkout_events`).length, 1);
        assert.strictEqual(
          (yield* sql`SELECT * FROM orchestration_v2_workspace_receipts`).length,
          1,
        );
        assert.strictEqual((yield* sql`SELECT * FROM orchestration_command_receipts`).length, 1);
        assert.strictEqual(yield* Ref.get(notifications), 1);
      }).pipe(Effect.provide(dependencies)),
  );

  it.effect(
    "persists exactly one intent for simultaneous competing requests and replays the conflict normally",
    () =>
      Effect.gen(function* () {
        const { acquireWithIntent, sql, notifications, service, mark } = yield* fixture;
        yield* sql`CREATE TABLE explicit_verification_intents (ownership_id TEXT PRIMARY KEY)`;
        const requests = [input("verification-a"), input("verification-b")];
        const results = yield* Effect.all(
          requests.map((request) =>
            acquireWithIntent({
              request,
              persistIntent: (handle) =>
                sql`INSERT INTO explicit_verification_intents VALUES (${handle.ownershipId})`,
            }),
          ),
          { concurrency: "unbounded" },
        );
        assert.strictEqual(results.filter((result) => result._tag === "Acquired").length, 1);
        assert.strictEqual(results.filter((result) => result._tag === "Conflict").length, 1);
        assert.strictEqual((yield* sql`SELECT * FROM explicit_verification_intents`).length, 1);
        assert.strictEqual((yield* sql`SELECT * FROM orchestration_v2_checkout_events`).length, 1);
        assert.strictEqual(
          (yield* sql`SELECT * FROM orchestration_v2_workspace_receipts`).length,
          2,
        );
        assert.strictEqual(yield* Ref.get(notifications), 1);
        const winner = results.find((result) => result._tag === "Acquired");
        if (winner?._tag !== "Acquired") throw new Error("Expected one admission");
        yield* mark(winner.handle, "stopped");
        yield* service.releaseWhenQuiescent({
          commandId: CommandId.make("release-winner"),
          handle: winner.handle,
        });
        for (const [index, request] of requests.entries()) {
          assert.deepStrictEqual(
            yield* acquireWithIntent({
              request,
              persistIntent: () =>
                Effect.die("Persisted acquisition or conflict replayed its intent"),
            }),
            results[index],
          );
        }
        assert.strictEqual((yield* sql`SELECT * FROM explicit_verification_intents`).length, 1);
        assert.strictEqual(yield* Ref.get(notifications), 1);
      }).pipe(Effect.provide(dependencies)),
  );

  it.effect("rejects a command-type collision before acquisition or intent persistence", () =>
    Effect.gen(function* () {
      const { acquireWithIntent, store, service, sql, notifications } = yield* fixture;
      const request = input("existing-wait-command");
      const waiter = yield* service.enqueueWait(request);
      const error = yield* acquireWithIntent({
        request,
        persistIntent: () => Effect.die("Colliding command persisted an intent"),
      }).pipe(Effect.flip);
      assert.strictEqual(error._tag, "CheckoutOwnershipServiceError");
      if (error._tag === "CheckoutOwnershipServiceError")
        assert.strictEqual(error.reason, "command-mismatch");
      assert.isNull(yield* store.read(identity.checkoutKey));
      assert.strictEqual((yield* sql`SELECT * FROM orchestration_v2_checkout_events`).length, 0);
      assert.strictEqual((yield* sql`SELECT * FROM orchestration_v2_workspace_receipts`).length, 1);
      assert.deepStrictEqual(yield* service.enqueueWait(request), waiter);
      assert.strictEqual(yield* Ref.get(notifications), 0);
    }).pipe(Effect.provide(dependencies)),
  );

  it.effect("admits independent worktrees and keeps their start boundaries independent", () =>
    Effect.gen(function* () {
      const other: CheckoutIdentity = {
        ...identity,
        checkoutKey: "other-checkout",
        canonicalCwd: "/other-worktree",
        filesystemIdentity: { device: "1", file: "3" },
        gitDir: "/repo/.git/worktrees/other",
      };
      const service = yield* Ownership.CheckoutOwnershipService.pipe(
        Effect.provide(Ownership.layer),
        Effect.provideService(Identity.CheckoutIdentityResolver, {
          resolve: ({ cwd }) => Effect.succeed(cwd === other.canonicalCwd ? other : identity),
        }),
      );
      const [first, second] = yield* Effect.all(
        [service.acquire(input("first")), service.acquire({ ...input("second"), identity: other })],
        { concurrency: "unbounded" },
      );
      if (first._tag !== "Acquired" || second._tag !== "Acquired")
        throw new Error("Expected independent admissions");
      const entered = yield* Deferred.make<void>();
      const finish = yield* Deferred.make<void>();
      const spawn = yield* service
        .withCurrent(
          first.handle,
          Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(finish))),
        )
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(entered);
      yield* service.assertCurrent(second.handle);
      yield* Deferred.succeed(finish, undefined);
      yield* Fiber.join(spawn);
    }).pipe(Effect.provide(dependencies)),
  );

  it.effect(
    "admits exactly one simultaneous service writer and replays the original outcomes",
    () =>
      Effect.gen(function* () {
        const { service, sql } = yield* fixture;
        const results = yield* Effect.all(
          [service.acquire(input("a")), service.acquire(input("b"))],
          {
            concurrency: "unbounded",
          },
        );
        assert.strictEqual(results.filter((result) => result._tag === "Acquired").length, 1);
        assert.strictEqual(results.filter((result) => result._tag === "Conflict").length, 1);
        assert.deepStrictEqual(yield* service.acquire(input("a")), results[0]);
        assert.deepStrictEqual(yield* service.acquire(input("b")), results[1]);
        assert.match(service.serverInstanceId, /^[0-9a-f-]{36}$/);
        assert.strictEqual((yield* sql`SELECT * FROM orchestration_v2_checkout_events`).length, 1);
      }).pipe(Effect.provide(dependencies)),
  );

  it.effect("rejects a foreign environment and forged physical identity before admission", () =>
    Effect.gen(function* () {
      const { service, sql } = yield* fixture;
      const foreign = { ...identity, environmentId: EnvironmentId.make("other-environment") };
      const acquireError = yield* service
        .acquire({ ...input("foreign"), identity: foreign })
        .pipe(Effect.flip);
      assert.strictEqual(acquireError._tag, "CheckoutOwnershipServiceError");
      if (acquireError._tag === "CheckoutOwnershipServiceError")
        assert.strictEqual(acquireError.reason, "environment-mismatch");
      assert.strictEqual(
        (yield* service.inspect({ identity: foreign }).pipe(Effect.flip))._tag,
        "CheckoutOwnershipServiceError",
      );
      assert.strictEqual(
        (yield* service
          .enqueueWait({ ...input("foreign-wait"), identity: foreign })
          .pipe(Effect.flip))._tag,
        "CheckoutOwnershipServiceError",
      );
      const forged = { ...identity, filesystemIdentity: { device: "1", file: "999" } };
      assert.strictEqual(
        (yield* service.acquire({ ...input("forged"), identity: forged }).pipe(Effect.flip))._tag,
        "CheckoutOwnershipServiceError",
      );
      assert.strictEqual((yield* sql`SELECT * FROM orchestration_v2_checkout_claims`).length, 0);
      assert.strictEqual((yield* sql`SELECT * FROM orchestration_v2_workspace_receipts`).length, 0);
    }).pipe(Effect.provide(dependencies)),
  );

  it.effect(
    "rejects changed boot, owner, operation and physical path at the final start boundary",
    () =>
      Effect.gen(function* () {
        const { service, acquire, physical } = yield* fixture;
        const handle = yield* acquire("owner:run");
        const starts = yield* Ref.make(0);
        const start = Ref.update(starts, (count) => count + 1);
        for (const invalid of [
          { ...handle, serverInstanceId: "previous-boot" },
          { ...handle, generation: handle.generation + 1 },
          { ...handle, owner: input("other-owner").owner },
          {
            ...handle,
            owner: {
              kind: "run" as const,
              threadId: ThreadId.make("owner"),
              runId: RunId.make("run:owner:run"),
            },
          },
          { ...handle, operation: "terminal" as const },
        ]) {
          assert.strictEqual(
            (yield* service.withCurrent(invalid, start).pipe(Effect.flip))._tag,
            "StaleOwnershipHandle",
          );
        }
        for (const changed of [
          { ...identity, canonicalCwd: "/moved-repo" },
          { ...identity, filesystemIdentity: { device: "1", file: "new-inode" } },
          { ...identity, checkoutKey: "replacement-checkout" },
          { ...identity, gitDir: "/other/.git" },
        ]) {
          yield* Ref.set(physical, changed);
          assert.strictEqual(
            (yield* service.withCurrent(handle, start).pipe(Effect.flip))._tag,
            "CheckoutOwnershipServiceError",
          );
        }
        assert.strictEqual(yield* Ref.get(starts), 0);
        yield* Ref.set(physical, identity);
        yield* service.assertCurrent(handle);
        assert.strictEqual(
          yield* service.withCurrent(handle, Effect.succeed("spawned")),
          "spawned",
        );
        assert.strictEqual(
          yield* service.withCurrent(handle, Effect.fail("spawn-error")).pipe(Effect.flip),
          "spawn-error",
        );
      }).pipe(Effect.provide(dependencies)),
  );

  it.effect(
    "advances a same-owner operation fence and prevents the old worker starting or releasing its successor",
    () =>
      Effect.gen(function* () {
        const { service, acquire, store, mark } = yield* fixture;
        const first = yield* acquire("owner");
        const command = {
          commandId: CommandId.make("advance"),
          handle: first,
          nextOperation: "provider" as const,
        };
        const next = yield* service.advanceOperation(command);
        assert.strictEqual(next.generation, first.generation + 1);
        assert.deepStrictEqual(next.owner, first.owner);
        assert.deepStrictEqual(yield* service.advanceOperation(command), next);
        let starts = 0;
        assert.strictEqual(
          (yield* service
            .withCurrent(
              first,
              Effect.sync(() => starts++),
            )
            .pipe(Effect.flip))._tag,
          "StaleOwnershipHandle",
        );
        yield* mark(next, "stopped");
        assert.strictEqual(
          (yield* service
            .releaseWhenQuiescent({ commandId: CommandId.make("stale-release"), handle: first })
            .pipe(Effect.flip))._tag,
          "StaleOwnershipHandle",
        );
        assert.strictEqual(starts, 0);
        assert.deepStrictEqual((yield* store.read(identity.checkoutKey))?.handle, next);
        yield* service.assertCurrent(next);
      }).pipe(Effect.provide(dependencies)),
  );

  it.effect(
    "keeps live owners held, fences unknown owners, and releases only after confirmed stop",
    () =>
      Effect.gen(function* () {
        const { service, acquire, mark, store, sql } = yield* fixture;
        const handle = yield* acquire("owner");
        yield* mark(handle, "live");
        assert.strictEqual(
          yield* service.releaseWhenQuiescent({
            commandId: CommandId.make("live-release"),
            handle,
          }),
          "uncertain",
        );
        assert.strictEqual((yield* store.read(identity.checkoutKey))?.state, "held");
        yield* mark(handle, "unknown");
        const unknownRelease = { commandId: CommandId.make("unknown-release"), handle };
        assert.strictEqual(yield* service.releaseWhenQuiescent(unknownRelease), "uncertain");
        assert.strictEqual((yield* store.read(identity.checkoutKey))?.state, "uncertain");
        assert.strictEqual(
          (yield* service.assertCurrent(handle).pipe(Effect.flip))._tag,
          "StaleOwnershipHandle",
        );
        assert.strictEqual((yield* service.acquire(input("competitor")))._tag, "Conflict");
        yield* mark(handle, "stopped");
        assert.strictEqual(yield* service.releaseWhenQuiescent(unknownRelease), "uncertain");
        const release = { commandId: CommandId.make("confirmed-release"), handle };
        assert.strictEqual(yield* service.releaseWhenQuiescent(release), "released");
        assert.strictEqual(yield* service.releaseWhenQuiescent(release), "released");
        assert.strictEqual(
          (yield* sql`SELECT * FROM orchestration_v2_checkout_events WHERE kind = 'release'`)
            .length,
          1,
        );
        const successor = yield* acquire("successor");
        assert.strictEqual(successor.generation, handle.generation + 1);
        assert.strictEqual(
          (yield* service
            .releaseWhenQuiescent({ commandId: CommandId.make("late-release"), handle })
            .pipe(Effect.flip))._tag,
          "StaleOwnershipHandle",
        );
        assert.deepStrictEqual((yield* store.read(identity.checkoutKey))?.handle, successor);
      }).pipe(Effect.provide(dependencies)),
  );

  it.effect(
    "cannot transfer a live or unknown owner and never gives its intent callback a turn",
    () =>
      Effect.gen(function* () {
        const { service, acquire, mark, store } = yield* fixture;
        const handle = yield* acquire("owner");
        let intents = 0;
        for (const state of ["live", "unknown"] as const) {
          yield* mark(handle, state);
          const error = yield* service
            .transferWithIntent({
              commandId: CommandId.make(`transfer-${state}`),
              handle,
              nextOwner: input("successor").owner,
              nextOperation: "provider",
              persistIntent: () => Effect.sync(() => intents++),
            })
            .pipe(Effect.flip);
          assert.strictEqual(error._tag, "CheckoutOwnershipServiceError");
          if (error._tag === "CheckoutOwnershipServiceError")
            assert.strictEqual(error.reason, state === "live" ? "owner-live" : "owner-unknown");
          const record = yield* store.read(identity.checkoutKey);
          assert.deepStrictEqual(record?.handle.owner, handle.owner);
          assert.strictEqual(record?.state, state === "live" ? "held" : "uncertain");
        }
        assert.strictEqual(intents, 0);
        yield* mark(handle, "stopped");
        const next = yield* service.transfer({
          commandId: CommandId.make("transfer-stopped"),
          handle,
          nextOwner: input("successor").owner,
          nextOperation: "provider",
        });
        assert.strictEqual(next.generation, handle.generation + 1);
        yield* service.assertCurrent(next);
      }).pipe(Effect.provide(dependencies)),
  );

  it.effect(
    "serializes the final spawn effect with transfer and observes the newly registered live process",
    () =>
      Effect.gen(function* () {
        const { service, acquire, mark, store } = yield* fixture;
        const handle = yield* acquire("owner");
        yield* mark(handle, "stopped");
        const entered = yield* Deferred.make<void>();
        const finishSpawn = yield* Deferred.make<void>();
        const spawning = yield* service
          .withCurrent(
            handle,
            Effect.gen(function* () {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(finishSpawn);
              yield* mark(handle, "live");
            }),
          )
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(entered);
        const transfer = yield* service
          .transfer({
            commandId: CommandId.make("racing-transfer"),
            handle,
            nextOwner: input("successor").owner,
            nextOperation: "provider",
          })
          .pipe(Effect.flip, Effect.forkChild({ startImmediately: true }));
        assert.isUndefined(transfer.pollUnsafe());
        yield* Deferred.succeed(finishSpawn, undefined);
        yield* Fiber.join(spawning);
        const error = yield* Fiber.join(transfer);
        assert.strictEqual(error._tag, "CheckoutOwnershipServiceError");
        if (error._tag === "CheckoutOwnershipServiceError")
          assert.strictEqual(error.reason, "owner-live");
        assert.deepStrictEqual((yield* store.read(identity.checkoutKey))?.handle, handle);
      }).pipe(Effect.provide(dependencies)),
  );

  it.effect("rechecks a queued spawn after transfer commits under the same checkout lock", () =>
    Effect.gen(function* () {
      const { service, acquire, mark } = yield* fixture;
      const handle = yield* acquire("owner");
      yield* mark(handle, "stopped");
      const entered = yield* Deferred.make<void>();
      const finish = yield* Deferred.make<void>();
      const transfer = yield* service
        .transferWithIntent({
          commandId: CommandId.make("first-transfer"),
          handle,
          nextOwner: input("successor").owner,
          nextOperation: "provider",
          // The gate instruments the transaction boundary; production intent callbacks only write SQL.
          persistIntent: () =>
            Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(finish))),
        })
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(entered);
      let starts = 0;
      const spawn = yield* service
        .withCurrent(
          handle,
          Effect.sync(() => starts++),
        )
        .pipe(Effect.flip, Effect.forkChild({ startImmediately: true }));
      assert.isUndefined(spawn.pollUnsafe());
      yield* Deferred.succeed(finish, undefined);
      const next = yield* Fiber.join(transfer);
      assert.strictEqual((yield* Fiber.join(spawn))._tag, "StaleOwnershipHandle");
      assert.strictEqual(starts, 0);
      yield* service.assertCurrent(next);
    }).pipe(Effect.provide(dependencies)),
  );

  it.effect(
    "rolls back a verification handoff and intent together and notifies only after a successful commit",
    () =>
      Effect.gen(function* () {
        const { service, acquire, mark, store, sql, notifications } = yield* fixture;
        const handle = yield* acquire("owner");
        yield* mark(handle, "stopped");
        yield* sql`CREATE TABLE verification_intents (ownership_id TEXT, generation INTEGER, owner TEXT)`;
        const handoff = {
          commandId: CommandId.make("verification-handoff"),
          handle,
          nextOwner: {
            kind: "verification" as const,
            threadId: handle.owner.threadId,
            verificationId: "verification",
          },
          nextOperation: "verification" as const,
        };
        const persist = Effect.fn("test.persistIntent")(function* (next: OwnershipHandle) {
          assert.isTrue(Option.isSome(yield* Effect.serviceOption(sql.transactionService)));
          yield* sql`INSERT INTO verification_intents VALUES (${next.ownershipId}, ${next.generation}, ${Store.ownerKey(next.owner)})`;
        });
        const failure = yield* service
          .transferWithIntent({
            ...handoff,
            persistIntent: (next) =>
              persist(next).pipe(Effect.andThen(Effect.fail("intent-failed" as const))),
          })
          .pipe(Effect.flip);
        assert.strictEqual(failure, "intent-failed");
        assert.deepStrictEqual((yield* store.read(identity.checkoutKey))?.handle, handle);
        assert.strictEqual((yield* store.read(identity.checkoutKey))?.state, "held");
        assert.strictEqual((yield* sql`SELECT * FROM verification_intents`).length, 0);
        assert.strictEqual(
          (yield* sql`SELECT * FROM orchestration_v2_checkout_events WHERE kind = 'transfer'`)
            .length,
          0,
        );
        assert.strictEqual(
          (yield* sql`SELECT * FROM orchestration_v2_workspace_receipts WHERE command_id = ${handoff.commandId}`)
            .length,
          0,
        );
        assert.strictEqual(
          (yield* sql`SELECT * FROM orchestration_command_receipts WHERE command_id = ${handoff.commandId}`)
            .length,
          0,
        );
        assert.strictEqual(yield* Ref.get(notifications), 0);
        yield* service.assertCurrent(handle);
        const next = yield* service.transferWithIntent({ ...handoff, persistIntent: persist });
        assert.strictEqual(next.generation, handle.generation + 1);
        assert.deepStrictEqual(next.owner, handoff.nextOwner);
        assert.deepStrictEqual(
          yield* service.transferWithIntent({
            ...handoff,
            persistIntent: () => Effect.die("Intent replayed"),
          }),
          next,
        );
        assert.strictEqual((yield* sql`SELECT * FROM verification_intents`).length, 1);
        assert.strictEqual(
          (yield* sql`SELECT * FROM orchestration_v2_checkout_events WHERE kind = 'transfer'`)
            .length,
          1,
        );
        assert.strictEqual(yield* Ref.get(notifications), 1);
        assert.strictEqual(
          (yield* service.assertCurrent(handle).pipe(Effect.flip))._tag,
          "StaleOwnershipHandle",
        );
        assert.strictEqual(
          (yield* service
            .releaseWhenQuiescent({ commandId: CommandId.make("late-original-release"), handle })
            .pipe(Effect.flip))._tag,
          "StaleOwnershipHandle",
        );
        assert.deepStrictEqual((yield* store.read(identity.checkoutKey))?.handle, next);
      }).pipe(Effect.provide(dependencies)),
  );

  it.effect("rolls back SQL failures in the handoff callback without publishing a successor", () =>
    Effect.gen(function* () {
      const { service, acquire, mark, store, sql, notifications } = yield* fixture;
      const handle = yield* acquire("owner");
      yield* mark(handle, "stopped");
      yield* sql`CREATE TABLE rejected_intents (id INTEGER CHECK(id > 0))`;
      const error = yield* service
        .transferWithIntent({
          commandId: CommandId.make("sql-failure"),
          handle,
          nextOwner: input("successor").owner,
          nextOperation: "provider",
          persistIntent: () => sql`INSERT INTO rejected_intents VALUES (-1)`,
        })
        .pipe(Effect.flip);
      assert.strictEqual(error._tag, "CheckoutOwnershipStoreError");
      assert.deepStrictEqual((yield* store.read(identity.checkoutKey))?.handle, handle);
      assert.strictEqual(yield* Ref.get(notifications), 0);
    }).pipe(Effect.provide(dependencies)),
  );

  it.effect(
    "persists cancellable waits, authorizes cancellation by thread, and wakes only the next eligible waiter",
    () =>
      Effect.gen(function* () {
        const { service, acquire, mark, store } = yield* fixture;
        assert.isNull(yield* service.inspect({ identity }));
        const handle = yield* acquire("owner");
        const first = yield* service.enqueueWait(input("wait-first"));
        const second = yield* service.enqueueWait(input("wait-second"));
        assert.strictEqual(first.state, "waiting");
        assert.isBelow(first.sequence, second.sequence);
        assert.deepStrictEqual(yield* service.enqueueWait(input("wait-first")), first);
        assert.isNull(
          yield* service.cancelWait({
            commandId: CommandId.make("foreign-cancel"),
            waitId: first.waitId,
            threadId: ThreadId.make("foreign-thread"),
          }),
        );
        assert.strictEqual((yield* store.getWait(first.waitId))?.state, "waiting");
        yield* service.cancelWait({
          commandId: CommandId.make("cancel-first"),
          waitId: first.waitId,
          threadId: first.input.owner.threadId,
        });
        yield* mark(handle, "stopped");
        yield* service.releaseWhenQuiescent({ commandId: CommandId.make("release-owner"), handle });
        assert.strictEqual((yield* store.getWait(first.waitId))?.state, "cancelled");
        assert.strictEqual((yield* store.getWait(second.waitId))?.state, "ready");
        assert.strictEqual((yield* service.inspect({ identity }))?.state, "released");
      }).pipe(Effect.provide(dependencies)),
  );
});
