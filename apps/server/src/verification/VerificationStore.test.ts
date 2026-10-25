import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  ProjectId,
  RunId,
  ThreadId,
  type SourceIdentity,
  type VerificationRecord,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Outbox from "../orchestration-v2/EffectOutbox.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Store from "./VerificationStore.ts";

const layer = Store.layer.pipe(
  Layer.provideMerge(Outbox.layer),
  Layer.provideMerge(Sqlite.layerMemory),
);
const now = "2026-10-25T12:00:00.000Z";
const source: SourceIdentity = {
  checkoutIdentity: {
    environmentId: EnvironmentId.make("environment"),
    checkoutKey: "checkout",
    canonicalCwd: "/repo",
    filesystemIdentity: { device: "1", file: "2" },
    gitDir: null,
    gitCommonDir: null,
  },
  checkpointRef: "checkpoint",
  head: null,
  branch: null,
  indexDigest: null,
  digest: "source",
  coverage: { complete: true, gaps: [], policyRevision: "inputs" },
  configRevision: "config",
  toolchainRevision: "tools",
};
const record = (id: string): VerificationRecord => ({
  verificationId: id,
  commandId: CommandId.make(id),
  projectId: ProjectId.make("project"),
  threadId: ThreadId.make("thread"),
  sourceRunId: RunId.make(id),
  environmentId: source.checkoutIdentity.environmentId,
  ownerHandle: {
    ownershipId: id,
    checkoutKey: "checkout",
    generation: 1,
    serverInstanceId: "server",
    owner: { kind: "verification", threadId: ThreadId.make("thread"), verificationId: id },
    operation: "verification",
  },
  actionSnapshot: {
    actionId: "check",
    revision: "revision",
    command: "check",
    shell: "/bin/sh",
    cwd: "/repo",
    timeoutMs: 1000,
  },
  execution: "queued",
  currentness: "Unknown",
  requestedAt: now,
  startedAt: null,
  finishedAt: null,
  exitCode: null,
  signal: null,
  terminationReason: null,
  startSource: null,
  endSource: null,
  startMutationGeneration: null,
  endMutationGeneration: null,
  observationState: "continuous",
  outputReferences: [],
  truncated: false,
});
const queue = (store: Store.VerificationStore["Service"], id: string, ordinal = 1) =>
  store.queue({
    record: record(id),
    sourceRunOrdinal: ordinal,
    checkoutKey: "checkout",
    effects: [
      {
        id: `effect:${id}`,
        commandId: CommandId.make(id),
        threadId: ThreadId.make("thread"),
        request: {
          type: "verification.execute",
          verificationId: id,
          executionClaimId: `claim:${id}`,
        },
      },
    ],
  });
const claim = (store: Store.VerificationStore["Service"], id: string) =>
  store.claim({
    verificationId: id,
    executionClaimId: `claim:${id}`,
    serverInstanceId: "server",
    handle: record(id).ownerHandle!,
    startedAt: now,
    source,
    mutationGeneration: 0,
    observationState: "continuous",
  });
const finish = (
  store: Store.VerificationStore["Service"],
  id: string,
  execution: "failed" | "passed" = "passed",
) =>
  store.finish({
    verificationId: id,
    executionClaimId: `claim:${id}`,
    commandId: CommandId.make(`finish:${id}`),
    completion: {
      execution,
      currentness: "Current",
      finishedAt: now,
      exitCode: execution === "passed" ? 0 : 1,
      signal: null,
      terminationReason: null,
      endSource: source,
      endMutationGeneration: 0,
      observationState: "continuous",
      outputReferences: [],
      truncated: false,
    },
  });

it.effect(
  "starts with no fabricated evidence and atomically persists one intent/outbox on replay",
  () =>
    Effect.gen(function* () {
      const store = yield* Store.VerificationStore;
      assert.isNull(yield* store.latestForThread(ThreadId.make("thread")));
      const first = yield* queue(store, "a");
      assert.isFalse((yield* queue(store, "a")).inserted);
      assert.strictEqual(first.record.execution, "queued");
      const sql = yield* SqlClient.SqlClient;
      assert.strictEqual(
        (yield* sql`SELECT * FROM orchestration_v2_verification_events`).length,
        1,
      );
      assert.strictEqual(
        (yield* (yield* Outbox.EffectOutboxV2).listByCommandId(CommandId.make("a"))).length,
        1,
      );
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "claims exactly once before execution and keeps cancellation intent separate from an actual exit",
  () =>
    Effect.gen(function* () {
      const store = yield* Store.VerificationStore;
      yield* queue(store, "a");
      const claims = yield* Effect.all([claim(store, "a"), claim(store, "a")], {
        concurrency: "unbounded",
      });
      assert.strictEqual(claims.filter(Boolean).length, 1);
      yield* store.requestCancel({ verificationId: "a", commandId: CommandId.make("cancel"), now });
      const completed = yield* finish(store, "a", "failed");
      assert.strictEqual(completed?.execution, "failed");
      assert.strictEqual(completed?.exitCode, 1);
      assert.isTrue(yield* store.isCancellationRequested("a"));
      assert.strictEqual((yield* finish(store, "a"))?.execution, "failed");
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "newer attempts supersede evidence and late older completions cannot replace the summary",
  () =>
    Effect.gen(function* () {
      const store = yield* Store.VerificationStore;
      yield* queue(store, "old", 1);
      yield* claim(store, "old");
      yield* queue(store, "new", 2);
      assert.strictEqual((yield* finish(store, "old"))?.currentness, "Historical only");
      assert.strictEqual(
        (yield* store.latestForThread(ThreadId.make("thread")))?.verificationId,
        "new",
      );
      yield* queue(store, "late-old", 1);
      assert.strictEqual((yield* store.get("late-old"))?.currentness, "Historical only");
    }).pipe(Effect.provide(layer)),
);

it.effect("rolls back intent, summary changes, receipts and outbox together", () =>
  Effect.gen(function* () {
    const store = yield* Store.VerificationStore;
    yield* queue(store, "a");
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TRIGGER reject_verification_event BEFORE INSERT ON orchestration_v2_verification_events WHEN NEW.verification_id = 'b' BEGIN SELECT RAISE(ABORT, 'fixture'); END`;
    yield* queue(store, "b", 2).pipe(Effect.flip);
    assert.isNull(yield* store.get("b"));
    assert.strictEqual((yield* store.get("a"))?.currentness, "Unknown");
    assert.strictEqual(
      (yield* (yield* Outbox.EffectOutboxV2).listByCommandId(CommandId.make("b"))).length,
      0,
    );
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "reloads durable failure and interrupted outcomes without ever reclaiming ambiguous execution",
  () =>
    Effect.gen(function* () {
      const store = yield* Store.VerificationStore;
      yield* queue(store, "failed");
      yield* claim(store, "failed");
      yield* finish(store, "failed", "failed");
      yield* queue(store, "running");
      yield* claim(store, "running");
      yield* store.interruptRunning({ serverInstanceId: "new-server", now });
      const reopened = yield* Store.VerificationStore.pipe(Effect.provide(Store.layer));
      assert.strictEqual((yield* reopened.get("failed"))?.execution, "failed");
      assert.strictEqual((yield* reopened.get("running"))?.execution, "interrupted");
      assert.isNull(yield* claim(reopened, "running"));
      assert.strictEqual((yield* reopened.listRecoverable).length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "persists failed-before-start and queued cancellation without creating execution evidence",
  () =>
    Effect.gen(function* () {
      const store = yield* Store.VerificationStore;
      yield* queue(store, "missing");
      const failed = yield* store.failQueued({
        verificationId: "missing",
        commandId: CommandId.make("missing-action"),
        now,
        reason: "not-configured",
      });
      assert.strictEqual(failed?.execution, "failed");
      assert.isNull(failed?.startedAt);
      yield* queue(store, "cancel");
      yield* store.requestCancel({
        verificationId: "cancel",
        commandId: CommandId.make("cancel-intent"),
        now,
      });
      assert.isNull(yield* claim(store, "cancel"));
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "keeps source invalidation and server gaps sticky through completion and returns bounded owned output",
  () =>
    Effect.gen(function* () {
      const store = yield* Store.VerificationStore;
      yield* queue(store, "a");
      yield* claim(store, "a");
      yield* store.invalidate({
        verificationId: "a",
        commandId: CommandId.make("gap"),
        currentness: "Unknown",
        observationGap: true,
      });
      const result = yield* finish(store, "a");
      assert.strictEqual(result?.currentness, "Unknown");
      assert.strictEqual(result?.observationState, "gap");
      yield* store.putOutput({
        verificationId: "a",
        referenceId: "stdout",
        kind: "stdout",
        text: "abcdef",
        truncated: true,
      });
      assert.deepStrictEqual(
        yield* store.getOutput({ verificationId: "a", referenceId: "stdout", offset: 0, limit: 3 }),
        { text: "abc", nextOffset: 3, truncated: true },
      );
      assert.isNull(
        yield* store.getOutput({
          verificationId: "other",
          referenceId: "stdout",
          offset: 0,
          limit: 3,
        }),
      );
    }).pipe(Effect.provide(layer)),
);

it.effect("stale fences and foreign source identities cannot consume a queued execution", () =>
  Effect.gen(function* () {
    const store = yield* Store.VerificationStore;
    yield* queue(store, "a");
    const handle = record("a").ownerHandle!;
    assert.isNull(
      yield* store.claim({
        verificationId: "a",
        executionClaimId: "stale",
        serverInstanceId: "server",
        handle: { ...handle, generation: handle.generation + 1 },
        startedAt: now,
        source,
        mutationGeneration: 0,
        observationState: "continuous",
      }),
    );
    assert.isNull(
      yield* store.claim({
        verificationId: "a",
        executionClaimId: "foreign",
        serverInstanceId: "server",
        handle,
        startedAt: now,
        source: {
          ...source,
          checkoutIdentity: { ...source.checkoutIdentity, checkoutKey: "other" },
        },
        mutationGeneration: 0,
        observationState: "continuous",
      }),
    );
    assert.strictEqual((yield* claim(store, "a"))?.execution, "running");
  }).pipe(Effect.provide(layer)),
);
