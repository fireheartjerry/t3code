import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  ProjectId,
  ThreadId,
  type SourceIdentity,
  type VerificationRecord,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Queue from "effect/Queue";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as Outbox from "../orchestration-v2/EffectOutbox.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Invalidation from "./VerificationInvalidationService.ts";
import * as Store from "./VerificationStore.ts";
import type { ObservationSnapshot } from "./VerificationSourceObserver.ts";

const layer = Invalidation.layer.pipe(
  Layer.provideMerge(Store.layer),
  Layer.provideMerge(Outbox.layer),
  Layer.provideMerge(Sqlite.layerMemory),
);
const time = "2026-10-25T12:00:00.000Z";
const fixture = Effect.fn("Invalidation.test.fixture")(function* (
  id = "check",
  checkoutKey = "checkout",
  checkpointRef = "checkpoint",
) {
  const store = yield* Store.VerificationStore;
  const service = yield* Invalidation.VerificationInvalidationService;
  const source: SourceIdentity = {
    checkoutIdentity: {
      environmentId: EnvironmentId.make("environment"),
      checkoutKey,
      canonicalCwd: `/${checkoutKey}`,
      filesystemIdentity: { device: "1", file: checkoutKey },
      gitDir: null,
      gitCommonDir: null,
    },
    checkpointRef,
    head: "head",
    branch: "branch",
    indexDigest: "index",
    digest: "source",
    coverage: { complete: true, gaps: [], policyRevision: "policy" },
    toolchainRevision: "tools",
    configRevision: "config",
  };
  const record: VerificationRecord = {
    verificationId: id,
    commandId: CommandId.make(id),
    projectId: ProjectId.make("project"),
    threadId: ThreadId.make(id),
    sourceRunId: null,
    environmentId: source.checkoutIdentity.environmentId,
    ownerHandle: {
      ownershipId: id,
      checkoutKey,
      generation: 1,
      serverInstanceId: "server",
      owner: { kind: "verification", threadId: ThreadId.make(id), verificationId: id },
      operation: "verification",
    },
    actionSnapshot: {
      actionId: "check",
      revision: "revision",
      command: "check",
      shell: "/bin/sh",
      cwd: `/${checkoutKey}`,
      timeoutMs: 1000,
    },
    execution: "queued",
    currentness: "Unknown",
    requestedAt: time,
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
  };
  yield* store.queue({ record, checkoutKey, sourceRunOrdinal: 1, effects: [] });
  yield* store.claim({
    verificationId: id,
    executionClaimId: id,
    serverInstanceId: "server",
    handle: record.ownerHandle!,
    startedAt: time,
    source,
    mutationGeneration: 0,
    observationState: "continuous",
  });
  yield* store.finish({
    verificationId: id,
    executionClaimId: id,
    commandId: CommandId.make(`finish:${id}`),
    completion: {
      execution: "passed",
      currentness: "Current",
      finishedAt: time,
      exitCode: 0,
      signal: null,
      terminationReason: null,
      endSource: source,
      endMutationGeneration: 0,
      observationState: "continuous",
      outputReferences: [],
      truncated: false,
    },
  });
  const sample = yield* Ref.make<ObservationSnapshot>({
    mutationGeneration: 0,
    observationState: "continuous",
  });
  yield* service.retain({
    verificationId: id,
    lease: { snapshot: Ref.get(sample), changes: Stream.never, close: Effect.void },
  });
  return { store, service, source, sample };
});

it.effect.each([
  "tracked-edit",
  "untracked-edit",
  "index",
  "head",
  "submodule",
  "restore",
  "action-config",
  "toolchain",
  "ownership-conflict",
] as const)("stales the appropriate current result after %s", (reason) =>
  Effect.gen(function* () {
    const { service, store } = yield* fixture();
    yield* fixture("unrelated", "other-checkout");
    yield* service.observeInvalidation({ checkoutKey: "checkout", reason });
    assert.strictEqual((yield* service.revalidate("check"))?.currentness, "Stale");
    assert.strictEqual((yield* store.get("unrelated"))?.currentness, "Current");
  }).pipe(Effect.provide(layer)),
);

it.effect("scopes checkpoint restore to the affected checkout and checkpoint", () =>
  Effect.gen(function* () {
    const { service, store } = yield* fixture();
    yield* fixture("other-checkpoint", "checkout", "different");
    yield* service.observeInvalidation({
      checkoutKey: "checkout",
      checkpointRef: "checkpoint",
      reason: "restore",
    });
    assert.strictEqual((yield* store.get("check"))?.currentness, "Stale");
    assert.strictEqual((yield* store.get("other-checkpoint"))?.currentness, "Current");
  }).pipe(Effect.provide(layer)),
);

it.effect("excluded output and client retrieval preserve an uninterrupted current pass", () =>
  Effect.gen(function* () {
    const { service } = yield* fixture();
    yield* service.observeInvalidation({ checkoutKey: "checkout", reason: "output-only" });
    assert.strictEqual((yield* service.revalidate("check"))?.currentness, "Current");
    assert.strictEqual((yield* service.revalidate("check"))?.currentness, "Current");
  }).pipe(Effect.provide(layer)),
);

it.effect("retains edit-and-revert mutation evidence and never resurrects it", () =>
  Effect.gen(function* () {
    const { service, sample } = yield* fixture();
    yield* Ref.set(sample, { mutationGeneration: 2, observationState: "continuous" as const });
    assert.strictEqual((yield* service.revalidate("check"))?.currentness, "Stale");
    yield* Ref.set(sample, { mutationGeneration: 0, observationState: "continuous" as const });
    assert.strictEqual((yield* service.revalidate("check"))?.currentness, "Stale");
  }).pipe(Effect.provide(layer)),
);

it.effect("server observation gaps stay Unknown even with equal fresh hashes and generations", () =>
  Effect.gen(function* () {
    const { service, source } = yield* fixture();
    yield* service.observeInvalidation({ verificationId: "check", reason: "server-gap" });
    const result = yield* service.revalidate("check", {
      source,
      mutationGeneration: 0,
      observationState: "continuous",
      newerSourceRun: false,
      configRevision: "config",
      toolchainRevision: "tools",
      ownershipConflict: false,
    });
    assert.strictEqual(result?.currentness, "Unknown");
    assert.strictEqual(result?.observationState, "gap");
  }).pipe(Effect.provide(layer)),
);

it.effect("newer source evidence dominates stale evidence and unscoped signals do nothing", () =>
  Effect.gen(function* () {
    const { service, store } = yield* fixture();
    yield* service.observeInvalidation({ reason: "source" });
    assert.strictEqual((yield* store.get("check"))?.currentness, "Current");
    yield* service.observeInvalidation({ verificationId: "check", reason: "source" });
    yield* service.observeInvalidation({ verificationId: "check", reason: "newer-run" });
    assert.strictEqual((yield* store.get("check"))?.currentness, "Historical only");
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "retained observer events durably invalidate completed evidence without a read or hash scan",
  () =>
    Effect.gen(function* () {
      const { store, sample } = yield* fixture();
      const persisted = yield* Deferred.make<void>();
      const context = yield* Layer.build(Layer.fresh(Invalidation.layer)).pipe(
        Effect.provideService(Store.VerificationStore, {
          ...store,
          invalidate: (input) =>
            store
              .invalidate(input)
              .pipe(
                Effect.tap((record) =>
                  record?.currentness === "Stale"
                    ? Deferred.succeed(persisted, undefined)
                    : Effect.void,
                ),
              ),
        }),
      );
      const service = Context.get(context, Invalidation.VerificationInvalidationService);
      const changes = yield* Queue.unbounded<ObservationSnapshot>();
      yield* service.retain({
        verificationId: "check",
        lease: {
          snapshot: Ref.get(sample),
          changes: Stream.fromQueue(changes),
          close: Effect.void,
        },
      });
      const mutation: ObservationSnapshot = {
        mutationGeneration: 2,
        observationState: "continuous",
      };
      yield* Ref.set(sample, mutation);
      yield* Queue.offer(changes, mutation);
      yield* Deferred.await(persisted);
      assert.strictEqual((yield* store.get("check"))?.currentness, "Stale");
    }).pipe(Effect.provide(layer)),
);
