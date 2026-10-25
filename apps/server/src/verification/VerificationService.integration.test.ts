// @effect-diagnostics preferSchemaOverJson:off -- Serialization assertions check that persisted evidence contains no fixture credential.
import { assert, it } from "@effect/vitest";
import {
  CheckpointId,
  CheckpointRef,
  CheckpointScopeId,
  CommandId,
  EnvironmentId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type CheckoutIdentity,
  type OrchestrationV2DomainEvent,
  type VerificationConfig,
  type VerificationRecord,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Exit from "effect/Exit";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as Environment from "../environment/ServerEnvironment.ts";
import * as Outbox from "../orchestration-v2/EffectOutbox.ts";
import * as Projects from "../orchestration-v2/ProjectStore.ts";
import * as Projections from "../orchestration-v2/ProjectionStore.ts";
import * as Policy from "../orchestration-v2/RuntimePolicy.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as Settings from "../serverSettings.ts";
import * as Admission from "../workspace/CheckoutAdmission.ts";
import * as Identity from "../workspace/CheckoutIdentity.ts";
import * as Ownership from "../workspace/CheckoutOwnershipService.ts";
import * as OwnershipStore from "../workspace/CheckoutOwnershipStore.ts";
import * as Accounting from "../workspace/CheckoutProcessAccounting.ts";
import * as Fingerprint from "./VerificationFingerprint.ts";
import * as Invalidation from "./VerificationInvalidationService.ts";
import * as Observer from "./VerificationSourceObserver.ts";
import * as Runner from "./VerificationRunner.ts";
import * as Service from "./VerificationService.ts";
import * as Store from "./VerificationStore.ts";

const config: VerificationConfig = {
  actionId: "check",
  automatic: false,
  timeoutMs: 1000,
  inputs: { include: ["**/*"], exclude: ["dist/**"], nonGit: "fingerprint" },
};
const environmentId = EnvironmentId.make("fixture");
const projectId = ProjectId.make("project");
const threadId = ThreadId.make("thread");
const runId = RunId.make("run");
const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "test" };
const identity: CheckoutIdentity = {
  environmentId,
  checkoutKey: "checkout",
  canonicalCwd: "/fixture",
  filesystemIdentity: { device: "1", file: "2" },
  gitDir: null,
  gitCommonDir: null,
};
const persistence = Layer.mergeAll(
  Outbox.layer,
  Projects.layer,
  Projections.layer,
  OwnershipStore.layer,
  Accounting.layer,
).pipe(Layer.provideMerge(Sqlite.layerMemory));
const layer = Store.layer.pipe(Layer.provideMerge(persistence));
const fixture = Effect.fn("VerificationService.test.fixture")(function* (
  options: {
    readonly blocked?: boolean;
    readonly configured?: boolean;
    readonly automatic?: boolean;
    readonly writes?: boolean;
    readonly policy?: "full-access" | "approval-required";
    readonly command?: string;
    readonly shell?: string;
    readonly platform?: NodeJS.Platform;
    readonly exitCode?: number;
    readonly noAcquireIntent?: boolean;
    readonly denyAdmission?: boolean;
    readonly duringCapture?: (
      settings: Settings.ServerSettingsService["Service"],
    ) => Effect.Effect<void>;
  } = {},
) {
  const projection = yield* Projections.ProjectionStoreV2;
  const projects = yield* Projects.ProjectStoreV2;
  const timestamp = yield* DateTime.now;
  const occurredAt = DateTime.formatIso(timestamp);
  yield* projects.apply({
    sequence: 0,
    eventId: EventId.make("project-created"),
    aggregateKind: "project",
    aggregateId: projectId,
    occurredAt,
    commandId: null,
    causationEventId: null,
    correlationId: null,
    metadata: {},
    type: "project.created",
    payload: {
      projectId,
      title: "Fixture",
      workspaceRoot: identity.canonicalCwd,
      defaultModelSelection: modelSelection,
      scripts: [],
      createdAt: occurredAt,
      updatedAt: occurredAt,
    },
  });
  const event = (
    type: "thread.created",
    payload: Extract<OrchestrationV2DomainEvent, { type: "thread.created" }>["payload"],
  ): OrchestrationV2DomainEvent => ({
    id: EventId.make(type),
    threadId,
    type,
    occurredAt: timestamp,
    payload,
  });
  yield* projection.apply(
    event("thread.created", {
      id: threadId,
      projectId,
      title: "Fixture",
      createdBy: "user",
      creationSource: "web",
      providerInstanceId: modelSelection.instanceId,
      modelSelection,
      runtimeMode: options.policy ?? "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: null,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
      forkedFrom: null,
      createdAt: timestamp,
      updatedAt: timestamp,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    }),
  );
  yield* projection.apply({
    id: EventId.make("run-created"),
    threadId,
    type: "run.created",
    occurredAt: timestamp,
    payload: {
      id: runId,
      threadId,
      ordinal: 1,
      providerInstanceId: modelSelection.instanceId,
      modelSelection,
      providerThreadId: null,
      userMessageId: MessageId.make("user"),
      rootNodeId: null,
      activeAttemptId: null,
      status: "completed",
      requestedAt: timestamp,
      startedAt: timestamp,
      completedAt: timestamp,
      checkpointId: null,
      contextHandoffId: null,
    },
  });
  yield* projection.apply({
    id: EventId.make("checkpoint"),
    threadId,
    type: "checkpoint.captured",
    occurredAt: timestamp,
    payload: {
      id: CheckpointId.make("checkpoint"),
      threadId,
      scopeId: CheckpointScopeId.make("scope"),
      runId,
      nodeId: NodeId.make("node"),
      parentCheckpointId: null,
      ordinalWithinScope: 1,
      appRunOrdinal: 1,
      ref: CheckpointRef.make("checkpoint-ref"),
      status: "ready",
      files:
        options.writes === false
          ? []
          : [{ path: "source.ts", kind: "modified", additions: 1, deletions: 0 }],
      capturedAt: timestamp,
    },
  });
  const settings = yield* Settings.ServerSettingsService.pipe(
    Effect.provide(
      Settings.ServerSettingsService.layerTest({
        verification:
          options.configured === false
            ? null
            : { ...config, automatic: options.automatic ?? false },
        defaultProjectScripts: [
          {
            id: "check",
            name: "Check",
            command: options.command ?? "check-fixture",
            icon: "test",
            runOnWorktreeCreate: false,
          },
        ],
        projectSettingsFolded: true,
      }),
    ),
  );
  const resolveIdentity: Identity.CheckoutIdentityResolver["Service"]["resolve"] = ({ cwd }) =>
    Effect.succeed(
      cwd === identity.canonicalCwd
        ? identity
        : {
            ...identity,
            canonicalCwd: cwd,
            checkoutKey: cwd,
            filesystemIdentity: { device: "1", file: cwd },
          },
    );
  const accounting = yield* Accounting.CheckoutProcessAccounting;
  const ownership = yield* Ownership.CheckoutOwnershipService.pipe(
    Effect.provide(Ownership.layer),
    Effect.provideService(Environment.ServerEnvironmentIdentity, {
      getEnvironmentId: Effect.succeed(environmentId),
    }),
    Effect.provideService(Identity.CheckoutIdentityResolver, { resolve: resolveIdentity }),
  );
  const gate = yield* Deferred.make<void>();
  const terminated = yield* Deferred.make<void>();
  const started = yield* Queue.unbounded<ProcessRunner.ProcessRunInput>();
  const completed = yield* Queue.unbounded<VerificationRecord>();
  const realStore = yield* Store.VerificationStore;
  const store = Store.VerificationStore.of({
    ...realStore,
    finish: (input) =>
      realStore
        .finish(input)
        .pipe(Effect.tap((record) => (record ? Queue.offer(completed, record) : Effect.void))),
    failQueued: (input) =>
      realStore
        .failQueued(input)
        .pipe(Effect.tap((record) => (record ? Queue.offer(completed, record) : Effect.void))),
  });
  const observation = yield* Ref.make<Observer.ObservationSnapshot>({
    mutationGeneration: 0,
    observationState: "continuous",
  });
  const closes = yield* Ref.make(0);
  const observer = Observer.VerificationSourceObserver.of({
    start: () =>
      Effect.succeed({
        snapshot: Ref.get(observation),
        changes: Stream.never,
        close: Ref.update(closes, (n) => n + 1),
      }),
  });
  const process = ProcessRunner.ProcessRunner.of({
    run: (input) =>
      Effect.gen(function* () {
        yield* input.onSpawn?.(12345) ?? Effect.void;
        yield* Queue.offer(started, input);
        yield* Deferred.await(gate);
        return {
          code: ChildProcessSpawner.ExitCode(options.exitCode ?? 0),
          stdout: "checked fixture-secret-value",
          stderr: "",
          stdoutTruncated: false,
          stderrTruncated: false,
          stdoutInvalidUtf8: false,
          stderrInvalidUtf8: false,
          timedOut: false,
        };
      }).pipe(Effect.ensuring(Deferred.succeed(terminated, undefined))),
  });
  if (!options.blocked) yield* Deferred.succeed(gate, undefined);
  const runner = yield* Runner.VerificationRunner.pipe(
    Effect.provide(Runner.layer),
    Effect.provideService(Ownership.CheckoutOwnershipService, ownership),
    Effect.provideService(ProcessRunner.ProcessRunner, process),
    Effect.provideService(Observer.VerificationSourceObserver, observer),
    Effect.provideService(Fingerprint.VerificationFingerprint, {
      capture: (input) =>
        (options.duringCapture ? options.duringCapture(settings) : Effect.void).pipe(
          Effect.as({
            checkoutIdentity: input.identity,
            checkpointRef: input.checkpointRef ?? null,
            head: null,
            branch: null,
            indexDigest: null,
            digest: "source",
            coverage: { complete: true, gaps: [], policyRevision: "inputs" },
            toolchainRevision: input.toolchainRevision,
            configRevision: input.configRevision,
          }),
        ),
    }),
    Effect.provideService(HostProcess.Platform, options.platform ?? "darwin"),
  );
  const invalidation = yield* Layer.build(Invalidation.layer).pipe(
    Effect.map((context) => Context.get(context, Invalidation.VerificationInvalidationService)),
    Effect.provideService(Store.VerificationStore, store),
  );
  const admission = yield* Admission.CheckoutAdmission;
  const gateCalls = yield* Ref.make(0);
  const scope = yield* Scope.make();
  yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
  const service = yield* Layer.buildWithScope(Service.layer, scope).pipe(
    Effect.map((context) => Context.get(context, Service.VerificationService)),
    Effect.provideService(Admission.CheckoutAdmission, {
      ...admission,
      withVerificationAdmission: (input, effect) =>
        Effect.gen(function* () {
          yield* Ref.update(gateCalls, (n) => n + 1);
          if (input.threadId === threadId)
            assert.strictEqual(input.identity.checkoutKey, identity.checkoutKey);
          if (options.denyAdmission)
            return yield* new Admission.CheckoutAdmissionError({ threadId: input.threadId });
          return yield* effect;
        }),
    }),
    Effect.provideService(Store.VerificationStore, store),
    Effect.provideService(Runner.VerificationRunner, runner),
    Effect.provideService(Invalidation.VerificationInvalidationService, invalidation),
    Effect.provideService(
      Ownership.CheckoutOwnershipService,
      options.noAcquireIntent ? { ...ownership, acquireWithIntent: undefined } : ownership,
    ),
    Effect.provideService(Settings.ServerSettingsService, settings),
    Effect.provideService(Policy.RuntimePolicyV2, {
      resolve: ({ thread }) =>
        Effect.succeed({
          runtimeMode: thread.runtimeMode,
          interactionMode: thread.interactionMode,
          cwd: thread.worktreePath ?? identity.canonicalCwd,
        }),
    }),
    Effect.provideService(Environment.ServerEnvironmentIdentity, {
      getEnvironmentId: Effect.succeed(environmentId),
    }),
    Effect.provideService(Identity.CheckoutIdentityResolver, { resolve: resolveIdentity }),
    Effect.provideService(HostProcess.Platform, options.platform ?? "darwin"),
    Effect.provideService(HostProcess.Environment, {
      SHELL: options.shell ?? "/bin/sh",
      PATH: "/bin",
      FIXTURE_API_KEY: "fixture-secret-value",
    }),
  );
  const execute = Effect.fn("test.execute")(function* (record: VerificationRecord) {
    const outbox = yield* Outbox.EffectOutboxV2;
    const effects = yield* outbox.listByCommandId(record.commandId);
    const request = effects[0]?.request;
    if (request?.type !== "verification.execute") throw new Error("Missing verification effect");
    yield* service.executeQueued(request);
    return request;
  });
  return {
    service,
    store,
    ownership,
    settings,
    accounting,
    gate,
    started,
    completed,
    terminated,
    observation,
    closes,
    execute,
    scope,
    gateCalls,
    projection,
  };
});
const request = (id = "request") => ({ commandId: CommandId.make(id), threadId });

it.effect(
  "duplicate request and lost-ack delivery execute one shell and read methods never spawn",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture({ blocked: true });
      const [record, replay] = yield* Effect.all(
        [f.service.request(request()), f.service.request(request())],
        { concurrency: "unbounded" },
      );
      assert.strictEqual(record.verificationId, replay.verificationId);
      const effect = yield* f.execute(record);
      yield* Queue.take(f.started);
      yield* f.service.executeQueued(effect);
      const running = yield* f.service.get(record.verificationId);
      assert.strictEqual(running?.execution, "running");
      assert.strictEqual(running?.currentness, "Unknown");
      assert.strictEqual(
        (yield* f.service.latest(threadId))?.verificationId,
        record.verificationId,
      );
      assert.isTrue(Queue.sizeUnsafe(f.started) === 0);
      yield* Deferred.succeed(f.gate, undefined);
      const result = yield* Queue.take(f.completed);
      assert.strictEqual(result.execution, "passed");
      assert.isNotNull(result.startedAt);
      yield* f.service.executeQueued(effect);
      assert.isTrue(Queue.sizeUnsafe(f.started) === 0);
      const output = yield* f.service.output({
        verificationId: record.verificationId,
        referenceId: `${record.verificationId}:stdout`,
        offset: 0,
        limit: 100,
      });
      assert.isFalse(output?.text.includes("fixture-secret-value") ?? true);
      const sql = yield* SqlClient.SqlClient;
      assert.isFalse(
        JSON.stringify(yield* sql`SELECT * FROM orchestration_v2_verification_attempts`).includes(
          "fixture-secret-value",
        ),
      );
    }).pipe(Effect.provide(layer)),
);

it.effect("explicit busy verification returns the structured owner conflict", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* f.ownership.acquire({
      commandId: CommandId.make("writer"),
      identity,
      owner: { kind: "run", threadId, runId },
      operation: "provider",
    });
    const error = yield* f.service.request(request()).pipe(Effect.flip);
    assert.strictEqual(error._tag, "VerificationConflict");
    if (error._tag === "VerificationConflict") {
      assert.strictEqual(error.conflict.owner.kind, "run");
      assert.strictEqual(error.conflict.identity.checkoutKey, "checkout");
    }
    assert.isNull(yield* f.service.latest(threadId));
  }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["missing configuration", { configured: false }, "VerificationNotConfigured"],
  ["restricted destination", { policy: "approval-required" }, "VerificationServiceError"],
  ["unsafe recorded command", { command: "echo fixture-secret-value" }, "VerificationServiceError"],
  ["missing atomic admission", { noAcquireIntent: true }, "VerificationServiceError"],
] as const)("rejects %s without persisting a passing record", ([_name, options, tag]) =>
  Effect.gen(function* () {
    const f = yield* fixture(options);
    assert.strictEqual((yield* f.service.request(request()).pipe(Effect.flip))._tag, tag);
    assert.isNull(yield* f.store.latestForThread(threadId));
    assert.strictEqual(Queue.sizeUnsafe(f.started), 0);
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "a changed or removed action fails before spawning and releases the never-started intent",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const record = yield* f.service.request(request());
      yield* f.settings.updateSettings({ verification: null });
      yield* f.execute(record);
      const failed = yield* Queue.take(f.completed);
      assert.strictEqual(failed.execution, "failed");
      assert.strictEqual(failed.terminationReason, "not-configured");
      assert.isNull(failed.startedAt);
      assert.isTrue(Queue.sizeUnsafe(f.started) === 0);
    }).pipe(Effect.provide(layer)),
);

it.effect("durably cancels a running attempt and awaits subprocess cleanup before returning", () =>
  Effect.gen(function* () {
    const f = yield* fixture({ blocked: true });
    const record = yield* f.service.request(request());
    yield* f.execute(record);
    yield* Queue.take(f.started);
    const cancelled = yield* f.service.cancel({
      commandId: CommandId.make("cancel"),
      verificationId: record.verificationId,
    });
    assert.strictEqual(cancelled?.execution, "cancelled");
    assert.isTrue(yield* f.store.isCancellationRequested(record.verificationId));
    yield* Deferred.await(f.terminated);
    assert.strictEqual(
      (yield* f.service.cancel({
        commandId: CommandId.make("cancel"),
        verificationId: record.verificationId,
      }))?.execution,
      "cancelled",
    );
  }).pipe(Effect.provide(layer)),
);

it.effect("queued cancellation prevents late effect delivery from starting", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    const record = yield* f.service.request(request());
    yield* f.service.cancel({
      commandId: CommandId.make("cancel"),
      verificationId: record.verificationId,
    });
    yield* f.execute(record);
    assert.strictEqual((yield* f.store.get(record.verificationId))?.execution, "cancelled");
    assert.isTrue(Queue.sizeUnsafe(f.started) === 0);
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "automatic finalization transfers ownership and queued intent together exactly once",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture({ automatic: true });
      const acquired = yield* f.ownership.acquire({
        commandId: CommandId.make("writer"),
        identity,
        owner: { kind: "run", threadId, runId },
        operation: "finalization",
      });
      if (acquired._tag !== "Acquired") throw new Error("Fixture owner missing");
      const processRef = { id: "provider", creationMarker: "provider" };
      yield* f.accounting.register({ handle: acquired.handle, processRef, kind: "provider" });
      yield* f.accounting.mark({ owner: acquired.handle.owner, processRef, state: "stopped" });
      const input = {
        ...request("automatic"),
        sourceRunId: runId,
        handle: acquired.handle,
        checkpointRef: "checkpoint-ref",
      };
      const queued = yield* f.service.requestAfterFinalization(input);
      assert.isNotNull(queued);
      assert.strictEqual(queued?.ownerHandle?.generation, acquired.handle.generation + 1);
      assert.strictEqual(
        (yield* f.service.requestAfterFinalization(input))?.verificationId,
        queued?.verificationId,
      );
      const owner = yield* f.ownership.inspect({ identity });
      assert.strictEqual(owner?.handle.owner.kind, "verification");
      assert.strictEqual(
        (yield* (yield* Outbox.EffectOutboxV2).listByCommandId(input.commandId)).length,
        1,
      );
    }).pipe(Effect.provide(layer)),
);

it.effect.each([{ automatic: false }, { automatic: true, writes: false }])(
  "does not queue automatic checks for opted-out or text-only runs: %o",
  (options) =>
    Effect.gen(function* () {
      const f = yield* fixture(options);
      const acquired = yield* f.ownership.acquire({
        commandId: CommandId.make("writer"),
        identity,
        owner: { kind: "run", threadId, runId },
        operation: "finalization",
      });
      if (acquired._tag !== "Acquired") throw new Error("Fixture owner missing");
      assert.isNull(
        yield* f.service.requestAfterFinalization({
          ...request(),
          sourceRunId: runId,
          handle: acquired.handle,
          checkpointRef: "checkpoint-ref",
        }),
      );
      assert.isNull(yield* f.store.latestForThread(threadId));
    }).pipe(Effect.provide(layer)),
);

it.effect("scope cleanup persists cancellation and awaits execution termination", () =>
  Effect.gen(function* () {
    const f = yield* fixture({ blocked: true });
    const record = yield* f.service.request(request());
    yield* f.execute(record);
    yield* Queue.take(f.started);
    yield* Scope.close(f.scope, Exit.void);
    yield* Deferred.await(f.terminated);
    assert.strictEqual((yield* f.store.get(record.verificationId))?.execution, "cancelled");
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "checks off-policy admission before atomically acquiring and queueing explicit verification",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture({ denyAdmission: true });
      const error = yield* f.service.request(request()).pipe(Effect.flip);
      assert.strictEqual(error._tag, "VerificationServiceError");
      assert.strictEqual(yield* Ref.get(f.gateCalls), 1);
      assert.isNull(yield* f.ownership.inspect({ identity }));
      assert.isNull(yield* f.store.latestForThread(threadId));
    }).pipe(Effect.provide(layer)),
);

it.effect("rechecks settings after source capture and never spawns a changed action", () =>
  Effect.gen(function* () {
    const f = yield* fixture({
      duringCapture: (settings) =>
        settings.updateSettings({ verification: null }).pipe(Effect.orDie, Effect.asVoid),
    });
    const record = yield* f.service.request(request());
    yield* f.execute(record);
    const failed = yield* Queue.take(f.completed);
    assert.strictEqual(failed.execution, "failed");
    assert.strictEqual(failed.terminationReason, "execution-boundary-changed");
    assert.isNull(failed.startedAt);
    assert.strictEqual(Queue.sizeUnsafe(f.started), 0);
    assert.strictEqual(yield* Ref.get(f.closes), 1);
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "a long verification returns control and an independent checkout starts concurrently",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture({ blocked: true });
      const first = yield* f.service.request(request("first"));
      yield* f.execute(first);
      yield* Queue.take(f.started);
      const original = yield* f.projection.getThread(threadId);
      const other = ThreadId.make("other");
      const at = yield* DateTime.now;
      yield* f.projection.apply({
        id: EventId.make("other-thread"),
        threadId: other,
        type: "thread.created",
        occurredAt: at,
        payload: {
          ...original,
          id: other,
          worktreePath: "/independent",
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: other },
        },
      });
      const second = yield* f.service.request({
        commandId: CommandId.make("second"),
        threadId: other,
      });
      yield* f.execute(second);
      const started = yield* Queue.take(f.started);
      assert.strictEqual(started.cwd, "/independent");
      assert.strictEqual((yield* f.store.get(first.verificationId))?.execution, "running");
      yield* Deferred.succeed(f.gate, undefined);
      yield* Queue.take(f.completed);
      yield* Queue.take(f.completed);
    }).pipe(Effect.provide(layer)),
);

it.effect("handoff SQL failure rolls back owner transfer, evidence and execution intent", () =>
  Effect.gen(function* () {
    const f = yield* fixture({ automatic: true });
    const acquired = yield* f.ownership.acquire({
      commandId: CommandId.make("writer"),
      identity,
      owner: { kind: "run", threadId, runId },
      operation: "finalization",
    });
    if (acquired._tag !== "Acquired") throw new Error("Fixture owner missing");
    const processRef = { id: "provider", creationMarker: "provider" };
    yield* f.accounting.register({ handle: acquired.handle, processRef, kind: "provider" });
    yield* f.accounting.mark({ owner: acquired.handle.owner, processRef, state: "stopped" });
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TRIGGER reject_intent BEFORE INSERT ON orchestration_v2_verification_attempts BEGIN SELECT RAISE(ABORT, 'fixture'); END`;
    yield* f.service
      .requestAfterFinalization({
        ...request("automatic"),
        sourceRunId: runId,
        handle: acquired.handle,
        checkpointRef: "checkpoint-ref",
      })
      .pipe(Effect.flip);
    assert.isNull(yield* f.store.latestForThread(threadId));
    assert.deepStrictEqual((yield* f.ownership.inspect({ identity }))?.handle, acquired.handle);
    assert.strictEqual(
      (yield* (yield* Outbox.EffectOutboxV2).listByCommandId(CommandId.make("automatic"))).length,
      0,
    );
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "recovery marks ambiguous prior-instance execution Interrupted and never replays its shell",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const record = yield* f.service.request(request());
      const source = {
        checkoutIdentity: identity,
        checkpointRef: null,
        head: null,
        branch: null,
        indexDigest: null,
        digest: "source",
        coverage: { complete: true, gaps: [], policyRevision: "inputs" },
        configRevision: "config",
        toolchainRevision: "tools",
      };
      yield* f.store.claim({
        verificationId: record.verificationId,
        executionClaimId: "lost-ack",
        serverInstanceId: "prior-instance",
        handle: record.ownerHandle!,
        startedAt: null,
        source,
        mutationGeneration: 0,
        observationState: "continuous",
      });
      yield* f.service.recover();
      yield* f.service.recover();
      assert.strictEqual((yield* f.store.get(record.verificationId))?.execution, "interrupted");
      yield* f.execute(record);
      assert.strictEqual(Queue.sizeUnsafe(f.started), 0);
      assert.strictEqual((yield* f.ownership.inspect({ identity }))?.state, "uncertain");
    }).pipe(Effect.provide(layer)),
);

it.effect("recovery re-enqueues a safe queued intent idempotently", () =>
  Effect.gen(function* () {
    const f = yield* fixture({ blocked: true });
    const record = yield* f.service.request(request());
    yield* f.service.recover();
    yield* Queue.take(f.started);
    yield* f.service.recover();
    assert.strictEqual(Queue.sizeUnsafe(f.started), 0);
    yield* Deferred.succeed(f.gate, undefined);
    assert.strictEqual((yield* Queue.take(f.completed)).verificationId, record.verificationId);
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "configuration changes invalidate completed evidence on read without executing another shell",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const record = yield* f.service.request(request());
      yield* f.execute(record);
      yield* Queue.take(f.started);
      yield* Queue.take(f.completed);
      yield* f.settings.updateSettings({ verification: null });
      assert.strictEqual((yield* f.service.get(record.verificationId))?.currentness, "Stale");
      assert.strictEqual(Queue.sizeUnsafe(f.started), 0);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "selects the destination platform action and shell without substituting a missing action",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture({ platform: "win32" });
      const windows = { ...config, platformActions: { windows: "windows-check" } };
      yield* f.settings.updateSettings({ verification: windows });
      assert.strictEqual(
        (yield* f.service.request(request("missing-platform")).pipe(Effect.flip))._tag,
        "VerificationNotConfigured",
      );
      yield* f.settings.updateSettings({
        defaultProjectScripts: [
          {
            id: "windows-check",
            name: "Check Windows",
            command: "windows-check-command",
            icon: "test",
            runOnWorktreeCreate: false,
          },
        ],
      });
      const record = yield* f.service.request(request("platform"));
      assert.strictEqual(record.actionSnapshot.actionId, "windows-check");
      assert.strictEqual(record.actionSnapshot.shell, "cmd.exe");
      yield* f.execute(record);
      const process = yield* Queue.take(f.started);
      assert.strictEqual(process.command, "cmd.exe");
      assert.deepStrictEqual(process.args, ["/d", "/s", "/c", "windows-check-command"]);
      yield* Queue.take(f.completed);
    }).pipe(Effect.provide(layer)),
);

it.effect("unsupported shells fail before process start with an explicit reason", () =>
  Effect.gen(function* () {
    const f = yield* fixture({ shell: "/bin/unsupported-shell" });
    const record = yield* f.service.request(request());
    yield* f.execute(record);
    const failed = yield* Queue.take(f.completed);
    assert.strictEqual(failed.execution, "failed");
    assert.strictEqual(failed.terminationReason, "unsupported-shell");
    assert.isNull(failed.startedAt);
    assert.strictEqual(Queue.sizeUnsafe(f.started), 0);
  }).pipe(Effect.provide(layer)),
);
