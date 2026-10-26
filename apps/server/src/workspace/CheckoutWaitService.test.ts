import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
  type CheckoutIdentity,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as Ids from "@t3tools/provider-core/server/IdAllocator";
import * as Deferred from "effect/Deferred";
import * as DateTime from "effect/DateTime";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Ref from "effect/Ref";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Settings from "../serverSettings.ts";
import * as Environment from "../environment/ServerEnvironment.ts";
import * as Projections from "../orchestration-v2/ProjectionStore.ts";
import * as Projects from "../orchestration-v2/ProjectStore.ts";
import * as Events from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as Outbox from "../orchestration-v2/EffectOutbox.ts";
import * as Receipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as Positions from "../orchestration-v2/TurnItemPositionStore.ts";
import * as Identity from "./CheckoutIdentity.ts";
import * as Ownership from "./CheckoutOwnershipService.ts";
import * as Store from "./CheckoutOwnershipStore.ts";
import * as Accounting from "./CheckoutProcessAccounting.ts";
import * as Recovery from "./CheckoutOwnershipRecovery.ts";
import * as Admission from "./CheckoutAdmission.ts";
import * as Live from "./CheckoutAdmissionLive.ts";
import * as Wait from "./CheckoutWaitService.ts";

const encodeKey = Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)));
const waitKey = (kind: string, waitId: string) => `checkout-wait:${encodeKey([kind, waitId])}`;
const environmentId = EnvironmentId.make("environment");
const projectId = ProjectId.make("project");
const threadId = ThreadId.make("waiting-thread");
const runId = RunId.make("waiting-run");
const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" };
const identity: CheckoutIdentity = {
  environmentId,
  checkoutKey: "checkout",
  canonicalCwd: "/fixture",
  filesystemIdentity: { device: "1", file: "2" },
  gitDir: null,
  gitCommonDir: null,
};
const storage = Layer.mergeAll(
  Store.layer,
  Accounting.layer,
  Projections.layer,
  Projects.layer,
  EventStore.layer,
  Outbox.layer,
  Receipts.layer,
  Positions.layer,
).pipe(Layer.provideMerge(Sqlite.layerMemory));
const dependencies = Recovery.layer.pipe(
  Layer.provideMerge(Ownership.layer),
  Layer.provideMerge(Events.layerFromStores),
  Layer.provideMerge(storage),
  Layer.provideMerge(
    Layer.mergeAll(
      Ids.layer,
      Settings.ServerSettingsService.layerTest({ workspaceOwnership: "enforce" }),
      Layer.succeed(Environment.ServerEnvironmentIdentity, {
        getEnvironmentId: Effect.succeed(environmentId),
      }),
      Layer.succeed(Identity.CheckoutIdentityResolver, { resolve: () => Effect.succeed(identity) }),
    ),
  ),
);
const layer = Live.layer.pipe(Layer.provideMerge(dependencies));

const fixture = Effect.fn("CheckoutWaitService.test.fixture")(function* (options?: {
  readonly serviceScope?: Scope.Scope;
  readonly ownership?: (
    service: Ownership.CheckoutOwnershipService["Service"],
  ) => Partial<Ownership.CheckoutOwnershipService["Service"]>;
}) {
  const admission = yield* Admission.CheckoutAdmission;
  const ownership = yield* Ownership.CheckoutOwnershipService;
  const store = yield* Store.CheckoutOwnershipStore;
  const sql = yield* SqlClient.SqlClient;
  const outbox = yield* Outbox.EffectOutboxV2;
  const projects = yield* Projects.ProjectStoreV2;
  const projections = yield* Projections.ProjectionStoreV2;
  const owner = {
    commandId: CommandId.make("owner"),
    projectId,
    threadId: ThreadId.make("owner"),
    runId: RunId.make("owner"),
    cwd: identity.canonicalCwd,
    operation: "provider" as const,
  };
  const handle = yield* admission.admit(owner);
  assert.isNotNull(handle);
  const now = yield* DateTime.now;
  const occurredAt = DateTime.formatIso(now);
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
  yield* projections.apply({
    id: EventId.make("thread-created"),
    threadId,
    type: "thread.created",
    occurredAt: now,
    payload: {
      id: threadId,
      projectId,
      title: "Fixture",
      createdBy: "user",
      creationSource: "web",
      providerInstanceId: modelSelection.instanceId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: null,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    },
  });
  const run: OrchestrationV2Run = {
    id: runId,
    threadId,
    ordinal: 1,
    providerInstanceId: modelSelection.instanceId,
    modelSelection,
    providerThreadId: null,
    userMessageId: MessageId.make("message"),
    rootNodeId: null,
    activeAttemptId: RunAttemptId.make("attempt"),
    status: "starting",
    requestedAt: now,
    startedAt: null,
    completedAt: null,
    checkpointId: null,
    contextHandoffId: null,
  };
  yield* projections.apply({
    id: EventId.make("run-created"),
    threadId,
    type: "run.created",
    occurredAt: now,
    payload: run,
  });
  const conflict = yield* admission
    .admit({
      commandId: CommandId.make("competing-start"),
      projectId,
      threadId,
      runId,
      cwd: identity.canonicalCwd,
      operation: "provider",
    })
    .pipe(Effect.flip);
  assert.isDefined(conflict.conflict);
  const makeService = (
    overrides?: Partial<Ownership.CheckoutOwnershipService["Service"]>,
    scope?: Scope.Scope,
  ) =>
    (scope === undefined
      ? Layer.build(Layer.fresh(Wait.layer))
      : Layer.buildWithScope(Layer.fresh(Wait.layer), scope)
    ).pipe(
      Effect.map((context) => Context.get(context, Wait.CheckoutWaitService)),
      Effect.provideService(Ownership.CheckoutOwnershipService, { ...ownership, ...overrides }),
    );
  const service = yield* makeService(options?.ownership?.(ownership), options?.serviceScope);
  const enqueue = (command = "wait") =>
    service.wait({
      threadId,
      commandId: CommandId.make(command),
      checkoutKey: identity.checkoutKey,
      generation: handle!.generation,
    });
  const release = Effect.gen(function* () {
    yield* admission.providerDrained({
      threadId: owner.threadId,
      runId: owner.runId,
      observedStopped: true,
    });
    yield* admission.finalized({
      threadId: owner.threadId,
      runId: owner.runId,
      checkpointRef: null,
    });
  });
  return {
    admission,
    ownership,
    store,
    sql,
    outbox,
    projections,
    service,
    makeService,
    enqueue,
    release,
    run,
    handle: handle!,
  };
});

describe("CheckoutWaitService", () => {
  it.effect("inspects a foreign owner from an idle no-run thread and hides released claims", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const thread = yield* f.projections.getThread(threadId);
      const target = ThreadId.make("idle-no-run");
      yield* f.projections.apply({
        id: EventId.make("idle-thread"),
        threadId: target,
        type: "thread.created",
        occurredAt: yield* DateTime.now,
        payload: {
          ...thread,
          id: target,
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: target },
        },
      });
      const before = yield* f.service.inspect(target);
      assert.isNull(before.checkout);
      assert.strictEqual(before.owner?.handle.owner.threadId, "owner");
      assert.strictEqual(before.owner?.state, "held");
      yield* f.release;
      const after = yield* f.service.inspect(target);
      assert.isNull(after.owner);
      assert.strictEqual(
        (yield* f.projections.getRuntimeRecoveryProjection(target)).runs.length,
        0,
      );
    }).pipe(Effect.provide(layer)),
  );
  it.effect("recovers ready waiters present before its scoped listener starts", () =>
    Effect.gen(function* () {
      const scope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
      const f = yield* fixture({ serviceScope: scope });
      const waiter = yield* f.enqueue();
      yield* Scope.close(scope, Exit.void);
      yield* f.release;
      assert.strictEqual((yield* f.store.getWait(waiter.waitId))?.state, "ready");
      yield* f.makeService();
      assert.isTrue(Option.isSome(yield* f.outbox.get(waitKey("resume", waiter.waitId))));
    }).pipe(Effect.provide(layer)),
  );

  it.effect(
    "rolls back a claim when the source is cancelled between observation and the SQL callback",
    () =>
      Effect.gen(function* () {
        const reached = yield* Deferred.make<void>();
        const proceed = yield* Deferred.make<void>();
        const completed = yield* Deferred.make<void>();
        const f = yield* fixture({
          ownership: (original) => ({
            acquireWithIntent: (request) =>
              Effect.gen(function* () {
                yield* Deferred.succeed(reached, undefined);
                yield* Deferred.await(proceed);
                return yield* original.acquireWithIntent!(request);
              }).pipe(Effect.ensuring(Deferred.succeed(completed, undefined))),
          }),
        });
        const waiter = yield* f.enqueue();
        yield* f.release;
        yield* Deferred.await(reached);
        yield* f.sql`UPDATE orchestration_v2_projection_runs SET status = 'cancelled' WHERE run_id = ${runId}`;
        yield* Deferred.succeed(proceed, undefined);
        yield* Deferred.await(completed);
        assert.strictEqual((yield* f.store.read(identity.checkoutKey))?.state, "released");
        assert.isTrue(Option.isNone(yield* f.outbox.get(waitKey("resume", waiter.waitId))));
        assert.strictEqual(
          (yield* f.sql`SELECT 1 FROM orchestration_v2_workspace_receipts WHERE command_id = ${waitKey("admit", waiter.waitId)}`)
            .length,
          0,
        );
      }).pipe(Effect.provide(layer)),
  );

  it.effect("rejects a stale admitted fence before queuing provider execution", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const waiter = yield* f.enqueue();
      yield* f.release;
      yield* f.outbox.awaitAvailable;
      const claim = yield* f.store.read(identity.checkoutKey);
      assert.isNotNull(claim);
      yield* f.ownership.advanceOperation({
        commandId: CommandId.make("advance-before-resume"),
        handle: claim!.handle,
        nextOperation: "setup",
      });
      const result = yield* f.service
        .resumeWait({ threadId, waitId: waiter.waitId })
        .pipe(Effect.flip);
      assert.strictEqual(result.reason, "stale");
      assert.isTrue(Option.isNone(yield* f.outbox.get(waitKey("provider", waiter.waitId))));
    }).pipe(Effect.provide(layer)),
  );

  it.effect("resumes failed workspace preparation through the late retry bridge exactly once", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const now = yield* DateTime.now;
      const projection = yield* f.projections.getThreadProjection(threadId);
      const notice = projection.turnItems.find((item) => item.type === "system_notice");
      assert.isDefined(notice);
      const run = {
        ...f.run,
        status: "failed" as const,
        workspacePreparation: { type: "root" as const },
        completedAt: now,
      };
      yield* f.projections.apply({
        id: EventId.make("preparation-failed"),
        threadId,
        type: "run.updated",
        occurredAt: now,
        payload: run,
      });
      yield* f.projections.apply({
        id: EventId.make("preparation-error"),
        threadId,
        type: "turn-item.updated",
        occurredAt: now,
        payload: {
          ...notice!,
          id: TurnItemId.make("preparation-error"),
          type: "error",
          status: "failed",
          failure: {
            class: "provider_error",
            message: "Workspace ownership conflict",
            code: "workspace_preparation_failed",
            retryable: true,
          },
        },
      });
      const waiter = yield* f.enqueue();
      yield* f.release;
      yield* f.outbox.awaitAvailable;
      const calls = yield* Ref.make(0);
      const retry = (input: {
        readonly commandId: CommandId;
        readonly threadId: ThreadId;
        readonly runId: RunId;
      }) =>
        Effect.gen(function* () {
          assert.strictEqual(input.runId, runId);
          yield* Ref.update(calls, (n) => n + 1);
          yield* f.projections.apply({
            id: EventId.make("retry-preparation"),
            threadId,
            type: "run.updated",
            occurredAt: now,
            payload: { ...run, status: "preparing", completedAt: null },
          });
        }).pipe(
          Effect.mapError((cause) => new Admission.CheckoutAdmissionError({ threadId, cause })),
        );
      yield* f.service
        .resumeWait({ threadId, waitId: waiter.waitId })
        .pipe(Effect.provideService(Wait.CheckoutPreparedRunRetry, { retry }));
      yield* f.service
        .resumeWait({ threadId, waitId: waiter.waitId })
        .pipe(Effect.provideService(Wait.CheckoutPreparedRunRetry, { retry }));
      assert.strictEqual(yield* Ref.get(calls), 1);
      assert.isTrue(Option.isNone(yield* f.outbox.get(waitKey("provider", waiter.waitId))));
    }).pipe(Effect.provide(layer)),
  );
  it.effect("cancels before atomic acquire and makes the next durable waiter ready", () =>
    Effect.gen(function* () {
      const reached = yield* Deferred.make<void>();
      const proceed = yield* Deferred.make<void>();
      const f = yield* fixture({
        ownership: (original) => ({
          acquireWithIntent: (request) =>
            Effect.gen(function* () {
              yield* Deferred.succeed(reached, undefined);
              yield* Deferred.await(proceed);
              return yield* original.acquireWithIntent!(request);
            }),
        }),
      });
      const first = yield* f.enqueue();
      const next = yield* f.enqueue("wait-next");
      yield* f.release;
      yield* Deferred.await(reached);
      yield* f.service.cancelWait({
        threadId,
        commandId: CommandId.make("cancel-ready"),
        waitId: first.waitId,
      });
      assert.strictEqual((yield* f.store.getWait(next.waitId))?.state, "ready");
      yield* Deferred.succeed(proceed, undefined);
      yield* f.outbox.awaitAvailable;
      assert.isTrue(Option.isNone(yield* f.outbox.get(waitKey("resume", first.waitId))));
      assert.isTrue(Option.isSome(yield* f.outbox.get(waitKey("resume", next.waitId))));
    }).pipe(Effect.provide(layer)),
  );
  it.effect("rolls back the waiter if binding persistence fails", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.sql`CREATE TRIGGER fail_wait_binding BEFORE INSERT ON orchestration_v2_workspace_receipts WHEN NEW.command_type = 'workspace.wait-binding' BEGIN SELECT RAISE(ABORT, 'binding failure'); END`;
      yield* f.enqueue().pipe(Effect.flip);
      assert.strictEqual((yield* f.store.listWaiters()).length, 0);
    }).pipe(Effect.provide(layer)),
  );
  it.effect("binds waits to the server-resolved key/fence and rejects stale requests", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const stale = yield* f.service
        .wait({
          threadId,
          commandId: CommandId.make("stale"),
          checkoutKey: "client-path",
          generation: f.handle.generation,
        })
        .pipe(Effect.flip);
      assert.strictEqual(stale.reason, "stale");
      const waiter = yield* f.enqueue();
      assert.strictEqual(waiter.state, "waiting");
      assert.deepStrictEqual(waiter.input.identity, identity);
      assert.deepStrictEqual(yield* f.enqueue(), waiter);
      assert.strictEqual((yield* f.service.inspect(threadId)).waiters.length, 1);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("atomically claims a ready waiter and queues one resumable provider start", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const waiter = yield* f.enqueue();
      yield* f.release;
      yield* f.outbox.awaitAvailable;
      const intent = yield* f.outbox.get(waitKey("resume", waiter.waitId));
      assert.isTrue(Option.isSome(intent));
      assert.strictEqual(
        (yield* f.store.read(identity.checkoutKey))?.handle.owner.threadId,
        threadId,
      );
      yield* f.service.resumeWait({ threadId, waitId: waiter.waitId });
      yield* f.service.resumeWait({ threadId, waitId: waiter.waitId });
      const projection = yield* f.projections.getRuntimeRecoveryProjection(threadId);
      assert.strictEqual(
        projection.runs.find((candidate) => candidate.id === runId)?.status,
        "starting",
      );
      const effects = yield* f.outbox.listByCommandId(
        CommandId.make(waitKey("provider", waiter.waitId)),
      );
      assert.strictEqual(effects.length, 1);
      assert.deepStrictEqual(effects[0]?.request, { type: "provider-turn.start", runId });
      const providerHandle = yield* f.admission.admit({
        commandId: CommandId.make("provider-after-wait"),
        projectId,
        threadId,
        runId,
        cwd: identity.canonicalCwd,
        operation: "provider",
      });
      assert.strictEqual(providerHandle?.owner.threadId, threadId);
      const denied = yield* f.service
        .cancelWait({ threadId, commandId: CommandId.make("too-late"), waitId: waiter.waitId })
        .pipe(Effect.flip);
      assert.strictEqual(denied.reason, "wait-already-admitted");
    }).pipe(Effect.provide(layer)),
  );

  it.effect(
    "never revives a cancelled wait, including duplicate delivery and startup recovery",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const waiter = yield* f.enqueue();
        yield* f.service.cancelWait({
          threadId,
          commandId: CommandId.make("cancel"),
          waitId: waiter.waitId,
        });
        yield* f.release;
        yield* f.makeService();
        assert.strictEqual((yield* f.enqueue()).state, "cancelled");
        const result = yield* f.service
          .resumeWait({ threadId, waitId: waiter.waitId })
          .pipe(Effect.flip);
        assert.strictEqual(result.reason, "cancelled");
        assert.strictEqual((yield* f.store.read(identity.checkoutKey))?.state, "released");
        assert.isTrue(Option.isNone(yield* f.outbox.get(waitKey("resume", waiter.waitId))));
      }).pipe(Effect.provide(layer)),
  );

  it.effect("never restarts a cancelled or superseded source run", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const waiter = yield* f.enqueue();
      yield* f.sql`UPDATE orchestration_v2_projection_runs SET status = 'cancelled' WHERE run_id = ${runId}`;
      yield* f.release;
      yield* f.makeService();
      assert.isTrue(Option.isNone(yield* f.outbox.get(waitKey("resume", waiter.waitId))));
      assert.strictEqual((yield* f.store.read(identity.checkoutKey))?.state, "released");
    }).pipe(Effect.provide(layer)),
  );
});
