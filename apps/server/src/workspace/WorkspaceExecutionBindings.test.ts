import {
  CommandId,
  EnvironmentId,
  ProjectId,
  ThreadId,
  RunId,
  type CheckoutOwnershipRecord,
  WorkspaceExecutionRequestError,
  type VerificationRecord,
} from "@t3tools/contracts";
import * as Ids from "@t3tools/provider-core/server/IdAllocator";
import { expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Secrets from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import * as Environment from "../environment/ServerEnvironment.ts";
import * as ProcessRunner from "../processRunner.ts";

import * as Events from "../orchestration-v2/EventSink.ts";
import * as Launch from "../orchestration-v2/ThreadLaunchService.ts";
import * as Verification from "../verification/VerificationService.ts";
import * as VerificationEffects from "../verification/VerificationEffects.ts";
import * as Admission from "./CheckoutAdmission.ts";
import * as Recovery from "./CheckoutOwnershipRecovery.ts";
import * as Wait from "./CheckoutWaitService.ts";
import * as Api from "./WorkspaceExecutionApi.ts";
import * as Bindings from "./WorkspaceExecutionBindings.ts";

const record: VerificationRecord = {
  verificationId: "verification",
  commandId: CommandId.make("request"),
  projectId: ProjectId.make("project"),
  threadId: ThreadId.make("thread"),
  sourceRunId: null,
  environmentId: EnvironmentId.make("fixture"),
  ownerHandle: null,
  actionSnapshot: {
    actionId: "check",
    revision: "1",
    command: "check",
    shell: "sh",
    cwd: "/fixture",
    timeoutMs: 1_000,
  },
  execution: "failed",
  currentness: "Unknown",
  requestedAt: "2026-10-25T00:00:00.000Z",
  startedAt: null,
  finishedAt: "2026-10-25T00:00:01.000Z",
  exitCode: 1,
  signal: null,
  terminationReason: "nonzero-exit",
  startSource: null,
  endSource: null,
  startMutationGeneration: null,
  endMutationGeneration: null,
  observationState: "gap",
  truncated: false,
  outputReferences: [{ id: "stderr", kind: "stderr", bytes: 20 }],
};

const owner: CheckoutOwnershipRecord = {
  identity: {
    environmentId: record.environmentId,
    checkoutKey: "checkout",
    canonicalCwd: "/fixture",
    filesystemIdentity: { device: "1", file: "2" },
    gitDir: null,
    gitCommonDir: null,
  },
  handle: {
    ownershipId: "owner",
    checkoutKey: "checkout",
    generation: 1,
    serverInstanceId: "fixture",
    owner: { kind: "run", threadId: record.threadId, runId: RunId.make("run") },
    operation: "provider",
  },
  state: "held",
  acquiredAt: record.requestedAt,
  updatedAt: record.requestedAt,
};

const fixture = Effect.fn("WorkspaceExecutionBindings.test.fixture")(function (
  options: {
    readonly reconcile?: Effect.Effect<void, Recovery.CheckoutOwnershipRecoveryError>;
    readonly recover?: Effect.Effect<void, Verification.VerificationServiceError>;
    readonly fail?: boolean;
    readonly noticeFails?: boolean;
    readonly publishOnExecute?: boolean;
    readonly owner?: CheckoutOwnershipRecord | null;
  } = {},
) {
  const calls: Array<{ method: string; input: unknown }> = [];
  const notices: Parameters<Events.EventSinkV2["Service"]["commitCommand"]>[0][] = [];
  const call = <A>(method: string, input: unknown, result: A) =>
    Effect.sync(() => {
      calls.push({ method, input });
      return result;
    });
  const fail = () =>
    Effect.fail(
      new Verification.VerificationServiceError({
        reason: "policy-denied",
        cause: "secret-canary",
      }),
    );
  const services = Layer.mergeAll(
    Layer.mock(Verification.VerificationService)({
      serverInstanceId: "fixture",
      request: (input) => (options.fail ? fail() : call("request", input, record)),
      cancel: (input) => call("cancel", input, null),
      get: (input) => call("get", input, record),
      latest: (input) => call("latest", input, record),
      output: (input) =>
        call("output", input, { text: "FAIL: expected true", truncated: false, nextOffset: null }),
      executeQueued: (input) =>
        call("execute", input, undefined).pipe(
          Effect.andThen(
            options.publishOnExecute
              ? Effect.gen(function* () {
                  yield* (yield* VerificationEffects.VerificationResultObserver).publish(record);
                })
              : Effect.void,
          ),
        ),
      requestAfterFinalization: (input) => call("handoff", input, record),
      invalidateCheckout: (input) => call("invalidate", input, undefined),
      recover: () =>
        call("recover", undefined, undefined).pipe(Effect.andThen(options.recover ?? Effect.void)),
    }),
    Layer.mock(Wait.CheckoutWaitService)({
      inspect: (input) =>
        call("inspect", input, { checkout: null, owner: options.owner ?? null, waiters: [] }),
      wait: (input) => call("wait", input, null as never),
      cancelWait: (input) => call("cancelWait", input, null),
      resumeWait: (input) => call("resume", input, undefined),
    }),
    Layer.mock(Launch.ThreadLaunchService)({
      retryPreparation: (input) => call("retry", input, {} as never),
    }),
    Layer.succeed(Recovery.CheckoutOwnershipRecovery, {
      reconcile: () =>
        call("reconcile", undefined, undefined).pipe(
          Effect.andThen(options.reconcile ?? Effect.void),
        ),
    }),
    Layer.mock(Events.EventSinkV2)({
      commitCommand: (input) =>
        Effect.gen(function* () {
          notices.push(input);
          if (options.noticeFails)
            return yield* new Events.EventSinkWriteError({ eventCount: 1, cause: "secret-canary" });
          return {
            committed: true,
            storedEvents: [],
            cancelledEffectCount: 0,
            receipt: {} as never,
          };
        }),
    }),
    Ids.layer,
  );
  return Effect.succeed({ calls, notices, layer: Bindings.layer.pipe(Layer.provide(services)) });
});

it.effect("routes every API and late effect to the provided production service", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* Effect.gen(function* () {
      expect(yield* Api.WorkspaceExecutionAvailable).toBe(true);
      const api = yield* Api.WorkspaceExecutionApi;
      const threadId = record.threadId;
      const commandId = record.commandId;
      expect(yield* api.inspect(threadId)).toBeNull();
      yield* api.wait({ threadId, commandId, checkoutKey: "checkout", generation: 1 });
      yield* api.cancelWait({ threadId, commandId, waitId: "wait" });
      expect(yield* api.request({ threadId, commandId })).toBe(record);
      yield* api.cancel({ commandId, verificationId: record.verificationId });
      yield* api.get(record.verificationId);
      yield* api.latest(threadId);
      yield* api.output({
        verificationId: record.verificationId,
        referenceId: "stderr",
        offset: 0,
        limit: 20,
      });
      yield* (yield* VerificationEffects.VerificationEffects).execute({
        verificationId: record.verificationId,
        executionClaimId: "claim",
      });
      yield* (yield* Admission.CheckoutWaitEffects).resume({ threadId, waitId: "wait" });
      yield* (yield* Admission.CheckoutVerificationHandoff).invalidate("checkout");
    }).pipe(Effect.provide(f.layer));
    expect(f.calls.map((call) => call.method)).toEqual([
      "reconcile",
      "recover",
      "inspect",
      "wait",
      "cancelWait",
      "request",
      "cancel",
      "get",
      "latest",
      "output",
      "execute",
      "resume",
      "invalidate",
    ]);
  }),
);

it.effect("maps service failures to bounded API errors", () =>
  Effect.gen(function* () {
    const f = yield* fixture({ fail: true });
    const error = yield* Effect.gen(function* () {
      return yield* (yield* Api.WorkspaceExecutionApi).request({
        commandId: record.commandId,
        threadId: record.threadId,
      });
    }).pipe(Effect.provide(f.layer), Effect.flip);
    expect(error).toBeInstanceOf(WorkspaceExecutionRequestError);
    expect(error).toMatchObject({ reason: "policy" });
    expect(error.message).not.toContain("secret-canary");
    expect("cause" in error).toBe(false);
  }),
);

it.effect("inspects independent active/idle ownership and omits released claims", () =>
  Effect.gen(function* () {
    for (const state of ["held", "uncertain", "released"] as const) {
      const current = { ...owner, state };
      const f = yield* fixture({ owner: current });
      const result = yield* Effect.gen(function* () {
        return yield* (yield* Api.WorkspaceExecutionApi).inspect(record.threadId);
      }).pipe(Effect.provide(f.layer));
      expect(result).toEqual(state === "released" ? null : current);
    }
  }),
);

it.effect(
  "keeps recovery unavailable and publishes notices from recovered background attempts",
  () =>
    Effect.gen(function* () {
      const workerScope = yield* Effect.scope;
      const reconciling = yield* Deferred.make<void>();
      const releaseReconcile = yield* Deferred.make<void>();
      const recovering = yield* Deferred.make<void>();
      const releaseRecovery = yield* Deferred.make<void>();
      const completeAttempt = yield* Deferred.make<void>();
      const attemptDone = yield* Deferred.make<void>();
      const ready = yield* Deferred.make<void>();
      const unavailable = Effect.gen(function* () {
        expect(yield* Api.WorkspaceExecutionAvailable).toBe(false);
        const result = yield* (yield* Api.WorkspaceExecutionApi)
          .request({ commandId: record.commandId, threadId: record.threadId })
          .pipe(Effect.result);
        expect(result).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "WorkspaceExecutionRequestError", reason: "unavailable" },
        });
      });
      const f = yield* fixture({
        reconcile: Effect.gen(function* () {
          yield* unavailable;
          yield* Deferred.succeed(reconciling, undefined);
          yield* Deferred.await(releaseReconcile);
        }),
        recover: Effect.gen(function* () {
          yield* unavailable;
          yield* Effect.gen(function* () {
            yield* Deferred.await(completeAttempt);
            yield* (yield* VerificationEffects.VerificationResultObserver).publish(record);
            yield* Deferred.succeed(attemptDone, undefined);
          }).pipe(Effect.forkIn(workerScope, { startImmediately: false }));
          yield* Deferred.succeed(recovering, undefined);
          yield* Deferred.await(releaseRecovery);
        }),
      });
      const build = yield* Effect.gen(function* () {
        expect(yield* Api.WorkspaceExecutionAvailable).toBe(true);
        expect(
          yield* (yield* Api.WorkspaceExecutionApi).request({
            commandId: record.commandId,
            threadId: record.threadId,
          }),
        ).toBe(record);
        yield* Deferred.succeed(ready, undefined);
        yield* Deferred.await(attemptDone);
      }).pipe(Effect.provide(f.layer), Effect.forkChild);
      yield* Deferred.await(reconciling);
      expect(f.calls.map((call) => call.method)).toEqual(["reconcile"]);
      yield* Deferred.succeed(releaseReconcile, undefined);
      yield* Deferred.await(recovering);
      yield* unavailable;
      expect(f.calls.map((call) => call.method)).toEqual(["reconcile", "recover"]);
      expect(f.notices).toHaveLength(0);
      yield* Deferred.succeed(releaseRecovery, undefined);
      yield* Deferred.await(ready);
      expect(f.notices).toHaveLength(0);
      yield* Deferred.succeed(completeAttempt, undefined);
      yield* Deferred.await(attemptDone);
      yield* Fiber.join(build);
      expect(f.calls.map((call) => call.method)).toEqual([
        "reconcile",
        "recover",
        "request",
        "output",
      ]);
      expect(f.notices).toHaveLength(1);
      expect(f.notices[0]).toMatchObject({
        commandId: "verification-result:verification:failed:Unknown",
        commandType: "verification.result-notice",
        threadId: record.threadId,
        events: [
          {
            type: "turn-item.updated",
            payload: {
              type: "system_notice",
              status: "completed",
              message: expect.stringContaining("FAIL: expected true"),
            },
          },
        ],
      });
    }).pipe(Effect.scoped),
);

it.effect("publishes bounded legacy notices with stable per-outcome command IDs", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* Effect.gen(function* () {
      const observer = yield* VerificationEffects.VerificationResultObserver;
      yield* observer.publish(record);
      yield* observer.publish(record);
      yield* observer.publish({ ...record, currentness: "Stale" });
    }).pipe(Effect.provide(f.layer));
    expect(f.notices).toHaveLength(3);
    expect(f.notices[0]?.commandId).toBe(f.notices[1]?.commandId);
    expect(f.notices[0]?.commandId).not.toBe(f.notices[2]?.commandId);
    expect(f.notices[0]?.events[0]).toMatchObject({
      type: "turn-item.updated",
      payload: {
        type: "system_notice",
        status: "completed",
        message: expect.stringContaining("FAIL: expected true"),
      },
    });
    expect(f.calls.filter((call) => call.method === "output")).toHaveLength(3);
  }),
);

it.effect(
  "provides notices inside managed execute calls and keeps persistence failure nonfatal",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture({ noticeFails: true, publishOnExecute: true });
      yield* Effect.gen(function* () {
        yield* (yield* VerificationEffects.VerificationEffects).execute({
          verificationId: record.verificationId,
          executionClaimId: "claim",
        });
      }).pipe(Effect.provide(f.layer));
      expect(f.notices).toHaveLength(1);
      expect(f.calls.some((call) => call.method === "execute")).toBe(true);
    }),
);

it.effect("normalizes recovery failures before publishing any availability context", () =>
  Effect.gen(function* () {
    let advertised = false;
    const f = yield* fixture({
      recover: Effect.gen(function* () {
        expect(yield* Api.WorkspaceExecutionAvailable).toBe(false);
        return yield* new Verification.VerificationServiceError({
          reason: "unavailable",
          cause: "secret-canary",
        });
      }),
    });
    const error = yield* Effect.sync(() => {
      advertised = true;
    }).pipe(Effect.provide(f.layer), Effect.flip);
    expect(error).toMatchObject({ _tag: "WorkspaceExecutionRequestError", reason: "unavailable" });
    expect("cause" in error).toBe(false);
    expect(advertised).toBe(false);
    expect(yield* Api.WorkspaceExecutionAvailable).toBe(false);
    expect(f.calls.map((call) => call.method)).toEqual(["reconcile", "recover"]);
    expect(f.notices).toHaveLength(0);
  }),
);

it.effect("reads capability availability at descriptor-read time, after early construction", () =>
  Effect.gen(function* () {
    const environment = yield* Environment.make;
    const absent = yield* environment.getDescriptor;
    expect(absent.capabilities).not.toHaveProperty("workspaceOwnership");
    expect(absent.capabilities).not.toHaveProperty("verificationEvidence");
    const present = yield* environment.getDescriptor.pipe(
      Effect.provideService(Api.WorkspaceExecutionAvailable, true),
    );
    expect(present.capabilities).toMatchObject({
      workspaceOwnership: true,
      verificationEvidence: true,
    });
    const absentAgain = yield* environment.getDescriptor;
    expect(absentAgain.capabilities).not.toHaveProperty("workspaceOwnership");
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        ServerConfig.layerTest(process.cwd(), { prefix: "t3-workspace-bindings-" }),
        Layer.mock(Secrets.ServerSecretStore)({ get: () => Effect.succeedNone }),
        Layer.succeed(Environment.ServerEnvironmentIdentity, {
          getEnvironmentId: Effect.succeed(record.environmentId),
        }),
        Layer.mock(ProcessRunner.ProcessRunner)({
          run: () =>
            Effect.succeed({
              stdout: "",
              stderr: "",
              code: null,
              timedOut: false,
              stdoutTruncated: false,
              stderrTruncated: false,
              stdoutInvalidUtf8: false,
              stderrInvalidUtf8: false,
            }),
        }),
        Layer.succeed(HostProcess.Platform, "darwin"),
        Layer.succeed(HostProcess.Environment, {}),
      ).pipe(Layer.provideMerge(NodeServices.layer)),
    ),
    Effect.scoped,
  ),
);
