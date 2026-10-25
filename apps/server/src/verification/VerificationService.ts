// @effect-diagnostics nodeBuiltinImport:off -- SHA256 records opaque configuration identities, never secret values.
// @effect-diagnostics preferSchemaOverJson:off -- JSON frames deterministic hash inputs only.
import * as NodeCrypto from "node:crypto";
import {
  CommandId,
  OwnershipConflict,
  type OwnershipHandle,
  type RunId,
  type ThreadId,
  type VerificationRecord,
} from "@t3tools/contracts";
import { randomUuidV4 } from "@t3tools/provider-core/server/randomUuid";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { resolveProjectScripts } from "@t3tools/shared/projectScripts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import * as Environment from "../environment/ServerEnvironment.ts";
import * as Outbox from "../orchestration-v2/EffectOutbox.ts";
import * as Projects from "../orchestration-v2/ProjectStore.ts";
import * as Projections from "../orchestration-v2/ProjectionStore.ts";
import * as Policy from "../orchestration-v2/RuntimePolicy.ts";
import * as Settings from "../serverSettings.ts";
import { mapSettingsSecrets } from "../settingsSecrets.ts";
import * as Admission from "../workspace/CheckoutAdmission.ts";
import * as Identity from "../workspace/CheckoutIdentity.ts";
import * as Ownership from "../workspace/CheckoutOwnershipService.ts";
import * as Accounting from "../workspace/CheckoutProcessAccounting.ts";
import * as Invalidation from "./VerificationInvalidationService.ts";
import {
  sanitizeVerificationOutput,
  verificationCommandContainsSecrets,
} from "./VerificationOutput.ts";
import * as Runner from "./VerificationRunner.ts";
import * as Store from "./VerificationStore.ts";
import { VerificationResultObserver } from "./VerificationEffects.ts";

export class VerificationNotConfigured extends Schema.TaggedError<VerificationNotConfigured>()(
  "VerificationNotConfigured",
  {
    threadId: Schema.String,
    reason: Schema.Literals(["missing-config", "missing-action"]),
  },
) {
  override get message(): string {
    return "Verification is not configured for this destination.";
  }
}
export class VerificationConflict extends Schema.TaggedError<VerificationConflict>()(
  "VerificationConflict",
  {
    conflict: OwnershipConflict,
  },
) {
  override get message(): string {
    return "Verification requires checkout ownership.";
  }
}
export class VerificationServiceError extends Schema.TaggedError<VerificationServiceError>()(
  "VerificationServiceError",
  {
    reason: Schema.Literals([
      "unavailable",
      "destination-unavailable",
      "policy-denied",
      "unsafe-command",
      "config-changed",
      "source-run-missing",
      "source-superseded",
      "ownership-lost",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Verification cannot proceed (${this.reason}).`;
  }
}
type ServiceError =
  | VerificationNotConfigured
  | VerificationConflict
  | VerificationServiceError
  | Store.VerificationStoreError;
type Request = {
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly sourceRunId?: RunId;
};

export class VerificationService extends Context.Service<
  VerificationService,
  {
    readonly serverInstanceId: string;
    readonly request: (input: Request) => Effect.Effect<VerificationRecord, ServiceError>;
    readonly requestAfterFinalization: (
      input: Request & {
        readonly sourceRunId: RunId;
        readonly handle: OwnershipHandle;
        readonly checkpointRef: string;
      },
    ) => Effect.Effect<VerificationRecord | null, ServiceError>;
    readonly cancel: (input: {
      readonly commandId: CommandId;
      readonly verificationId: string;
    }) => Effect.Effect<VerificationRecord | null, ServiceError>;
    readonly get: (id: string) => Effect.Effect<VerificationRecord | null, ServiceError>;
    readonly latest: (threadId: ThreadId) => Effect.Effect<VerificationRecord | null, ServiceError>;
    readonly output: Store.VerificationStore["Service"]["getOutput"];
    readonly executeQueued: (input: {
      readonly verificationId: string;
      readonly executionClaimId: string;
    }) => Effect.Effect<void, ServiceError>;
    readonly invalidateCheckout: (
      checkoutKey: string,
    ) => Effect.Effect<void, Store.VerificationStoreError>;
    readonly recover: () => Effect.Effect<void, ServiceError>;
  }
>()("t3/verification/VerificationService") {}

const digest = (value: unknown) =>
  NodeCrypto.createHash("sha256").update(JSON.stringify(value)).digest("hex");
const isAdmissionError = Schema.is(Admission.CheckoutAdmissionError);
const isStoreError = Schema.is(Store.VerificationStoreError);
const isServiceError = Schema.is(VerificationServiceError);
const now = Effect.map(DateTime.now, DateTime.formatIso);
const make = Effect.gen(function* () {
  const store = yield* Store.VerificationStore;
  const runner = yield* Runner.VerificationRunner;
  const invalidation = yield* Invalidation.VerificationInvalidationService;
  const ownership = yield* Ownership.CheckoutOwnershipService;
  const admission = yield* Admission.CheckoutAdmission;
  const accounting = yield* Accounting.CheckoutProcessAccounting;
  const resolver = yield* Identity.CheckoutIdentityResolver;
  const projects = yield* Projects.ProjectStoreV2;
  const projections = yield* Projections.ProjectionStoreV2;
  const settings = yield* Settings.ServerSettingsService;
  const policy = yield* Policy.RuntimePolicyV2;
  const outbox = yield* Outbox.EffectOutboxV2;
  const sql = yield* SqlClient.SqlClient;
  const environmentId = yield* (yield* Environment.ServerEnvironmentIdentity).getEnvironmentId;
  const platform = yield* HostProcess.Platform;
  const architecture = yield* HostProcess.Architecture;
  const hostEnv = yield* HostProcess.Environment;
  const workerScope = yield* Scope.make();
  const fibers = new Map<string, Fiber.Fiber<void, Store.VerificationStoreError>>();
  let closing = false;

  const resolve = Effect.fn("VerificationService.resolve")(function* (
    input: Request,
    automaticOnly = false,
    checkpointRef?: string,
  ) {
    const projection = yield* projections
      .getThreadProjection(input.threadId)
      .pipe(
        Effect.mapError(
          (cause) => new VerificationServiceError({ reason: "destination-unavailable", cause }),
        ),
      );
    const thread = projection.thread;
    const found = yield* projects
      .get(thread.projectId)
      .pipe(
        Effect.mapError(
          (cause) => new VerificationServiceError({ reason: "destination-unavailable", cause }),
        ),
      );
    if (Option.isNone(found))
      return yield* new VerificationServiceError({ reason: "destination-unavailable" });
    const project = found.value;
    const raw = yield* settings.getSettings.pipe(
      Effect.mapError(
        (cause) => new VerificationServiceError({ reason: "destination-unavailable", cause }),
      ),
    );
    const effective = resolveProjectSettings(raw, thread.projectId, project).settings;
    const config = effective.verification;
    if (config === null || (automaticOnly && !config.automatic))
      return yield* new VerificationNotConfigured({
        threadId: input.threadId,
        reason: "missing-config",
      });
    const scripts = resolveProjectScripts(raw, { id: project.projectId, scripts: project.scripts });
    const actionId =
      (platform === "win32"
        ? config.platformActions?.windows
        : platform === "darwin"
          ? config.platformActions?.darwin
          : config.platformActions?.linux) ?? config.actionId;
    const action = scripts.find((script) => script.id === actionId);
    if (action === undefined)
      return yield* new VerificationNotConfigured({
        threadId: input.threadId,
        reason: "missing-action",
      });
    const destination = yield* policy
      .resolve({ thread, modelSelection: thread.modelSelection })
      .pipe(
        Effect.mapError(
          (cause) => new VerificationServiceError({ reason: "policy-denied", cause }),
        ),
      );
    // The process runner cannot enforce provider sandboxes or interactive approvals.
    if (
      destination.runtimeMode !== "full-access" ||
      destination.approvalPolicy !== undefined ||
      destination.sandboxPolicy !== undefined
    )
      return yield* new VerificationServiceError({ reason: "policy-denied" });
    const cwd = destination.cwd ?? thread.worktreePath ?? project.workspaceRoot;
    const identity = yield* resolver
      .resolve({ environmentId, cwd })
      .pipe(
        Effect.mapError(
          (cause) => new VerificationServiceError({ reason: "destination-unavailable", cause }),
        ),
      );
    const secrets: string[] = Object.entries(hostEnv).flatMap(([key, value]) =>
      /secret|token|password|passwd|credential|(?:api|access|private|signing|encryption)[_-]?key|authorization|cookie/i.test(
        key,
      ) && value
        ? [value]
        : [],
    );
    mapSettingsSecrets(raw, (secret) => {
      if (secret.sensitive && !secret.redacted && secret.value) secrets.push(secret.value);
      return secret;
    });
    const shell =
      platform === "win32" ? (hostEnv.ComSpec ?? "cmd.exe") : (hostEnv.SHELL ?? "/bin/sh");
    if (
      [action.command, shell, identity.canonicalCwd].some((text) =>
        verificationCommandContainsSecrets(text, secrets),
      )
    )
      return yield* new VerificationServiceError({ reason: "unsafe-command" });
    const configRevision = digest([
      config,
      action.id,
      action.command,
      shell,
      identity.canonicalCwd,
    ]);
    const toolchainRevision = digest([
      platform,
      architecture,
      hostEnv.PATH ?? "",
      shell,
      [...new Set(secrets)].sort().map((secret) => digest(secret)),
    ]);
    const latestRun = projection.runs.reduce<(typeof projection.runs)[number] | undefined>(
      (latest, run) => (latest === undefined || run.ordinal > latest.ordinal ? run : latest),
      undefined,
    );
    const sourceRun =
      input.sourceRunId === undefined
        ? latestRun
        : projection.runs.find((run) => run.id === input.sourceRunId);
    if (input.sourceRunId !== undefined && sourceRun === undefined)
      return yield* new VerificationServiceError({ reason: "source-run-missing" });
    const checkpoint = projection.checkpoints.find(
      (entry) =>
        entry.runId === sourceRun?.id &&
        entry.status === "ready" &&
        (checkpointRef === undefined || entry.ref === checkpointRef),
    );
    return {
      thread,
      config,
      configRevision,
      toolchainRevision,
      identity,
      secrets,
      sourceRun,
      latestRun,
      checkpoint,
      actionSnapshot: {
        actionId,
        revision: configRevision,
        command: action.command,
        shell,
        cwd: identity.canonicalCwd,
        timeoutMs: config.timeoutMs,
      },
    };
  });
  const build = Effect.fn("VerificationService.buildIntent")(function* (
    input: Request,
    resolved: Effect.Success<ReturnType<typeof resolve>>,
  ) {
    const verificationId = yield* randomUuidV4;
    const requestedAt = yield* now;
    const record: VerificationRecord = {
      verificationId,
      commandId: input.commandId,
      projectId: resolved.thread.projectId,
      threadId: input.threadId,
      sourceRunId: resolved.sourceRun?.id ?? null,
      environmentId,
      ownerHandle: null,
      actionSnapshot: resolved.actionSnapshot,
      execution: "queued",
      currentness: "Unknown",
      requestedAt,
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
    const executionClaimId = yield* randomUuidV4;
    return {
      record,
      persistIntent: (handle: OwnershipHandle) =>
        store.queue({
          record: { ...record, ownerHandle: handle },
          checkoutKey: resolved.identity.checkoutKey,
          sourceRunOrdinal: resolved.sourceRun?.ordinal ?? 0,
          effects: [
            {
              id: `verification:${verificationId}`,
              commandId: input.commandId,
              threadId: input.threadId,
              request: { type: "verification.execute", verificationId, executionClaimId },
            },
          ],
        }),
    };
  });
  const request: VerificationService["Service"]["request"] = Effect.fn(
    "VerificationService.request",
  )(function* (input) {
    const previous = yield* store.getByCommandId(input.commandId);
    if (previous !== null) return previous;
    if (closing || ownership.acquireWithIntent === undefined)
      return yield* new VerificationServiceError({ reason: "unavailable" });
    const resolved = yield* resolve(input);
    const intent = yield* build(input, resolved);
    const result = yield* admission
      .withVerificationAdmission(
        { threadId: input.threadId, identity: resolved.identity },
        ownership.acquireWithIntent({
          request: {
            commandId: input.commandId,
            identity: resolved.identity,
            owner: {
              kind: "verification",
              threadId: input.threadId,
              verificationId: intent.record.verificationId,
            },
            operation: "verification",
          },
          persistIntent: intent.persistIntent,
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          isAdmissionError(cause) && cause.conflict !== undefined
            ? new VerificationConflict({ conflict: cause.conflict })
            : isStoreError(cause)
              ? cause
              : new VerificationServiceError({ reason: "ownership-lost", cause }),
        ),
      );
    if (result._tag === "Conflict")
      return yield* new VerificationConflict({ conflict: result.conflict });
    yield* outbox.notifyAvailable();
    const queued = yield* store.getByCommandId(input.commandId);
    if (queued === null) return yield* new VerificationServiceError({ reason: "unavailable" });
    return queued;
  });
  const requestAfterFinalization: VerificationService["Service"]["requestAfterFinalization"] =
    Effect.fn("VerificationService.requestAfterFinalization")(function* (input) {
      const previous = yield* store.getByCommandId(input.commandId);
      if (previous !== null) return previous;
      const resolved = yield* resolve(input, true, input.checkpointRef).pipe(
        Effect.catchTags({ VerificationNotConfigured: () => Effect.succeed(null) }),
      );
      if (
        resolved === null ||
        !resolved.config.automatic ||
        resolved.sourceRun?.id !== resolved.latestRun?.id ||
        resolved.checkpoint?.ref !== input.checkpointRef ||
        resolved.checkpoint.files.length === 0
      )
        return null;
      if (
        input.handle.owner.kind !== "run" ||
        input.handle.owner.threadId !== input.threadId ||
        input.handle.owner.runId !== input.sourceRunId ||
        input.handle.checkoutKey !== resolved.identity.checkoutKey
      )
        return yield* new VerificationServiceError({ reason: "ownership-lost" });
      const intent = yield* build(input, resolved);
      yield* ownership
        .transferWithIntent({
          commandId: input.commandId,
          handle: input.handle,
          nextOwner: {
            kind: "verification",
            threadId: input.threadId,
            verificationId: intent.record.verificationId,
          },
          nextOperation: "verification",
          persistIntent: Effect.fn("VerificationService.persistFinalizationIntent")(function* (
            handle: OwnershipHandle,
          ) {
            const latest = yield* sql<{
              readonly run_id: string;
            }>`SELECT run_id FROM orchestration_v2_projection_runs WHERE thread_id = ${input.threadId} ORDER BY ordinal DESC LIMIT 1`;
            if (latest[0]?.run_id !== input.sourceRunId)
              return yield* new VerificationServiceError({ reason: "source-superseded" });
            return yield* intent.persistIntent(handle);
          }),
        })
        .pipe(
          Effect.mapError((cause) =>
            isStoreError(cause) || isServiceError(cause)
              ? cause
              : new VerificationServiceError({ reason: "ownership-lost", cause }),
          ),
          Effect.catchTags({
            VerificationServiceError: (error) =>
              error.reason === "source-superseded" ? Effect.succeed(null) : Effect.fail(error),
          }),
        );
      yield* outbox.notifyAvailable();
      return yield* store.getByCommandId(input.commandId);
    });

  const releaseUnstarted = Effect.fn("VerificationService.releaseUnstarted")(function* (
    record: VerificationRecord,
  ) {
    const handle = record.ownerHandle;
    if (handle === null) return;
    // A terminal queued CAS proves this intent never reached the shell boundary.
    const processRef = {
      id: `unstarted:${record.verificationId}`,
      creationMarker: record.verificationId,
    };
    yield* accounting
      .register({ handle, processRef, kind: "verification-unstarted" })
      .pipe(Effect.ignore);
    yield* accounting
      .mark({ owner: handle.owner, processRef, state: "stopped" })
      .pipe(Effect.ignore);
    yield* ownership
      .releaseWhenQuiescent({
        commandId: CommandId.make(`${record.verificationId}:release-unstarted`),
        handle,
      })
      .pipe(Effect.ignore);
  });
  const runAttempt = Effect.fn("VerificationService.runAttempt")(function* (input: {
    readonly verificationId: string;
    readonly executionClaimId: string;
  }) {
    const record = yield* store.get(input.verificationId);
    if (record === null || record.execution !== "queued") return;
    const resolution = yield* resolve({
      commandId: record.commandId,
      threadId: record.threadId,
      ...(record.sourceRunId === null ? {} : { sourceRunId: record.sourceRunId }),
    }).pipe(Effect.result);
    if (resolution._tag === "Failure") {
      const failed = yield* store.failQueued({
        verificationId: record.verificationId,
        commandId: CommandId.make(`${record.verificationId}:preflight`),
        now: yield* now,
        reason:
          resolution.failure._tag === "VerificationNotConfigured"
            ? "not-configured"
            : resolution.failure.reason,
      });
      if (failed?.execution === "failed") yield* releaseUnstarted(failed);
      return;
    }
    const resolved = resolution.success;
    if (
      resolved.configRevision !== record.actionSnapshot.revision ||
      resolved.identity.checkoutKey !== record.ownerHandle?.checkoutKey ||
      resolved.sourceRun?.id !== resolved.latestRun?.id
    ) {
      const failed = yield* store.failQueued({
        verificationId: record.verificationId,
        commandId: CommandId.make(`${record.verificationId}:changed`),
        now: yield* now,
        reason: "config-or-source-changed",
      });
      if (failed?.execution === "failed") yield* releaseUnstarted(failed);
      return;
    }
    const handle = record.ownerHandle;
    if (handle === null) return;
    const prepared = yield* Effect.uninterruptibleMask((restore) =>
      restore(
        runner.prepare({
          identity: resolved.identity,
          config: resolved.config,
          configRevision: resolved.configRevision,
          toolchainRevision: resolved.toolchainRevision,
          checkpointRef: resolved.checkpoint?.ref ?? null,
        }),
      ).pipe(
        Effect.tap((prepared) =>
          invalidation.retain({ verificationId: record.verificationId, lease: prepared.lease }),
        ),
      ),
    );
    // Source capture can take time. Re-resolve destination policy and settings immediately before claiming.
    const boundary = yield* resolve({
      commandId: record.commandId,
      threadId: record.threadId,
      ...(record.sourceRunId === null ? {} : { sourceRunId: record.sourceRunId }),
    }).pipe(Effect.result);
    if (
      boundary._tag === "Failure" ||
      boundary.success.configRevision !== resolved.configRevision ||
      boundary.success.toolchainRevision !== resolved.toolchainRevision ||
      boundary.success.sourceRun?.id !== boundary.success.latestRun?.id
    ) {
      yield* prepared.lease.close;
      const failed = yield* store.failQueued({
        verificationId: record.verificationId,
        commandId: CommandId.make(`${record.verificationId}:boundary-changed`),
        now: yield* now,
        reason: "execution-boundary-changed",
      });
      if (failed?.execution === "failed") yield* releaseUnstarted(failed);
      return;
    }
    yield* ownership
      .assertCurrent(handle)
      .pipe(
        Effect.mapError(
          (cause) => new VerificationServiceError({ reason: "ownership-lost", cause }),
        ),
      );
    const claimed = yield* store.claim({
      verificationId: record.verificationId,
      executionClaimId: input.executionClaimId,
      serverInstanceId: ownership.serverInstanceId,
      handle,
      startedAt: null,
      source: prepared.startSource,
      mutationGeneration: prepared.startObservation.mutationGeneration,
      observationState: prepared.startObservation.observationState,
    });
    if (claimed === null) return;
    const outcome = yield* runner
      .run({
        ...input,
        handle,
        actionSnapshot: record.actionSnapshot,
        prepared,
        secrets: resolved.secrets,
      })
      .pipe(Effect.result);
    if (outcome._tag === "Failure") {
      if (outcome.failure.reason === "ownership-lost") return yield* outcome.failure;
      const failed = yield* store.finish({
        ...input,
        commandId: CommandId.make(`${record.verificationId}:rejected-before-start`),
        completion: {
          execution: "failed",
          currentness: "Unknown",
          startedAt: null,
          finishedAt: yield* now,
          exitCode: null,
          signal: null,
          terminationReason: outcome.failure.reason,
          endSource: null,
          endMutationGeneration: null,
          observationState: "unavailable",
          outputReferences: [],
          truncated: false,
        },
      });
      if (failed?.execution === "failed") yield* releaseUnstarted(failed);
      return;
    }
    const result = outcome.success;
    yield* Effect.uninterruptible(
      Effect.gen(function* () {
        const sanitized = sanitizeVerificationOutput({
          stdout: result.stdout,
          stderr: result.stderr,
          diagnostic: result.diagnostic,
          secrets: resolved.secrets,
        });
        const outputReferences: VerificationRecord["outputReferences"][number][] = [];
        yield* sql
          .withTransaction(
            Effect.gen(function* () {
              for (const kind of ["stdout", "stderr", "diagnostic"] as const) {
                const text = sanitized[kind];
                if (!text) continue;
                const referenceId = `${record.verificationId}:${kind}`;
                yield* store.putOutput({
                  verificationId: record.verificationId,
                  referenceId,
                  kind,
                  text,
                  truncated: result.completion.truncated || sanitized.truncated,
                });
                outputReferences.push({ id: referenceId, kind, bytes: Buffer.byteLength(text) });
              }
              yield* store.finish({
                verificationId: record.verificationId,
                executionClaimId: input.executionClaimId,
                commandId: CommandId.make(`${record.verificationId}:completed`),
                completion: {
                  ...result.completion,
                  startedAt: result.startedAt,
                  outputReferences,
                  truncated: result.completion.truncated || sanitized.truncated,
                },
              });
            }),
          )
          .pipe(
            Effect.mapError((cause) =>
              isStoreError(cause)
                ? cause
                : new Store.VerificationStoreError({ operation: "complete", cause }),
            ),
          );
        yield* invalidation.revalidate(record.verificationId);
        yield* ownership
          .releaseWhenQuiescent({
            commandId: CommandId.make(`${record.verificationId}:release`),
            handle,
          })
          .pipe(Effect.ignore);
        const completed = yield* store.get(record.verificationId);
        if (completed !== null) yield* (yield* VerificationResultObserver).publish(completed);
      }),
    );
  });
  const settleFailure = Effect.fn("VerificationService.settleFailure")(function* (input: {
    readonly verificationId: string;
    readonly executionClaimId: string;
  }) {
    const record = yield* store.get(input.verificationId);
    if (record === null) return;
    const requested = yield* store.isCancellationRequested(input.verificationId);
    if (record.execution === "queued") {
      const failed = yield* store.failQueued({
        verificationId: record.verificationId,
        commandId: CommandId.make(`${record.verificationId}:prepare-failed`),
        now: yield* now,
        reason: "preparation-failed",
      });
      if (failed?.execution === "failed") yield* releaseUnstarted(failed);
    } else if (record.execution === "cancelled" && record.startedAt === null) {
      yield* releaseUnstarted(record);
    } else if (record.execution === "running") {
      yield* store.finish({
        ...input,
        commandId: CommandId.make(`${record.verificationId}:execution-ended`),
        completion: {
          execution: requested ? "cancelled" : "interrupted",
          currentness: "Unknown",
          finishedAt: yield* now,
          exitCode: null,
          signal: null,
          terminationReason: requested ? "cancelled" : "execution-outcome-unavailable",
          endSource: null,
          endMutationGeneration: null,
          observationState: "gap",
          outputReferences: [],
          truncated: false,
        },
      });
      if (record.ownerHandle)
        yield* ownership
          .releaseWhenQuiescent({
            commandId: CommandId.make(`${record.verificationId}:release`),
            handle: record.ownerHandle,
          })
          .pipe(Effect.ignore);
    }
  });
  const executeQueued: VerificationService["Service"]["executeQueued"] = Effect.fn(
    "VerificationService.executeQueued",
  )(function* (input) {
    if (closing || fibers.has(input.verificationId)) return;
    const record = yield* store.get(input.verificationId);
    if (record?.execution !== "queued") return;
    yield* Effect.uninterruptible(
      Effect.gen(function* () {
        if (closing || fibers.has(input.verificationId)) return;
        const fiber = yield* runAttempt(input).pipe(
          Effect.onExit((exit) => (Exit.isFailure(exit) ? settleFailure(input) : Effect.void)),
          Effect.ignore,
          Effect.interruptible,
          Effect.ensuring(
            Effect.sync(() => {
              fibers.delete(input.verificationId);
            }),
          ),
          Effect.forkIn(workerScope, { startImmediately: false }),
        );
        fibers.set(input.verificationId, fiber);
      }),
    );
  });
  const cancel: VerificationService["Service"]["cancel"] = Effect.fn("VerificationService.cancel")(
    function* (input) {
      return yield* Effect.uninterruptible(
        Effect.gen(function* () {
          const record = yield* store.requestCancel({ ...input, now: yield* now });
          const fiber = fibers.get(input.verificationId);
          if (fiber) yield* Fiber.interrupt(fiber);
          else if (record?.execution === "cancelled" && record.startedAt === null)
            yield* releaseUnstarted(record);
          return yield* store.get(input.verificationId);
        }),
      );
    },
  );
  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      closing = true;
      for (const id of fibers.keys())
        yield* store
          .requestCancel({
            verificationId: id,
            commandId: CommandId.make(`${id}:scope-cancel`),
            now: yield* now,
          })
          .pipe(Effect.ignore);
      yield* Scope.close(workerScope, Exit.void);
    }),
  );
  const get: VerificationService["Service"]["get"] = Effect.fn("VerificationService.get")(
    function* (id) {
      const record = yield* store.get(id);
      if (record === null || record.startSource === null) return record;
      const resolved = yield* resolve({
        commandId: record.commandId,
        threadId: record.threadId,
        ...(record.sourceRunId === null ? {} : { sourceRunId: record.sourceRunId }),
      }).pipe(Effect.result);
      if (resolved._tag === "Failure") {
        yield* invalidation.observeInvalidation({
          verificationId: id,
          reason:
            resolved.failure._tag === "VerificationNotConfigured" ? "action-config" : "server-gap",
        });
      } else {
        const current = resolved.success;
        if (current.sourceRun?.id !== current.latestRun?.id)
          yield* invalidation.observeInvalidation({ verificationId: id, reason: "newer-run" });
        if (current.configRevision !== record.startSource.configRevision)
          yield* invalidation.observeInvalidation({ verificationId: id, reason: "action-config" });
        if (current.toolchainRevision !== record.startSource.toolchainRevision)
          yield* invalidation.observeInvalidation({ verificationId: id, reason: "toolchain" });
        if (current.identity.checkoutKey !== record.startSource.checkoutIdentity.checkoutKey)
          yield* invalidation.observeInvalidation({ verificationId: id, reason: "source" });
        const owner = yield* ownership
          .inspect({ identity: current.identity })
          .pipe(Effect.orElseSucceed(() => null));
        if (
          owner !== null &&
          owner.state !== "released" &&
          owner.handle.ownershipId !== record.ownerHandle?.ownershipId
        )
          yield* invalidation.observeInvalidation({
            verificationId: id,
            reason: "ownership-conflict",
          });
      }
      return yield* invalidation.revalidate(id);
    },
  );
  const latest: VerificationService["Service"]["latest"] = Effect.fn("VerificationService.latest")(
    function* (threadId) {
      const record = yield* store.latestForThread(threadId);
      return record ? yield* get(record.verificationId) : null;
    },
  );
  const recover: VerificationService["Service"]["recover"] = Effect.fn(
    "VerificationService.recover",
  )(function* () {
    const interrupted = yield* store.interruptRunning({
      serverInstanceId: ownership.serverInstanceId,
      now: yield* now,
    });
    for (const record of interrupted)
      if (record.ownerHandle)
        yield* ownership.reconcileClaim(record.ownerHandle).pipe(Effect.ignore);
    for (const record of yield* store.listEvidence) {
      if (record.execution !== "queued" && !fibers.has(record.verificationId))
        yield* invalidation.observeInvalidation({
          verificationId: record.verificationId,
          reason: "server-gap",
        });
    }
    for (const record of yield* store.listRecoverable) {
      if (record.execution !== "queued") continue;
      if (record.ownerHandle?.serverInstanceId !== ownership.serverInstanceId) {
        const failed = yield* store.failQueued({
          verificationId: record.verificationId,
          commandId: CommandId.make(`${record.verificationId}:restart-before-start`),
          now: yield* now,
          reason: "server-restart-before-start",
        });
        if (failed?.ownerHandle) {
          const processRef = {
            id: `unstarted:${record.verificationId}`,
            creationMarker: record.verificationId,
          };
          yield* accounting
            .register({ handle: failed.ownerHandle, processRef, kind: "verification-unstarted" })
            .pipe(Effect.ignore);
          yield* accounting
            .mark({ owner: failed.ownerHandle.owner, processRef, state: "stopped" })
            .pipe(Effect.ignore);
          yield* ownership.reconcileClaim(failed.ownerHandle).pipe(Effect.ignore);
        }
        continue;
      }
      const effects = yield* outbox
        .listByCommandId(record.commandId)
        .pipe(
          Effect.mapError(
            (cause) => new Store.VerificationStoreError({ operation: "recover-outbox", cause }),
          ),
        );
      const effect = effects.find((entry) => entry.request.type === "verification.execute");
      if (effect?.request.type === "verification.execute") yield* executeQueued(effect.request);
    }
  });
  const settingChanges = yield* settings.subscribeChanges;
  yield* Stream.runForEach(settingChanges, () =>
    Effect.gen(function* () {
      for (const record of yield* store.listEvidence) yield* get(record.verificationId);
    }),
  ).pipe(Effect.ignore, Effect.forkIn(workerScope));
  return VerificationService.of({
    serverInstanceId: ownership.serverInstanceId,
    request,
    requestAfterFinalization,
    cancel,
    get,
    latest,
    output: store.getOutput,
    executeQueued,
    invalidateCheckout: Effect.fn("VerificationService.invalidateCheckout")((checkoutKey) =>
      invalidation.observeInvalidation({ checkoutKey, reason: "source" }),
    ),
    recover,
  });
});

export const layer = Layer.effect(VerificationService, make);
