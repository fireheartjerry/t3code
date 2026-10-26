import {
  CheckoutIdentity,
  CommandId,
  OwnershipOwner,
  ThreadId,
  TurnItemId,
  ORCHESTRATION_V2_WORKSPACE_PREPARATION_FAILURE_CODE,
  type AcquireCheckoutOwnershipInput,
  type CheckoutOwnershipRecord,
  type OrchestrationV2Run,
  type OwnershipHandle,
  type ProjectId,
  type ServerSettings,
} from "@t3tools/contracts";
import * as Ids from "@t3tools/provider-core/server/IdAllocator";
import { randomUuidV4 } from "@t3tools/provider-core/server/randomUuid";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Environment from "../environment/ServerEnvironment.ts";
import * as Events from "../orchestration-v2/EventSink.ts";
import * as Projections from "../orchestration-v2/ProjectionStore.ts";
import * as Projects from "../orchestration-v2/ProjectStore.ts";
import * as Settings from "../serverSettings.ts";
import * as Admission from "./CheckoutAdmission.ts";
import * as Identity from "./CheckoutIdentity.ts";
import * as Ownership from "./CheckoutOwnershipService.ts";
import * as Store from "./CheckoutOwnershipStore.ts";
import * as Accounting from "./CheckoutProcessAccounting.ts";
import * as Recovery from "./CheckoutOwnershipRecovery.ts";

export interface CheckoutAdmissionInspection {
  readonly request: AcquireCheckoutOwnershipInput;
  readonly run: OrchestrationV2Run;
  readonly claim: CheckoutOwnershipRecord | null;
}
export class CheckoutAdmissionLive extends Context.Service<
  CheckoutAdmissionLive,
  {
    readonly admission: Context.Service.Shape<typeof Admission.CheckoutAdmission>;
    readonly inspect: (
      threadId: ThreadId,
    ) => Effect.Effect<CheckoutAdmissionInspection | null, Admission.CheckoutAdmissionError>;
    readonly inspectOwner: (
      threadId: ThreadId,
    ) => Effect.Effect<CheckoutOwnershipRecord | null, Admission.CheckoutAdmissionError>;
    readonly registerAdmitted: (
      handle: OwnershipHandle,
    ) => Effect.Effect<void, Admission.CheckoutAdmissionError>;
    readonly withGate: <A, E, R>(
      input: {
        readonly threadId: ThreadId;
        readonly identity: CheckoutIdentity;
        readonly owner?: OwnershipOwner;
      },
      effect: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | Admission.CheckoutAdmissionError, R>;
  }
>()("t3/workspace/CheckoutAdmissionLive") {}

const MAX_ACTIVITY = 256;
class CheckoutActivityObservationGap extends Schema.TaggedError<CheckoutActivityObservationGap>()(
  "CheckoutActivityObservationGap",
  { limit: Schema.Number },
) {
  override get message(): string {
    return "Passive checkout activity exceeded the observation limit; protected starts are blocked.";
  }
}
const sameIdentity = Schema.toEquivalence(CheckoutIdentity);
const sameOwner = Schema.toEquivalence(OwnershipOwner);
const isAdmissionError = Schema.is(Admission.CheckoutAdmissionError);
const isOwnershipError = Schema.is(
  Schema.Union([
    Store.StaleOwnershipHandle,
    Ownership.CheckoutOwnershipServiceError,
    Identity.CheckoutIdentityError,
    Store.CheckoutOwnershipStoreError,
  ]),
);
const encodeKey = Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)));
const command = (kind: string, ...parts: ReadonlyArray<string>) =>
  CommandId.make(`checkout-admission:${encodeKey([kind, ...parts])}`);
const noticeId = (threadId: ThreadId, runId: string) =>
  TurnItemId.make(`checkout-admission:${encodeKey(["notice", threadId, runId])}`);
type WriterInput = Omit<Admission.RunAdmissionInput, "projectId"> & {
  readonly projectId: ProjectId | null;
};
type Activity = {
  readonly owner: OwnershipOwner;
  readonly commandId: CommandId;
  readonly cwd: string;
  readonly projectId: ProjectId | null;
  handle: OwnershipHandle | null;
  providerStopped: boolean;
  finalized: boolean;
  checkpointRef: string | null;
  uncertain: boolean;
  settling: boolean;
  settled: boolean;
  terminalObserved: boolean;
  setupObservationGap: boolean;
};
type Terminal = {
  readonly activity: Activity;
  readonly cwd: string;
  readonly reservation: Accounting.OwnedProcessIdentity;
  process: Accounting.OwnedProcessIdentity | null;
  state: "live" | "stopped" | "unknown";
};

const make = Effect.gen(function* () {
  const ownership = yield* Ownership.CheckoutOwnershipService;
  const store = yield* Store.CheckoutOwnershipStore;
  const recovery = yield* Recovery.CheckoutOwnershipRecovery;
  const resolver = yield* Identity.CheckoutIdentityResolver;
  const accounting = yield* Accounting.CheckoutProcessAccounting;
  const settings = yield* Settings.ServerSettingsService;
  const environment = yield* Environment.ServerEnvironmentIdentity;
  const projects = yield* Projects.ProjectStoreV2;
  const projections = yield* Projections.ProjectionStoreV2;
  const events = yield* Events.EventSinkV2;
  const ids = yield* Ids.IdAllocatorV2;
  const locks = yield* KeyedLock.make<string>();
  const registration = yield* Semaphore.make(1);
  const activity = new Map<string, Activity>();
  const terminals = new Map<string, Terminal>();
  const blocked = new Map<ThreadId, AcquireCheckoutOwnershipInput>();
  let pendingProtected = 0;
  let observationGap = false;

  const rootRef = (entry: Activity): Accounting.OwnedProcessIdentity => ({
    id: encodeKey(["execution", entry.handle?.ownershipId ?? Store.ownerKey(entry.owner)]),
    creationMarker: ownership.serverInstanceId,
  });
  const terminalKey = (threadId: string, terminalId: string) => encodeKey([threadId, terminalId]);
  const error = (threadId: ThreadId, cause?: unknown) =>
    new Admission.CheckoutAdmissionError({ threadId, ...(cause === undefined ? {} : { cause }) });
  const wrap = <A, E, R>(threadId: ThreadId, effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.mapError((cause) => (isAdmissionError(cause) ? cause : error(threadId, cause))),
    );
  const enabled = (snapshot: ServerSettings, projectId: ProjectId | null) => {
    const policy = resolveProjectSettings(snapshot, projectId).settings;
    return policy.workspaceOwnership === "enforce" || policy.verification !== null;
  };
  const track = (input: {
    owner: OwnershipOwner;
    commandId: CommandId;
    cwd: string;
    projectId: ProjectId | null;
  }) => {
    const key = Store.ownerKey(input.owner);
    const existing = activity.get(key);
    if (existing !== undefined)
      return existing.handle === null && existing.cwd !== input.cwd
        ? Effect.fail(error(input.owner.threadId))
        : Effect.succeed(existing);
    if (activity.size >= MAX_ACTIVITY) {
      observationGap = true;
      return Effect.fail(
        error(input.owner.threadId, new CheckoutActivityObservationGap({ limit: MAX_ACTIVITY })),
      );
    }
    const entry: Activity = {
      ...input,
      handle: null,
      providerStopped: false,
      finalized: false,
      checkpointRef: null,
      uncertain: false,
      settling: false,
      settled: false,
      terminalObserved: false,
      setupObservationGap: false,
    };
    activity.set(key, entry);
    return Effect.succeed(entry);
  };
  const rememberPassive = Effect.fn("CheckoutAdmissionLive.rememberPassive")(function* (
    input: Parameters<typeof track>[0],
  ) {
    const existing = activity.get(Store.ownerKey(input.owner));
    if (existing !== undefined) {
      if (existing.cwd !== input.cwd) observationGap = true;
      return;
    }
    if (activity.size >= MAX_ACTIVITY) {
      observationGap = true;
      return;
    }
    yield* track(input);
  });
  const registerProcess = Effect.fn("CheckoutAdmissionLive.registerProcess")(function* (
    entry: Activity,
    processRef: Accounting.OwnedProcessIdentity,
    state: "live" | "stopped" | "unknown",
    kind: string,
  ) {
    const handle = entry.handle;
    if (handle === null) return;
    yield* accounting.register({ handle, processRef, kind });
    yield* accounting.mark({ handle, processRef, state });
  });
  const attach = Effect.fn("CheckoutAdmissionLive.attach")(function* (
    entry: Activity,
    handle: OwnershipHandle,
  ) {
    entry.handle = handle;
    yield* registerProcess(
      entry,
      rootRef(entry),
      entry.uncertain ? "unknown" : "live",
      "execution-reservation",
    );
    for (const terminal of terminals.values()) {
      if (terminal.activity !== entry) continue;
      yield* registerProcess(
        entry,
        terminal.reservation,
        terminal.process === null ? terminal.state : "stopped",
        "terminal-reservation",
      );
      if (terminal.process !== null)
        yield* registerProcess(entry, terminal.process, terminal.state, "terminal");
    }
  });
  const invalidate = Effect.fn("CheckoutAdmissionLive.invalidate")(function* (checkoutKey: string) {
    yield* (yield* Admission.CheckoutVerificationHandoff).invalidate(checkoutKey);
  });
  const notice = Effect.fn("CheckoutAdmissionLive.notice")(function* (
    input: Pick<Admission.RunAdmissionInput, "threadId" | "runId">,
    failure: Admission.CheckoutAdmissionError,
  ) {
    if (input.runId === null) return;
    const projection = yield* projections.getRuntimeRecoveryProjection(input.threadId);
    const run = projection.runs.find((candidate) => candidate.id === input.runId);
    if (run === undefined) return;
    const now = yield* DateTime.now;
    const notification = {
      id: yield* ids.allocate.event({ threadId: input.threadId }),
      threadId: input.threadId,
      runId: run.id,
      occurredAt: now,
      type: "turn-item.updated" as const,
      payload: {
        id: noticeId(input.threadId, run.id),
        threadId: input.threadId,
        runId: run.id,
        nodeId: run.rootNodeId,
        providerThreadId: run.providerThreadId,
        providerTurnId: null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 0,
        type: "system_notice" as const,
        status: "completed" as const,
        title: "Checkout ownership",
        message: failure.message,
        startedAt: now,
        completedAt: now,
        updatedAt: now,
      },
    };
    if (run.status === "starting" && run.activeAttemptId !== null) {
      yield* events.writeIfRunCurrent({
        threadId: input.threadId,
        runId: run.id,
        activeAttemptId: run.activeAttemptId,
        expectedStatus: "starting",
        events: [
          notification,
          {
            id: yield* ids.allocate.event({ threadId: input.threadId }),
            threadId: input.threadId,
            runId: run.id,
            occurredAt: now,
            type: "run.updated",
            payload: { ...run, status: "waiting" },
          },
        ],
      });
    } else yield* events.write({ events: [notification] });
  });
  const reconcileActivity = Effect.fn("CheckoutAdmissionLive.reconcileActivity")(function* (
    threadId: ThreadId,
    identity: CheckoutIdentity,
    owner?: OwnershipOwner,
  ) {
    yield* recovery.reconcile();
    if (observationGap)
      return yield* error(threadId, new CheckoutActivityObservationGap({ limit: MAX_ACTIVITY }));
    if ((yield* projections.getUnreadableThreadIds()).length > 0) return yield* error(threadId);
    const candidates = yield* projections.getRecoveryThreadIds("runtime");
    if (candidates.length > MAX_ACTIVITY || activity.size > MAX_ACTIVITY)
      return yield* error(threadId);
    for (const candidate of candidates) {
      const projection = yield* projections.getRuntimeRecoveryProjection(candidate);
      for (const run of projection.runs) {
        if (
          !["preparing", "starting", "running", "waiting"].includes(run.status) &&
          !projection.nodes.some(
            (node) => node.runId === run.id && ["running", "waiting"].includes(node.status),
          )
        )
          continue;
        const runOwner: OwnershipOwner = { kind: "run", threadId: candidate, runId: run.id };
        if (
          (owner !== undefined && sameOwner(owner, runOwner)) ||
          activity.has(Store.ownerKey(runOwner))
        )
          continue;
        if (
          run.status === "waiting" &&
          projection.turnItems.some(
            (item) =>
              item.runId === run.id &&
              item.type === "system_notice" &&
              item.id === noticeId(run.threadId, run.id),
          )
        )
          continue;
        const project = yield* projects.get(projection.thread.projectId);
        const cwd = projection.thread.worktreePath ?? Option.getOrNull(project)?.workspaceRoot;
        if (cwd === undefined) return yield* error(threadId);
        const entry = yield* track({
          owner: runOwner,
          cwd,
          projectId: projection.thread.projectId,
          commandId: command("recovered", candidate, run.id),
        });
        entry.uncertain = true;
      }
    }
    for (const entry of activity.values()) {
      if (
        entry.settled ||
        entry.handle !== null ||
        (owner !== undefined && sameOwner(owner, entry.owner))
      )
        continue;
      const resolved = yield* resolver.resolve({
        environmentId: identity.environmentId,
        cwd: entry.cwd,
      });
      if (resolved.checkoutKey !== identity.checkoutKey) continue;
      const result = yield* ownership.acquire({
        commandId: command("reconcile", entry.commandId, yield* randomUuidV4),
        identity: resolved,
        owner: entry.owner,
        operation: entry.owner.kind === "terminal" ? "terminal" : "provider",
      });
      if (result._tag === "Conflict")
        return yield* new Admission.CheckoutAdmissionError({ threadId, conflict: result.conflict });
      yield* attach(entry, result.handle);
    }
  });
  const withGate: CheckoutAdmissionLive["Service"]["withGate"] = Effect.fn(
    "CheckoutAdmissionLive.withGate",
  )(function* <A, E, R>(
    input: {
      readonly threadId: ThreadId;
      readonly identity: CheckoutIdentity;
      readonly owner?: OwnershipOwner;
    },
    effect: Effect.Effect<A, E, R>,
  ): Effect.fn.Return<A, E | Admission.CheckoutAdmissionError, R> {
    yield* registration.withPermit(
      Effect.sync(() => {
        pendingProtected++;
      }),
    );
    return yield* locks
      .withLock(
        input.identity.checkoutKey,
        Effect.gen(function* () {
          yield* wrap(
            input.threadId,
            Effect.gen(function* () {
              const current = yield* resolver.resolve({
                environmentId: yield* environment.getEnvironmentId,
                cwd: input.identity.canonicalCwd,
              });
              if (!sameIdentity(current, input.identity)) return yield* error(input.threadId);
              yield* invalidate(current.checkoutKey);
              yield* reconcileActivity(input.threadId, current, input.owner);
            }),
          );
          return yield* effect;
        }),
      )
      .pipe(
        Effect.ensuring(
          registration.withPermit(
            Effect.sync(() => {
              pendingProtected--;
            }),
          ),
        ),
      );
  });
  const admit = Effect.fn("CheckoutAdmissionLive.admit")(function* (input: WriterInput) {
    const owner: OwnershipOwner =
      input.runId === null
        ? {
            kind: "terminal",
            threadId: input.threadId,
            terminalId: input.setupTerminalId ?? encodeKey(["unobserved", input.commandId]),
          }
        : { kind: "run", threadId: input.threadId, runId: input.runId };
    const plan = yield* wrap(
      input.threadId,
      settings.withSettingsSnapshot((snapshot) =>
        registration.withPermit(
          Effect.gen(function* () {
            const projectId =
              input.projectId === null &&
              Object.values(snapshot.projectSettingsOverrides).some(
                (override) =>
                  override.workspaceOwnership !== undefined || override.verification !== undefined,
              )
                ? (yield* projections.getThread(input.threadId)).projectId
                : input.projectId;
            const enforce = enabled(snapshot, projectId);
            const claims = yield* store.listRecoverable;
            if (!enforce && pendingProtected === 0 && claims.length === 0) {
              yield* rememberPassive({
                owner,
                commandId: input.commandId,
                cwd: input.cwd,
                projectId,
              });
              return { passive: true as const };
            }
            if (enforce && input.runId === null && input.setupTerminalId === undefined)
              return yield* error(input.threadId);
            pendingProtected++;
            return { passive: false as const, enforce, projectId };
          }),
        ),
      ),
    );
    if (plan.passive) return null;
    return yield* wrap(
      input.threadId,
      Effect.gen(function* () {
        const identity = yield* resolver.resolve({
          environmentId: yield* environment.getEnvironmentId,
          cwd: input.cwd,
        });
        return yield* locks.withLock(
          identity.checkoutKey,
          wrap(
            input.threadId,
            Effect.gen(function* () {
              yield* invalidate(identity.checkoutKey);
              yield* reconcileActivity(input.threadId, identity, owner);
              const known = activity.get(Store.ownerKey(owner));
              if (
                known?.handle !== null &&
                known?.handle !== undefined &&
                known.handle.checkoutKey !== identity.checkoutKey
              )
                return yield* error(input.threadId);
              const current = yield* store.read(identity.checkoutKey);
              if (
                current !== null &&
                current.state !== "released" &&
                sameOwner(current.handle.owner, owner)
              ) {
                yield* ownership.assertCurrent(current.handle);
                const handle =
                  current.handle.operation === input.operation
                    ? current.handle
                    : yield* ownership.advanceOperation({
                        commandId: command(
                          "operation",
                          input.commandId,
                          String(current.handle.generation),
                          input.operation,
                        ),
                        handle: current.handle,
                        nextOperation: input.operation,
                      });
                const entry = yield* track({
                  owner,
                  commandId: input.commandId,
                  cwd: input.cwd,
                  projectId: plan.projectId,
                });
                if (entry.handle === null || entry.handle.generation !== handle.generation) {
                  if (input.operation === "provider" || input.operation === "setup") {
                    entry.providerStopped = false;
                    entry.finalized = false;
                  }
                  yield* attach(entry, handle);
                }
                blocked.delete(input.threadId);
                return handle;
              }
              if (!plan.enforce && (current === null || current.state === "released")) {
                yield* track({
                  owner,
                  commandId: input.commandId,
                  cwd: input.cwd,
                  projectId: plan.projectId,
                });
                return null;
              }
              const request: AcquireCheckoutOwnershipInput = {
                commandId: input.commandId,
                identity,
                owner,
                operation: input.operation,
              };
              const result = yield* ownership.acquire(request);
              if (result._tag === "Conflict") {
                if (blocked.size >= MAX_ACTIVITY && !blocked.has(input.threadId))
                  return yield* error(input.threadId);
                blocked.set(input.threadId, request);
                return yield* new Admission.CheckoutAdmissionError({
                  threadId: input.threadId,
                  conflict: result.conflict,
                });
              }
              const entry = yield* track({
                owner,
                commandId: input.commandId,
                cwd: input.cwd,
                projectId: plan.projectId,
              });
              yield* attach(entry, result.handle);
              blocked.delete(input.threadId);
              return result.handle;
            }),
          ),
        );
      }),
    ).pipe(
      Effect.ensuring(
        registration.withPermit(
          Effect.sync(() => {
            pendingProtected--;
          }),
        ),
      ),
      Effect.tapError((failure) =>
        notice(input, failure).pipe(
          Effect.catchCause(() =>
            Effect.logWarning("Could not persist checkout admission explanation", {
              threadId: input.threadId,
            }),
          ),
        ),
      ),
    );
  });
  const finish = Effect.fn("CheckoutAdmissionLive.finish")(function* (entry: Activity) {
    if (
      !entry.providerStopped ||
      !entry.finalized ||
      entry.settled ||
      entry.settling ||
      entry.setupObservationGap
    )
      return;
    entry.settling = true;
    yield* Effect.gen(function* () {
      const open = [...terminals.values()].some(
        (terminal) => terminal.activity === entry && terminal.state !== "stopped",
      );
      if (entry.handle === null) {
        if (!open) {
          entry.settled = true;
          activity.delete(Store.ownerKey(entry.owner));
        }
        return;
      }
      const initial = entry.handle;
      yield* locks.withLock(
        initial.checkoutKey,
        Effect.gen(function* () {
          yield* registerProcess(entry, rootRef(entry), "stopped", "execution-reservation");
          if (open || (yield* accounting.observe(entry.handle!)).state !== "stopped") return;
          const record = yield* store.read(initial.checkoutKey);
          if (
            record === null ||
            record.state === "released" ||
            record.handle.ownershipId !== initial.ownershipId ||
            !sameOwner(record.handle.owner, entry.owner)
          )
            return;
          entry.handle = record.handle;
          const hook = yield* Admission.CheckoutVerificationHandoff;
          if (
            entry.owner.kind === "run" &&
            entry.checkpointRef !== null &&
            (yield* hook.request({
              commandId: command("finalized", record.handle.ownershipId),
              threadId: entry.owner.threadId,
              sourceRunId: entry.owner.runId,
              handle: record.handle,
              checkpointRef: entry.checkpointRef,
            }))
          )
            entry.settled = true;
          else
            entry.settled =
              (yield* ownership.releaseWhenQuiescent({
                commandId: command(
                  "release",
                  record.handle.ownershipId,
                  String(record.handle.generation),
                ),
                handle: record.handle,
              })) === "released";
          if (entry.settled) activity.delete(Store.ownerKey(entry.owner));
        }),
      );
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          entry.settling = false;
        }),
      ),
    );
  });
  const observeFailure = <A, E>(effect: Effect.Effect<A, E>) =>
    effect.pipe(
      Effect.asVoid,
      Effect.catchCause(() => Effect.logWarning("Checkout process observation remains uncertain")),
    );
  const providerDrained: Context.Service.Shape<
    typeof Admission.CheckoutAdmission
  >["providerDrained"] = Effect.fn("CheckoutAdmissionLive.providerDrained")(function* (input) {
    const entry = activity.get(
      Store.ownerKey({ kind: "run", threadId: input.threadId, runId: input.runId }),
    );
    if (entry === undefined) return;
    entry.providerStopped = input.observedStopped;
    if (!input.observedStopped) {
      entry.uncertain = true;
      yield* observeFailure(
        registerProcess(entry, rootRef(entry), "unknown", "execution-reservation"),
      );
    }
    yield* observeFailure(finish(entry));
  });
  const finalized: Context.Service.Shape<typeof Admission.CheckoutAdmission>["finalized"] =
    Effect.fn("CheckoutAdmissionLive.finalized")(function* (input) {
      const entry = activity.get(
        Store.ownerKey({ kind: "run", threadId: input.threadId, runId: input.runId }),
      );
      if (entry === undefined) return;
      entry.finalized = true;
      entry.checkpointRef = input.checkpointRef;
      yield* observeFailure(finish(entry));
    });
  const terminalOpen: Context.Service.Shape<typeof Admission.CheckoutAdmission>["terminalOpen"] =
    Effect.fn("CheckoutAdmissionLive.terminalOpen")(function* (input) {
      const threadId = ThreadId.make(input.threadId);
      const key = terminalKey(input.threadId, input.terminalId);
      const previous = terminals.get(key);
      if (previous !== undefined && previous.process !== null && previous.state === "unknown") {
        if (previous.activity.handle !== null) return yield* error(threadId);
        // Legacy sessions may continue while policy is off, but a respawn cannot
        // erase descendants whose termination was never observed.
        observationGap = true;
      }
      const active = yield* Admission.ActiveCheckoutOwnership;
      let entry = previous?.state !== "stopped" ? previous?.activity : undefined;
      if (entry !== undefined && entry.cwd !== input.cwd) {
        const resolved = yield* wrap(
          threadId,
          resolver.resolve({ environmentId: yield* environment.getEnvironmentId, cwd: input.cwd }),
        );
        const original = yield* wrap(
          threadId,
          resolver.resolve({ environmentId: resolved.environmentId, cwd: entry.cwd }),
        );
        if (resolved.checkoutKey !== original.checkoutKey) return yield* error(threadId);
      }
      if (entry === undefined && active !== null && active.owner.threadId === threadId) {
        const identity = yield* wrap(
          threadId,
          resolver.resolve({ environmentId: yield* environment.getEnvironmentId, cwd: input.cwd }),
        );
        const record = yield* wrap(threadId, store.read(active.checkoutKey));
        if (
          identity.checkoutKey !== active.checkoutKey ||
          record === null ||
          record.handle.ownershipId !== active.ownershipId ||
          !sameOwner(record.handle.owner, active.owner) ||
          record.handle.generation < active.generation
        )
          return yield* error(threadId);
        yield* wrap(threadId, ownership.assertCurrent(record.handle));
        entry = activity.get(Store.ownerKey(active.owner));
        if (entry === undefined) return yield* error(threadId);
      }
      if (entry === undefined) {
        const admitted = yield* admit({
          commandId: command("terminal", yield* randomUuidV4),
          projectId: null,
          threadId,
          runId: null,
          cwd: input.cwd,
          operation: "terminal",
          setupTerminalId: input.terminalId,
        });
        entry = activity.get(
          Store.ownerKey({ kind: "terminal", threadId, terminalId: input.terminalId }),
        );
        if (entry === undefined) {
          if (admitted === null) return;
          return yield* error(threadId);
        }
      } else if (entry.handle === null) {
        yield* admit({
          commandId: command("terminal-write", yield* randomUuidV4),
          projectId: entry.projectId,
          threadId,
          runId: entry.owner.kind === "run" ? entry.owner.runId : null,
          cwd: input.cwd,
          operation: entry.owner.kind === "run" ? "provider" : "terminal",
          ...(entry.owner.kind === "terminal" ? { setupTerminalId: entry.owner.terminalId } : {}),
        });
      }
      if (entry.handle !== null) yield* wrap(threadId, ownership.assertCurrent(entry.handle));
      entry.terminalObserved = true;
      if (previous !== undefined && previous.state !== "stopped") return;
      if (terminals.size >= MAX_ACTIVITY) {
        observationGap = true;
        if (entry.handle === null) return;
        return yield* error(threadId, new CheckoutActivityObservationGap({ limit: MAX_ACTIVITY }));
      }
      const terminal: Terminal = {
        activity: entry,
        cwd: input.cwd,
        reservation: {
          id: encodeKey([
            "terminal-pending",
            input.threadId,
            input.terminalId,
            yield* randomUuidV4,
          ]),
          creationMarker: ownership.serverInstanceId,
        },
        process: null,
        state: "unknown",
      };
      terminals.set(key, terminal);
      yield* wrap(
        threadId,
        registerProcess(entry, terminal.reservation, "unknown", "terminal-reservation"),
      );
    });
  const terminalWrite: Context.Service.Shape<typeof Admission.CheckoutAdmission>["terminalWrite"] =
    Effect.fn("CheckoutAdmissionLive.terminalWrite")(function* (input) {
      yield* terminalOpen(input);
      const terminal = terminals.get(terminalKey(input.threadId, input.terminalId));
      if (
        terminal !== undefined &&
        terminal.activity.handle !== null &&
        (terminal.state !== "live" || terminal.process === null)
      )
        return yield* error(ThreadId.make(input.threadId));
    });
  const terminalStarted: Context.Service.Shape<
    typeof Admission.CheckoutAdmission
  >["terminalStarted"] = Effect.fn("CheckoutAdmissionLive.terminalStarted")(function* (input) {
    const terminal = terminals.get(terminalKey(input.threadId, input.terminalId));
    if (terminal === undefined) {
      const entry = activity.get(
        Store.ownerKey({
          kind: "terminal",
          threadId: ThreadId.make(input.threadId),
          terminalId: input.terminalId,
        }),
      );
      if (entry !== undefined) {
        entry.terminalObserved = true;
        entry.uncertain = true;
        yield* observeFailure(
          registerProcess(entry, rootRef(entry), "unknown", "execution-reservation"),
        );
      }
      return;
    }
    if (terminal.cwd !== input.cwd || !Number.isSafeInteger(input.pid) || input.pid <= 0) {
      terminal.state = "unknown";
      return;
    }
    if (terminal.process !== null) {
      if (terminal.process.pid !== input.pid) {
        terminal.state = "unknown";
        yield* observeFailure(
          registerProcess(terminal.activity, terminal.process, "unknown", "terminal"),
        );
      }
      return;
    }
    const processRef = {
      id: encodeKey(["terminal-process", terminal.reservation.id, String(input.pid)]),
      creationMarker: encodeKey([ownership.serverInstanceId, yield* randomUuidV4]),
      pid: input.pid,
    };
    terminal.process = processRef;
    terminal.state = "live";
    yield* observeFailure(
      Effect.gen(function* () {
        yield* registerProcess(terminal.activity, processRef, "live", "terminal");
        yield* registerProcess(
          terminal.activity,
          terminal.reservation,
          "stopped",
          "terminal-reservation",
        );
      }),
    );
  });
  const terminalClosed: Context.Service.Shape<
    typeof Admission.CheckoutAdmission
  >["terminalClosed"] = Effect.fn("CheckoutAdmissionLive.terminalClosed")(function* (input) {
    const key = terminalKey(input.threadId, input.terminalId);
    const terminal = terminals.get(key);
    if (terminal === undefined) return;
    // A captured PTY may exit before terminalStarted records its PID.
    const stopped =
      input.confirmed &&
      (input.descendantsStopped === true ||
        (terminal.process === null && input.descendantsStopped === undefined));
    terminal.state = stopped ? "stopped" : "unknown";
    if (!stopped) terminal.activity.uncertain = true;
    yield* observeFailure(
      Effect.gen(function* () {
        yield* registerProcess(
          terminal.activity,
          terminal.reservation,
          terminal.state,
          "terminal-reservation",
        );
        if (terminal.process !== null)
          yield* registerProcess(terminal.activity, terminal.process, terminal.state, "terminal");
        if (terminal.activity.owner.kind === "terminal") {
          terminal.activity.providerStopped = stopped;
          terminal.activity.finalized = stopped;
        }
        if (!stopped && terminal.activity.handle !== null) {
          const handle = terminal.activity.handle;
          yield* ownership.releaseWhenQuiescent({
            commandId: command(
              "terminal-unknown",
              terminal.reservation.id,
              handle.ownershipId,
              String(handle.generation),
            ),
            handle,
          });
        } else yield* finish(terminal.activity);
      }),
    );
    if (stopped) terminals.delete(key);
  });
  const inspectOwner = Effect.fn("CheckoutAdmissionLive.inspectOwner")(function* (
    threadId: ThreadId,
  ) {
    return yield* wrap(
      threadId,
      Effect.gen(function* () {
        const thread = yield* projections.getThread(threadId);
        const project = yield* projects.get(thread.projectId);
        const cwd = thread.worktreePath ?? Option.getOrNull(project)?.workspaceRoot;
        if (cwd === undefined) return yield* error(threadId);
        const identity = yield* resolver.resolve({
          environmentId: yield* environment.getEnvironmentId,
          cwd,
        });
        const claim = yield* ownership.inspect({ identity });
        return claim === null || claim.state === "released" ? null : claim;
      }),
    );
  });
  const inspect = Effect.fn("CheckoutAdmissionLive.inspect")(function* (threadId: ThreadId) {
    return yield* wrap(
      threadId,
      Effect.gen(function* () {
        const projection = yield* projections.getThreadProjection(threadId);
        if (projection.thread.archivedAt !== null || projection.thread.deletedAt !== null)
          return null;
        const run = [...projection.runs]
          .filter((candidate) => candidate.status !== "queued")
          .sort((a, b) => b.ordinal - a.ordinal)[0];
        if (run === undefined || run.activeAttemptId === null) return null;
        const preparation =
          run.status === "failed" &&
          run.workspacePreparation !== undefined &&
          projection.turnItems.some(
            (item) =>
              item.runId === run.id &&
              item.type === "error" &&
              item.status === "failed" &&
              item.failure.code === ORCHESTRATION_V2_WORKSPACE_PREPARATION_FAILURE_CODE,
          );
        if (
          (run.status !== "waiting" ||
            !projection.turnItems.some(
              (item) =>
                item.runId === run.id &&
                item.type === "system_notice" &&
                item.id === noticeId(run.threadId, run.id),
            )) &&
          !preparation
        )
          return null;
        const cached = blocked.get(threadId);
        const project = yield* projects.get(projection.thread.projectId);
        const cwd =
          cached?.owner.kind === "run" && cached.owner.runId === run.id
            ? cached.identity.canonicalCwd
            : (projection.thread.worktreePath ?? Option.getOrNull(project)?.workspaceRoot);
        if (cwd === undefined) return yield* error(threadId);
        const identity = yield* resolver.resolve({
          environmentId: yield* environment.getEnvironmentId,
          cwd,
        });
        const request: AcquireCheckoutOwnershipInput = {
          commandId: command("inspect", threadId, run.id),
          identity,
          owner: { kind: "run", threadId, runId: run.id },
          operation: preparation ? "setup" : "provider",
        };
        return { request, run, claim: yield* store.read(identity.checkoutKey) };
      }),
    );
  });
  const registerAdmitted = Effect.fn("CheckoutAdmissionLive.registerAdmitted")(function* (
    handle: OwnershipHandle,
  ) {
    yield* wrap(
      handle.owner.threadId,
      Effect.gen(function* () {
        yield* ownership.assertCurrent(handle);
        const record = yield* store.read(handle.checkoutKey);
        if (record === null) return yield* error(handle.owner.threadId);
        const thread = yield* projections.getThread(handle.owner.threadId);
        const entry = yield* track({
          owner: handle.owner,
          commandId: command("admitted", handle.ownershipId),
          cwd: record.identity.canonicalCwd,
          projectId: thread.projectId,
        });
        if (entry.handle !== null && entry.handle.ownershipId !== handle.ownershipId)
          return yield* error(handle.owner.threadId);
        if (entry.handle === null || entry.handle.generation !== handle.generation)
          yield* attach(entry, handle);
      }),
    );
  });
  const setupUnstartedCompleted = Effect.fn("CheckoutAdmissionLive.setupUnstartedCompleted")(
    function* (input: { readonly threadId: ThreadId; readonly setupTerminalId: string }) {
      const entry = activity.get(
        Store.ownerKey({
          kind: "terminal",
          threadId: input.threadId,
          terminalId: input.setupTerminalId,
        }),
      );
      if (
        entry === undefined ||
        entry.terminalObserved ||
        entry.setupObservationGap ||
        entry.settled ||
        (entry.handle !== null && entry.handle.operation !== "setup")
      )
        return;
      yield* observeFailure(
        Effect.gen(function* () {
          if (entry.handle !== null) yield* ownership.assertCurrent(entry.handle);
          entry.providerStopped = true;
          entry.finalized = true;
          yield* finish(entry);
        }),
      );
    },
  );
  const setupUncertain = Effect.fn("CheckoutAdmissionLive.setupUncertain")(function* (input: {
    readonly threadId: ThreadId;
    readonly setupTerminalId: string;
  }) {
    const entry = activity.get(
      Store.ownerKey({
        kind: "terminal",
        threadId: input.threadId,
        terminalId: input.setupTerminalId,
      }),
    );
    if (entry === undefined || entry.settled) return;
    entry.uncertain = true;
    entry.setupObservationGap = true;
    entry.providerStopped = false;
    yield* observeFailure(
      Effect.gen(function* () {
        yield* registerProcess(entry, rootRef(entry), "unknown", "execution-reservation");
        const handle = entry.handle;
        if (handle !== null)
          yield* ownership.releaseWhenQuiescent({
            commandId: command("setup-uncertain", handle.ownershipId, String(handle.generation)),
            handle,
          });
      }),
    );
  });
  const admission: Context.Service.Shape<typeof Admission.CheckoutAdmission> = {
    admit,
    withCurrent: (handle, effect) =>
      handle === null
        ? effect
        : ownership
            .withCurrent(handle, effect)
            .pipe(
              Effect.mapError((cause) =>
                isOwnershipError(cause) ? error(handle.owner.threadId, cause) : cause,
              ),
            ),
    withVerificationAdmission: (input, effect) => withGate(input, effect),
    providerDrained,
    finalized,
    terminalOpen,
    terminalWrite,
    terminalStarted,
    terminalClosed,
    setupUnstartedCompleted,
    setupUncertain,
  };
  return CheckoutAdmissionLive.of({ admission, inspect, inspectOwner, withGate, registerAdmitted });
});

const serviceLayer = Layer.effect(CheckoutAdmissionLive, make);
export const layer = Layer.effect(
  Admission.CheckoutAdmission,
  Effect.map(CheckoutAdmissionLive, (service) => service.admission),
).pipe(Layer.provideMerge(serviceLayer));
