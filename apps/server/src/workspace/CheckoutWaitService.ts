import {
  AcquireCheckoutOwnershipResult,
  CheckoutOwnershipWait,
  type CheckoutOwnershipRecord,
  CommandId,
  OwnershipHandle,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Ids from "@t3tools/provider-core/server/IdAllocator";
import { randomUuidV4 } from "@t3tools/provider-core/server/randomUuid";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as Events from "../orchestration-v2/EventSink.ts";
import * as Outbox from "../orchestration-v2/EffectOutbox.ts";
import * as Projections from "../orchestration-v2/ProjectionStore.ts";
import * as Admission from "./CheckoutAdmission.ts";
import * as Live from "./CheckoutAdmissionLive.ts";
import * as Ownership from "./CheckoutOwnershipService.ts";
import * as Store from "./CheckoutOwnershipStore.ts";

export class CheckoutWaitError extends Schema.TaggedError<CheckoutWaitError>()(
  "CheckoutWaitError",
  {
    threadId: ThreadId,
    reason: Schema.Literals([
      "ineligible",
      "stale",
      "cancelled",
      "wait-already-admitted",
      "unavailable",
      "storage",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.reason === "wait-already-admitted"
      ? "This wait has already been admitted. Use the normal stop flow."
      : `Checkout wait cannot proceed: ${this.reason}.`;
  }
}

/** Late bridge to ThreadLaunch.retryPreparation; keeps launch and admission layers acyclic. */
export class CheckoutPreparedRunRetry extends Context.Reference<{
  readonly retry: (input: {
    readonly commandId: CommandId;
    readonly threadId: ThreadId;
    readonly runId: RunId;
  }) => Effect.Effect<void, Admission.CheckoutAdmissionError>;
}>("t3/workspace/CheckoutWaitService/CheckoutPreparedRunRetry", {
  defaultValue: () => ({
    retry: (input) =>
      Effect.fail(new Admission.CheckoutAdmissionError({ threadId: input.threadId })),
  }),
}) {}

export class CheckoutWaitService extends Context.Service<
  CheckoutWaitService,
  {
    readonly inspect: (threadId: ThreadId) => Effect.Effect<
      {
        readonly checkout: Live.CheckoutAdmissionInspection | null;
        readonly owner: CheckoutOwnershipRecord | null;
        readonly waiters: ReadonlyArray<CheckoutOwnershipWait>;
      },
      CheckoutWaitError
    >;
    readonly wait: (input: {
      readonly threadId: ThreadId;
      readonly commandId: CommandId;
      readonly checkoutKey: string;
      readonly generation: number;
    }) => Effect.Effect<CheckoutOwnershipWait, CheckoutWaitError>;
    readonly cancelWait: (input: {
      readonly threadId: ThreadId;
      readonly commandId: CommandId;
      readonly waitId: string;
    }) => Effect.Effect<CheckoutOwnershipWait | null, CheckoutWaitError>;
    readonly resumeWait: (input: {
      readonly threadId: ThreadId;
      readonly waitId: string;
    }) => Effect.Effect<void, CheckoutWaitError>;
  }
>()("t3/workspace/CheckoutWaitService") {}

const Binding = Schema.Struct({
  waitId: Schema.String,
  threadId: ThreadId,
  runId: RunId,
  attemptId: RunAttemptId,
  ordinal: Schema.Number,
  status: Schema.Literals(["waiting", "failed"]),
  generation: Schema.Number,
});
type Binding = typeof Binding.Type;
const encodeBinding = Schema.encodeSync(Schema.fromJsonString(Binding));
const decodeBinding = Schema.decodeUnknownSync(Schema.fromJsonString(Binding));
const decodeWait = Schema.decodeUnknownSync(Schema.fromJsonString(CheckoutOwnershipWait));
const encodeWait = Schema.encodeSync(Schema.fromJsonString(CheckoutOwnershipWait));
const MAX_WAITERS = 256;
const isWaitError = Schema.is(CheckoutWaitError);
const sameHandle = Schema.toEquivalence(OwnershipHandle);
const decodeAcquisition = Schema.decodeUnknownSync(
  Schema.fromJsonString(AcquireCheckoutOwnershipResult),
);
const encodeKey = Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)));
const waitKey = (kind: string, waitId: string) => `checkout-wait:${encodeKey([kind, waitId])}`;
const admissionCommand = (waitId: string) => CommandId.make(waitKey("admit", waitId));
const resumeId = (waitId: string) => waitKey("resume", waitId);
const providerId = (waitId: string) => waitKey("provider", waitId);

const make = Effect.gen(function* () {
  const live = yield* Live.CheckoutAdmissionLive;
  const ownership = yield* Ownership.CheckoutOwnershipService;
  const store = yield* Store.CheckoutOwnershipStore;
  const sql = yield* SqlClient.SqlClient;
  const outbox = yield* Outbox.EffectOutboxV2;
  const projections = yield* Projections.ProjectionStoreV2;
  const events = yield* Events.EventSinkV2;
  const ids = yield* Ids.IdAllocatorV2;
  const wrap = <A, E, R>(threadId: ThreadId, effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.mapError((cause) =>
        isWaitError(cause) ? cause : new CheckoutWaitError({ threadId, reason: "storage", cause }),
      ),
    );
  const bindingFor = Effect.fn("CheckoutWaitService.bindingFor")(function* (waitId: string) {
    const rows = yield* sql<{
      readonly result_json: string;
    }>`SELECT result_json FROM orchestration_v2_workspace_receipts WHERE command_id = ${waitKey("binding", waitId)} AND command_type = 'workspace.wait-binding'`;
    return rows[0] === undefined
      ? null
      : yield* Effect.try({
          try: () => decodeBinding(rows[0]!.result_json),
          catch: (cause) =>
            new Store.CheckoutOwnershipStoreError({ operation: "wait-binding", cause }),
        });
  });
  // This predicate is SQL-only and is repeated within the claim transaction. A
  // projection loaded before the transaction is never sufficient to authorize a start.
  const eligible = Effect.fn("CheckoutWaitService.eligible")(function* (binding: Binding) {
    const rows = yield* sql`SELECT r.run_id FROM orchestration_v2_projection_runs r
      JOIN orchestration_v2_projection_threads t ON t.thread_id = r.thread_id
      WHERE r.run_id = ${binding.runId} AND r.thread_id = ${binding.threadId}
        AND r.status = ${binding.status} AND r.ordinal = ${binding.ordinal}
        AND json_extract(r.payload_json, '$.activeAttemptId') = ${binding.attemptId}
        AND t.archived_at IS NULL AND t.deleted_at IS NULL
        AND NOT EXISTS (SELECT 1 FROM orchestration_v2_projection_run_attempts a WHERE a.attempt_id = ${binding.attemptId} AND a.status NOT IN ('pending', 'failed'))
        AND (${binding.status} <> 'failed' OR EXISTS (SELECT 1 FROM orchestration_v2_projection_turn_items i WHERE i.thread_id = r.thread_id AND i.run_id = r.run_id AND i.type = 'error' AND i.status = 'failed' AND json_extract(i.payload_json, '$.failure.code') = 'workspace_preparation_failed'))
        AND NOT EXISTS (SELECT 1 FROM orchestration_v2_projection_runs newer WHERE newer.thread_id = r.thread_id AND newer.ordinal > r.ordinal AND newer.status <> 'queued')`;
    return rows.length === 1;
  });

  const wakeNext = Effect.fn("CheckoutWaitService.wakeNext")(function* (checkoutKey: string) {
    const changed = yield* sql.withTransaction(
      Effect.gen(function* () {
        const claim = yield* store.read(checkoutKey);
        if (claim !== null && claim.state !== "released") return false;
        const ready =
          yield* sql`SELECT 1 FROM orchestration_v2_checkout_waiters w WHERE w.checkout_key = ${checkoutKey} AND w.state = 'ready'
        AND NOT EXISTS (SELECT 1 FROM orchestration_v2_effect_outbox e WHERE e.effect_type = 'checkout-wait.resume' AND json_extract(e.payload_json, '$.waitId') = w.wait_id) LIMIT 1`;
        if (ready.length > 0) return false;
        const rows = yield* sql<{
          readonly wait_id: string;
        }>`SELECT wait_id FROM orchestration_v2_checkout_waiters WHERE checkout_key = ${checkoutKey} AND state = 'waiting' ORDER BY sequence LIMIT 1`;
        if (rows[0] === undefined) return false;
        const waiter = yield* store.getWait(rows[0].wait_id);
        if (waiter === null || waiter.state !== "waiting") return false;
        yield* sql`UPDATE orchestration_v2_checkout_waiters SET state = 'ready', payload_json = ${encodeWait({ ...waiter, state: "ready" })} WHERE wait_id = ${waiter.waitId} AND state = 'waiting'`;
        return true;
      }),
    );
    if (changed) yield* store.notify(checkoutKey);
  });
  const promote = Effect.fn("CheckoutWaitService.promote")(function* (checkoutKey?: string) {
    const rows = yield* sql<{
      readonly payload_json: string;
    }>`SELECT w.payload_json FROM orchestration_v2_checkout_waiters w
      WHERE w.state = 'ready' AND ${checkoutKey === undefined ? sql`1 = 1` : sql`w.checkout_key = ${checkoutKey}`}
      AND NOT EXISTS (SELECT 1 FROM orchestration_v2_effect_outbox e WHERE e.effect_type = 'checkout-wait.resume' AND json_extract(e.payload_json, '$.waitId') = w.wait_id)
      ORDER BY w.sequence LIMIT ${MAX_WAITERS + 1}`;
    if (rows.length > MAX_WAITERS) return;
    const waiters = yield* Effect.try({
      try: () => rows.map((row) => decodeWait(row.payload_json)),
      catch: (cause) =>
        new Store.CheckoutOwnershipStoreError({ operation: "ready-waiters", cause }),
    });
    for (const waiter of waiters) {
      if (waiter.state !== "ready" || waiter.input.owner.kind !== "run") continue;
      const threadId = waiter.input.owner.threadId;
      yield* wrap(
        threadId,
        Effect.gen(function* () {
          const binding = yield* bindingFor(waiter.waitId);
          if (binding === null) return;
          if (!(yield* eligible(binding))) {
            yield* ownership.cancelWait({
              commandId: CommandId.make(waitKey("obsolete", waiter.waitId)),
              waitId: waiter.waitId,
              threadId,
            });
            yield* wakeNext(waiter.input.identity.checkoutKey);
            return;
          }
          if (Option.isSome(yield* outbox.get(resumeId(waiter.waitId)))) return;
          const acquireWithIntent = ownership.acquireWithIntent;
          if (acquireWithIntent === undefined)
            return yield* new CheckoutWaitError({ threadId, reason: "unavailable" });
          const result = yield* live.withGate(
            { threadId, identity: waiter.input.identity, owner: waiter.input.owner },
            Effect.gen(function* () {
              if (Option.isSome(yield* outbox.get(resumeId(waiter.waitId)))) return null;
              const currentClaim = yield* store.read(waiter.input.identity.checkoutKey);
              if (currentClaim !== null && currentClaim.state !== "released") return null;
              const result = yield* acquireWithIntent({
                request: { ...waiter.input, commandId: admissionCommand(waiter.waitId) },
                persistIntent: Effect.fn("CheckoutWaitService.persistResumeIntent")(function* () {
                  const current = yield* store.getWait(waiter.waitId);
                  if (
                    current === null ||
                    current.state !== "ready" ||
                    current.input.owner.threadId !== binding.threadId ||
                    current.input.owner.kind !== "run" ||
                    current.input.owner.runId !== binding.runId ||
                    current.input.identity.checkoutKey !== waiter.input.identity.checkoutKey ||
                    !(yield* eligible(binding))
                  )
                    return yield* new CheckoutWaitError({ threadId, reason: "cancelled" });
                  yield* outbox.enqueue([
                    {
                      id: resumeId(waiter.waitId),
                      commandId: admissionCommand(waiter.waitId),
                      threadId,
                      request: { type: "checkout-wait.resume", waitId: waiter.waitId },
                    },
                  ]);
                }),
              });
              if (result._tag === "Acquired") yield* live.registerAdmitted(result.handle);
              return result;
            }),
          );
          if (result?._tag === "Acquired") yield* outbox.notifyAvailable();
        }),
      ).pipe(
        Effect.catchCause(() => Effect.logWarning("Checkout waiter remains blocked", { threadId })),
      );
    }
  });

  const wait: CheckoutWaitService["Service"]["wait"] = Effect.fn("CheckoutWaitService.wait")(
    function* (input) {
      return yield* wrap(
        input.threadId,
        Effect.gen(function* () {
          const previous = yield* sql<{
            readonly command_type: string;
            readonly result_json: string;
          }>`SELECT command_type, result_json FROM orchestration_v2_workspace_receipts WHERE command_id = ${input.commandId}`;
          if (previous[0] !== undefined) {
            if (previous[0].command_type !== "workspace.wait")
              return yield* new CheckoutWaitError({ threadId: input.threadId, reason: "stale" });
            const original = yield* Effect.try({
              try: () => decodeWait(previous[0]!.result_json),
              catch: (cause) =>
                new CheckoutWaitError({ threadId: input.threadId, reason: "storage", cause }),
            });
            if (
              original.input.owner.threadId !== input.threadId ||
              original.input.identity.checkoutKey !== input.checkoutKey
            )
              return yield* new CheckoutWaitError({ threadId: input.threadId, reason: "stale" });
            return (yield* store.getWait(original.waitId)) ?? original;
          }
          const inspected = yield* live.inspect(input.threadId);
          if (
            inspected === null ||
            inspected.run.activeAttemptId === null ||
            (inspected.run.status !== "waiting" && inspected.run.status !== "failed")
          )
            return yield* new CheckoutWaitError({ threadId: input.threadId, reason: "ineligible" });
          if (
            inspected.request.identity.checkoutKey !== input.checkoutKey ||
            inspected.claim === null ||
            inspected.claim.handle.generation !== input.generation
          )
            return yield* new CheckoutWaitError({ threadId: input.threadId, reason: "stale" });
          return yield* live.withGate(
            {
              threadId: input.threadId,
              identity: inspected.request.identity,
              owner: inspected.request.owner,
            },
            Effect.gen(function* () {
              const waitId = yield* randomUuidV4;
              const binding: Binding = {
                waitId,
                threadId: input.threadId,
                runId: inspected.run.id,
                attemptId: inspected.run.activeAttemptId!,
                ordinal: inspected.run.ordinal,
                status: inspected.run.status as "waiting" | "failed",
                generation: input.generation,
              };
              const waiter = yield* sql.withTransaction(
                Effect.gen(function* () {
                  const replay = yield* sql<{
                    readonly command_type: string;
                    readonly result_json: string;
                  }>`SELECT command_type, result_json FROM orchestration_v2_workspace_receipts WHERE command_id = ${input.commandId}`;
                  if (replay[0] !== undefined) {
                    if (replay[0].command_type !== "workspace.wait")
                      return yield* new CheckoutWaitError({
                        threadId: input.threadId,
                        reason: "stale",
                      });
                    const original = yield* Effect.try({
                      try: () => decodeWait(replay[0]!.result_json),
                      catch: (cause) =>
                        new CheckoutWaitError({
                          threadId: input.threadId,
                          reason: "storage",
                          cause,
                        }),
                    });
                    if (
                      original.input.owner.threadId !== input.threadId ||
                      original.input.identity.checkoutKey !== input.checkoutKey
                    )
                      return yield* new CheckoutWaitError({
                        threadId: input.threadId,
                        reason: "stale",
                      });
                    return (yield* store.getWait(original.waitId)) ?? original;
                  }
                  const claim = yield* store.read(input.checkoutKey);
                  if (claim === null || claim.handle.generation !== input.generation)
                    return yield* new CheckoutWaitError({
                      threadId: input.threadId,
                      reason: "stale",
                    });
                  if (!(yield* eligible(binding)))
                    return yield* new CheckoutWaitError({
                      threadId: input.threadId,
                      reason: "ineligible",
                    });
                  // Identity resolution already completed under the admission gate. This
                  // transaction contains only durable waiter/binding/receipt writes.
                  const waiter = yield* store.enqueueWait({
                    ...inspected.request,
                    commandId: input.commandId,
                    waitId,
                  });
                  yield* sql`INSERT INTO orchestration_v2_workspace_receipts(command_id, thread_id, command_type, result_json) VALUES (${waitKey("binding", waiter.waitId)}, ${input.threadId}, 'workspace.wait-binding', ${encodeBinding(binding)})`;
                  return waiter;
                }),
              );
              yield* store.notify(input.checkoutKey);
              const persisted = yield* store.getWait(waiter.waitId);
              if (persisted === null)
                return yield* new CheckoutWaitError({
                  threadId: input.threadId,
                  reason: "storage",
                });
              return persisted;
            }),
          );
        }),
      );
    },
  );
  const cancelWait: CheckoutWaitService["Service"]["cancelWait"] = Effect.fn(
    "CheckoutWaitService.cancelWait",
  )(function* (input) {
    const result = yield* wrap(
      input.threadId,
      ownership.cancelWait(input).pipe(
        Effect.mapError((cause) => {
          let current: unknown = cause;
          while (Predicate.hasProperty(current, "cause")) {
            if (
              Predicate.hasProperty(current, "operation") &&
              current.operation === "wait-already-admitted"
            )
              return new CheckoutWaitError({
                threadId: input.threadId,
                reason: "wait-already-admitted",
                cause,
              });
            current = current.cause;
          }
          return cause;
        }),
      ),
    );
    if (result !== null && result.state === "cancelled")
      yield* wrap(input.threadId, wakeNext(result.input.identity.checkoutKey));
    return result;
  });

  const resumeWait: CheckoutWaitService["Service"]["resumeWait"] = Effect.fn(
    "CheckoutWaitService.resumeWait",
  )(function* (input) {
    yield* wrap(
      input.threadId,
      Effect.gen(function* () {
        const waiter = yield* store.getWait(input.waitId);
        const binding = yield* bindingFor(input.waitId);
        if (
          waiter === null ||
          binding === null ||
          waiter.input.owner.threadId !== input.threadId ||
          binding.threadId !== input.threadId ||
          waiter.input.owner.kind !== "run"
        )
          return yield* new CheckoutWaitError({ threadId: input.threadId, reason: "ineligible" });
        if (waiter.state !== "ready")
          return yield* new CheckoutWaitError({ threadId: input.threadId, reason: "cancelled" });
        if (Option.isSome(yield* outbox.get(providerId(waiter.waitId)))) return;
        if (
          (yield* sql`SELECT 1 FROM orchestration_v2_workspace_receipts WHERE command_id = ${waitKey("retry-completed", waiter.waitId)} AND command_type = 'workspace.wait-retry-completed'`)
            .length > 0
        )
          return;
        if (Option.isNone(yield* outbox.get(resumeId(waiter.waitId))))
          return yield* new CheckoutWaitError({ threadId: input.threadId, reason: "stale" });
        const claim = yield* store.read(waiter.input.identity.checkoutKey);
        if (
          claim === null ||
          Store.ownerKey(claim.handle.owner) !== Store.ownerKey(waiter.input.owner)
        )
          return yield* new CheckoutWaitError({ threadId: input.threadId, reason: "stale" });
        const acquisition = yield* sql<{
          readonly result_json: string;
        }>`SELECT result_json FROM orchestration_v2_workspace_receipts WHERE command_id = ${admissionCommand(waiter.waitId)} AND command_type = 'workspace.acquire'`;
        const admitted =
          acquisition[0] === undefined
            ? null
            : yield* Effect.try({
                try: () => decodeAcquisition(acquisition[0]!.result_json),
                catch: (cause) =>
                  new CheckoutWaitError({ threadId: input.threadId, reason: "storage", cause }),
              });
        if (admitted?._tag !== "Acquired" || !sameHandle(admitted.handle, claim.handle))
          return yield* new CheckoutWaitError({ threadId: input.threadId, reason: "stale" });
        const handle: OwnershipHandle = claim.handle;
        const retryCommand = CommandId.make(waitKey("retry", waiter.waitId));
        const projection = yield* projections.getRuntimeRecoveryProjection(input.threadId);
        const run = projection.runs.find((candidate) => candidate.id === binding.runId);
        if (
          run === undefined ||
          run.activeAttemptId !== binding.attemptId ||
          projection.thread.archivedAt !== null ||
          projection.thread.deletedAt !== null ||
          projection.runs.some(
            (candidate) => candidate.ordinal > binding.ordinal && candidate.status !== "queued",
          )
        )
          return yield* new CheckoutWaitError({ threadId: input.threadId, reason: "ineligible" });
        if (binding.status === "failed") {
          const retryIntent =
            yield* sql`SELECT 1 FROM orchestration_v2_workspace_receipts WHERE command_id = ${retryCommand} AND command_type = 'workspace.wait-retry'`;
          if (run.status !== "failed" && !(run.status === "preparing" && retryIntent.length === 1))
            return yield* new CheckoutWaitError({ threadId: input.threadId, reason: "ineligible" });
          yield* ownership.assertCurrent(handle);
          yield* sql.withTransaction(
            Effect.gen(function* () {
              if (retryIntent.length === 0 && !(yield* eligible(binding)))
                return yield* new CheckoutWaitError({
                  threadId: input.threadId,
                  reason: "ineligible",
                });
              yield* sql`INSERT INTO orchestration_v2_workspace_receipts(command_id, thread_id, command_type, result_json) VALUES (${retryCommand}, ${input.threadId}, 'workspace.wait-retry', 'null') ON CONFLICT(command_id) DO NOTHING`;
            }),
          );
          const bridge = yield* CheckoutPreparedRunRetry;
          yield* bridge.retry({
            commandId: retryCommand,
            threadId: input.threadId,
            runId: binding.runId,
          });
          yield* sql`INSERT INTO orchestration_v2_workspace_receipts(command_id, thread_id, command_type, result_json) VALUES (${waitKey("retry-completed", waiter.waitId)}, ${input.threadId}, 'workspace.wait-retry-completed', 'null') ON CONFLICT(command_id) DO NOTHING`;
          return;
        }
        if (run.status !== "waiting")
          return yield* new CheckoutWaitError({ threadId: input.threadId, reason: "ineligible" });
        const now = yield* DateTime.now;
        yield* ownership.withCurrent(
          handle,
          events.writeIfRunCurrent({
            threadId: input.threadId,
            runId: run.id,
            activeAttemptId: binding.attemptId,
            expectedStatus: "waiting",
            commandId: CommandId.make(providerId(waiter.waitId)),
            events: [
              {
                id: yield* ids.allocate.event({ threadId: input.threadId }),
                type: "run.updated",
                threadId: input.threadId,
                runId: run.id,
                occurredAt: now,
                payload: { ...run, status: "starting", completedAt: null },
              },
            ],
            effects: [
              {
                id: providerId(waiter.waitId),
                commandId: CommandId.make(providerId(waiter.waitId)),
                threadId: input.threadId,
                request: { type: "provider-turn.start", runId: run.id },
              },
            ],
          }),
        );
      }),
    );
  });
  const inspect: CheckoutWaitService["Service"]["inspect"] = (threadId) =>
    wrap(
      threadId,
      Effect.gen(function* () {
        const checkout = yield* live.inspect(threadId);
        const owner = yield* live.inspectOwner(threadId);
        const waiters = yield* store.listWaiters(checkout?.request.identity.checkoutKey);
        if (waiters.length > MAX_WAITERS)
          return yield* new CheckoutWaitError({ threadId, reason: "unavailable" });
        return {
          checkout,
          owner,
          waiters: waiters.filter((waiter) => waiter.input.owner.threadId === threadId),
        };
      }),
    );

  // Subscribe before scanning durable ready records. Immediate startup reaches the
  // PubSub subscription before yielding, so a concurrent release cannot be missed.
  yield* Stream.runForEach(store.changes, (key) =>
    promote(key).pipe(
      Effect.catchCause(() => Effect.logWarning("Checkout waiter change remains unresolved")),
    ),
  ).pipe(Effect.forkScoped({ startImmediately: true }));
  yield* promote().pipe(
    Effect.catchCause(() => Effect.logWarning("Checkout waiter recovery remains blocked")),
  );
  return CheckoutWaitService.of({ inspect, wait, cancelWait, resumeWait });
});

export const layer = Layer.effect(CheckoutWaitService, make);
