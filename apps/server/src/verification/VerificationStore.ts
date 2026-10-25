import {
  VerificationRecord,
  type CommandId,
  type OwnershipHandle,
  type SourceIdentity,
  type ThreadId,
  type VerificationCurrentness,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import * as EffectOutbox from "../orchestration-v2/EffectOutbox.ts";

export class VerificationStoreError extends Schema.TaggedError<VerificationStoreError>()(
  "VerificationStoreError",
  {
    operation: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Verification persistence failed during ${this.operation}.`;
  }
}

type Completion = Pick<
  VerificationRecord,
  | "execution"
  | "currentness"
  | "finishedAt"
  | "exitCode"
  | "signal"
  | "terminationReason"
  | "endSource"
  | "endMutationGeneration"
  | "observationState"
  | "outputReferences"
  | "truncated"
> & { readonly startedAt?: string | null };
const codec = Schema.fromJsonString(VerificationRecord);
const decode = Schema.decodeUnknownEffect(codec);
const encode = Schema.encodeEffect(codec);
const isStoreError = Schema.is(VerificationStoreError);

export class VerificationStore extends Context.Service<
  VerificationStore,
  {
    readonly get: (id: string) => Effect.Effect<VerificationRecord | null, VerificationStoreError>;
    readonly getByCommandId: (
      id: CommandId,
    ) => Effect.Effect<VerificationRecord | null, VerificationStoreError>;
    readonly latestForThread: (
      threadId: ThreadId,
    ) => Effect.Effect<VerificationRecord | null, VerificationStoreError>;
    readonly listForCheckout: (
      checkoutKey: string,
    ) => Effect.Effect<ReadonlyArray<VerificationRecord>, VerificationStoreError>;
    readonly listForThread: (
      threadId: ThreadId,
    ) => Effect.Effect<ReadonlyArray<VerificationRecord>, VerificationStoreError>;
    readonly listEvidence: Effect.Effect<ReadonlyArray<VerificationRecord>, VerificationStoreError>;
    readonly failQueued: (input: {
      readonly verificationId: string;
      readonly commandId: CommandId;
      readonly now: string;
      readonly reason: string;
    }) => Effect.Effect<VerificationRecord | null, VerificationStoreError>;
    readonly listRecoverable: Effect.Effect<
      ReadonlyArray<VerificationRecord>,
      VerificationStoreError
    >;
    readonly queue: (input: {
      readonly record: VerificationRecord;
      readonly checkoutKey: string;
      readonly sourceRunOrdinal: number;
      readonly effects: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2>;
    }) => Effect.Effect<
      { readonly record: VerificationRecord; readonly inserted: boolean },
      VerificationStoreError
    >;
    readonly claim: (input: {
      readonly verificationId: string;
      readonly executionClaimId: string;
      readonly serverInstanceId: string;
      readonly handle: OwnershipHandle;
      readonly startedAt: string | null;
      readonly source: SourceIdentity;
      readonly mutationGeneration: number;
      readonly observationState: VerificationRecord["observationState"];
    }) => Effect.Effect<VerificationRecord | null, VerificationStoreError>;
    readonly finish: (input: {
      readonly verificationId: string;
      readonly executionClaimId: string;
      readonly commandId: CommandId;
      readonly completion: Completion;
    }) => Effect.Effect<VerificationRecord | null, VerificationStoreError>;
    readonly requestCancel: (input: {
      readonly verificationId: string;
      readonly commandId: CommandId;
      readonly now: string;
    }) => Effect.Effect<VerificationRecord | null, VerificationStoreError>;
    readonly isCancellationRequested: (
      id: string,
    ) => Effect.Effect<boolean, VerificationStoreError>;
    readonly invalidate: (input: {
      readonly verificationId: string;
      readonly currentness: Exclude<VerificationCurrentness, "Current">;
      readonly commandId: CommandId;
      readonly observationGap?: boolean;
    }) => Effect.Effect<VerificationRecord | null, VerificationStoreError>;
    readonly putOutput: (input: {
      readonly verificationId: string;
      readonly referenceId: string;
      readonly kind: string;
      readonly text: string;
      readonly truncated: boolean;
    }) => Effect.Effect<void, VerificationStoreError>;
    readonly getOutput: (input: {
      readonly verificationId: string;
      readonly referenceId: string;
      readonly offset: number;
      readonly limit: number;
    }) => Effect.Effect<
      {
        readonly text: string;
        readonly truncated: boolean;
        readonly nextOffset: number | null;
      } | null,
      VerificationStoreError
    >;
    readonly interruptRunning: (input: {
      readonly serverInstanceId: string;
      readonly now: string;
    }) => Effect.Effect<ReadonlyArray<VerificationRecord>, VerificationStoreError>;
  }
>()("t3/verification/VerificationStore") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const get = Effect.fn("VerificationStore.get")(
    function* (id: string) {
      const rows = yield* sql<{
        readonly payload_json: string;
      }>`SELECT payload_json FROM orchestration_v2_verification_attempts WHERE verification_id = ${id}`;
      return rows[0] ? yield* decode(rows[0].payload_json) : null;
    },
    Effect.mapError((cause) => new VerificationStoreError({ operation: "get", cause })),
  );
  const getByCommandId = Effect.fn("VerificationStore.getByCommandId")(
    function* (id: CommandId) {
      const rows = yield* sql<{
        readonly payload_json: string;
      }>`SELECT payload_json FROM orchestration_v2_verification_attempts WHERE command_id = ${id}`;
      return rows[0] ? yield* decode(rows[0].payload_json) : null;
    },
    Effect.mapError((cause) => new VerificationStoreError({ operation: "get-command", cause })),
  );
  const latestForThread = Effect.fn("VerificationStore.latestForThread")(
    function* (threadId: ThreadId) {
      const rows = yield* sql<{
        readonly payload_json: string;
      }>`SELECT payload_json FROM orchestration_v2_verification_attempts WHERE thread_id = ${threadId} ORDER BY source_run_ordinal DESC, sequence DESC LIMIT 1`;
      return rows[0] ? yield* decode(rows[0].payload_json) : null;
    },
    Effect.mapError((cause) => new VerificationStoreError({ operation: "latest", cause })),
  );
  const event = Effect.fn("VerificationStore.event")(function* (
    record: VerificationRecord,
    commandId: CommandId,
  ) {
    yield* sql`INSERT INTO orchestration_v2_verification_events(verification_id, command_id, payload_json) VALUES (${record.verificationId}, ${commandId}, ${yield* encode(record)}) ON CONFLICT(command_id) DO NOTHING`;
  });
  const update = Effect.fn("VerificationStore.update")(function* (
    record: VerificationRecord,
    commandId: CommandId,
  ) {
    yield* sql`UPDATE orchestration_v2_verification_attempts SET execution = ${record.execution}, currentness = ${record.currentness}, payload_json = ${yield* encode(record)} WHERE verification_id = ${record.verificationId}`;
    yield* event(record, commandId);
  });
  const listForCheckout = Effect.fn("VerificationStore.listForCheckout")(
    function* (checkoutKey: string) {
      const rows = yield* sql<{
        readonly payload_json: string;
      }>`SELECT payload_json FROM orchestration_v2_verification_attempts WHERE checkout_key = ${checkoutKey} ORDER BY sequence`;
      return yield* Effect.forEach(rows, (row) => decode(row.payload_json));
    },
    Effect.mapError((cause) => new VerificationStoreError({ operation: "checkout", cause })),
  );
  const listForThread = Effect.fn("VerificationStore.listForThread")(
    function* (threadId: ThreadId) {
      const rows = yield* sql<{
        readonly payload_json: string;
      }>`SELECT payload_json FROM orchestration_v2_verification_attempts WHERE thread_id = ${threadId} ORDER BY source_run_ordinal DESC, sequence DESC`;
      return yield* Effect.forEach(rows, (row) => decode(row.payload_json));
    },
    Effect.mapError((cause) => new VerificationStoreError({ operation: "thread", cause })),
  );
  const listEvidence = Effect.gen(function* () {
    const rows = yield* sql<{
      readonly payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_verification_attempts WHERE currentness <> 'Historical only' ORDER BY sequence`;
    return yield* Effect.forEach(rows, (row) => decode(row.payload_json));
  }).pipe(Effect.mapError((cause) => new VerificationStoreError({ operation: "evidence", cause })));
  const queue = Effect.fn("VerificationStore.queue")(
    function* (input: {
      readonly record: VerificationRecord;
      readonly checkoutKey: string;
      readonly sourceRunOrdinal: number;
      readonly effects: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2>;
    }) {
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const old = yield* getByCommandId(input.record.commandId);
          if (old !== null) return { record: old, inserted: false };
          let record = input.record;
          if (
            !Number.isSafeInteger(input.sourceRunOrdinal) ||
            input.sourceRunOrdinal < 0 ||
            record.currentness === "Current" ||
            record.ownerHandle === null ||
            record.ownerHandle.checkoutKey !== input.checkoutKey
          )
            return yield* new VerificationStoreError({ operation: "queue-identity" });
          const previous = yield* sql<{
            readonly payload_json: string;
            readonly source_run_ordinal: number;
          }>`SELECT payload_json, source_run_ordinal FROM orchestration_v2_verification_attempts WHERE thread_id = ${record.threadId}`;
          for (const row of previous) {
            const prior = yield* decode(row.payload_json);
            if (row.source_run_ordinal > input.sourceRunOrdinal)
              record = Object.assign({}, record, { currentness: "Historical only" as const });
            else if (prior.currentness !== "Historical only") {
              yield* sql`UPDATE orchestration_v2_verification_attempts SET invalidated = 1 WHERE verification_id = ${prior.verificationId}`;
              yield* update(
                { ...prior, currentness: "Historical only" },
                `${record.verificationId}:supersedes:${prior.verificationId}` as CommandId,
              );
            }
          }
          if (record.execution !== "queued")
            return yield* new VerificationStoreError({ operation: "queue-state" });
          yield* sql`INSERT INTO orchestration_v2_verification_attempts(verification_id, command_id, thread_id, checkout_key, source_run_id, source_run_ordinal, execution, currentness, payload_json)
        VALUES (${record.verificationId}, ${record.commandId}, ${record.threadId}, ${input.checkoutKey}, ${record.sourceRunId}, ${input.sourceRunOrdinal}, ${record.execution}, ${record.currentness}, ${yield* encode(record)})`;
          yield* event(record, record.commandId);
          yield* sql`INSERT INTO orchestration_command_receipts(command_id, aggregate_kind, aggregate_id, command_type, accepted_at, result_sequence, status, error)
        VALUES (${record.commandId}, 'thread', ${record.threadId}, 'verification.request', ${record.requestedAt}, 0, 'accepted', NULL) ON CONFLICT(command_id) DO NOTHING`;
          yield* outbox.enqueue(input.effects);
          return { record, inserted: true };
        }),
      );
    },
    Effect.mapError((cause) =>
      isStoreError(cause) ? cause : new VerificationStoreError({ operation: "queue", cause }),
    ),
  );

  const claim = Effect.fn("VerificationStore.claim")(
    function* (input: {
      readonly verificationId: string;
      readonly executionClaimId: string;
      readonly serverInstanceId: string;
      readonly handle: OwnershipHandle;
      readonly startedAt: string | null;
      readonly source: SourceIdentity;
      readonly mutationGeneration: number;
      readonly observationState: VerificationRecord["observationState"];
    }) {
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const current = yield* get(input.verificationId);
          if (current === null || current.execution !== "queued") return null;
          if (
            current.ownerHandle === null ||
            current.ownerHandle.ownershipId !== input.handle.ownershipId ||
            current.ownerHandle.generation !== input.handle.generation ||
            current.ownerHandle.serverInstanceId !== input.handle.serverInstanceId ||
            input.handle.checkoutKey !== input.source.checkoutIdentity.checkoutKey ||
            current.environmentId !== input.source.checkoutIdentity.environmentId
          )
            return null;
          const record: VerificationRecord = {
            ...current,
            ownerHandle: input.handle,
            execution: "running",
            startedAt: input.startedAt,
            startSource: input.source,
            startMutationGeneration: input.mutationGeneration,
            observationState: input.observationState,
          };
          const rows =
            yield* sql`UPDATE orchestration_v2_verification_attempts SET execution = 'running', server_instance_id = ${input.serverInstanceId}, execution_claim_id = ${input.executionClaimId}, payload_json = ${yield* encode(record)} WHERE verification_id = ${input.verificationId} AND execution = 'queued' AND cancel_requested = 0 RETURNING verification_id`;
          if (rows.length !== 1) return null;
          yield* event(record, `${input.verificationId}:claimed` as CommandId);
          return record;
        }),
      );
    },
    Effect.mapError((cause) => new VerificationStoreError({ operation: "claim", cause })),
  );

  const finish = Effect.fn("VerificationStore.finish")(
    function* (input: {
      readonly verificationId: string;
      readonly executionClaimId: string;
      readonly commandId: CommandId;
      readonly completion: Completion;
    }) {
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const rows = yield* sql<{
            readonly payload_json: string;
            readonly invalidated: number;
            readonly cancel_requested: number;
          }>`SELECT payload_json, invalidated, cancel_requested FROM orchestration_v2_verification_attempts WHERE verification_id = ${input.verificationId} AND execution = 'running' AND execution_claim_id = ${input.executionClaimId}`;
          if (!rows[0]) return yield* get(input.verificationId);
          if (input.completion.execution === "queued" || input.completion.execution === "running")
            return yield* new VerificationStoreError({ operation: "finish-state" });
          const current = yield* decode(rows[0].payload_json);
          const record: VerificationRecord = {
            ...current,
            ...input.completion,
            ...(rows[0].invalidated ? { currentness: current.currentness } : {}),
            ...(current.observationState !== "continuous"
              ? { observationState: current.observationState }
              : {}),
          };
          yield* update(record, input.commandId);
          return record;
        }),
      );
    },
    Effect.mapError((cause) =>
      isStoreError(cause) ? cause : new VerificationStoreError({ operation: "finish", cause }),
    ),
  );

  const requestCancel = Effect.fn("VerificationStore.requestCancel")(
    function* (input: {
      readonly verificationId: string;
      readonly commandId: CommandId;
      readonly now: string;
    }) {
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const current = yield* get(input.verificationId);
          if (
            current === null ||
            (current.execution !== "queued" && current.execution !== "running")
          )
            return current;
          yield* sql`UPDATE orchestration_v2_verification_attempts SET cancel_requested = 1 WHERE verification_id = ${input.verificationId}`;
          const record: VerificationRecord =
            current.execution === "queued"
              ? {
                  ...current,
                  execution: "cancelled",
                  currentness:
                    current.currentness === "Historical only" ? "Historical only" : "Unknown",
                  finishedAt: input.now,
                  terminationReason: "cancelled",
                }
              : current;
          yield* update(record, input.commandId);
          return record;
        }),
      );
    },
    Effect.mapError((cause) => new VerificationStoreError({ operation: "cancel", cause })),
  );
  const isCancellationRequested = Effect.fn("VerificationStore.isCancellationRequested")(
    function* (id: string) {
      const rows = yield* sql<{
        readonly cancel_requested: number;
      }>`SELECT cancel_requested FROM orchestration_v2_verification_attempts WHERE verification_id = ${id}`;
      return rows[0]?.cancel_requested === 1;
    },
    Effect.mapError((cause) => new VerificationStoreError({ operation: "read-cancel", cause })),
  );

  const invalidate = Effect.fn("VerificationStore.invalidate")(
    function* (input: {
      readonly verificationId: string;
      readonly currentness: Exclude<VerificationCurrentness, "Current">;
      readonly commandId: CommandId;
      readonly observationGap?: boolean;
    }) {
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const current = yield* get(input.verificationId);
          if (current === null) return null;
          const currentness =
            current.currentness === "Historical only" || input.currentness === "Historical only"
              ? "Historical only"
              : current.currentness === "Stale"
                ? "Stale"
                : input.currentness;
          const record: VerificationRecord = {
            ...current,
            currentness,
            ...(input.observationGap ? { observationState: "gap" } : {}),
          };
          if (
            record.currentness === current.currentness &&
            record.observationState === current.observationState
          )
            return current;
          yield* sql`UPDATE orchestration_v2_verification_attempts SET invalidated = 1 WHERE verification_id = ${input.verificationId}`;
          yield* update(record, input.commandId);
          return record;
        }),
      );
    },
    Effect.mapError((cause) => new VerificationStoreError({ operation: "invalidate", cause })),
  );

  const failQueued = Effect.fn("VerificationStore.failQueued")(
    function* (input: {
      readonly verificationId: string;
      readonly commandId: CommandId;
      readonly now: string;
      readonly reason: string;
    }) {
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const current = yield* get(input.verificationId);
          if (current === null || current.execution !== "queued") return current;
          const record: VerificationRecord = {
            ...current,
            execution: "failed",
            finishedAt: input.now,
            terminationReason: input.reason,
            currentness: current.currentness === "Historical only" ? "Historical only" : "Unknown",
          };
          yield* update(record, input.commandId);
          return record;
        }),
      );
    },
    Effect.mapError((cause) => new VerificationStoreError({ operation: "fail-queued", cause })),
  );
  const putOutput = Effect.fn("VerificationStore.putOutput")(
    function* (input: {
      readonly verificationId: string;
      readonly referenceId: string;
      readonly kind: string;
      readonly text: string;
      readonly truncated: boolean;
    }) {
      if (Buffer.byteLength(input.text) > 8 * 1024 * 1024)
        return yield* new VerificationStoreError({ operation: "output-limit" });
      yield* sql`INSERT INTO orchestration_v2_verification_output(reference_id, verification_id, kind, content, truncated) VALUES (${input.referenceId}, ${input.verificationId}, ${input.kind}, ${input.text}, ${input.truncated ? 1 : 0}) ON CONFLICT(reference_id) DO NOTHING`;
    },
    Effect.mapError((cause) =>
      isStoreError(cause)
        ? cause
        : new VerificationStoreError({ operation: "output-write", cause }),
    ),
  );
  const getOutput = Effect.fn("VerificationStore.getOutput")(
    function* (input: {
      readonly verificationId: string;
      readonly referenceId: string;
      readonly offset: number;
      readonly limit: number;
    }) {
      if (
        !Number.isSafeInteger(input.offset) ||
        input.offset < 0 ||
        !Number.isSafeInteger(input.limit) ||
        input.limit < 1 ||
        input.limit > 65_536
      )
        return yield* new VerificationStoreError({ operation: "output-page" });
      const rows = yield* sql<{
        readonly content: string;
        readonly truncated: number;
      }>`SELECT content, truncated FROM orchestration_v2_verification_output WHERE reference_id = ${input.referenceId} AND verification_id = ${input.verificationId}`;
      if (!rows[0]) return null;
      const bytes = Buffer.from(rows[0].content);
      const end = Math.min(bytes.length, input.offset + input.limit);
      return {
        text: bytes.subarray(input.offset, end).toString("utf8"),
        truncated: rows[0].truncated === 1,
        nextOffset: end < bytes.length ? end : null,
      };
    },
    Effect.mapError((cause) =>
      isStoreError(cause) ? cause : new VerificationStoreError({ operation: "output-read", cause }),
    ),
  );
  const listRecoverable = Effect.gen(function* () {
    const rows = yield* sql<{
      readonly payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_verification_attempts WHERE execution IN ('queued', 'running') ORDER BY sequence`;
    return yield* Effect.forEach(rows, (row) => decode(row.payload_json));
  }).pipe(
    Effect.mapError((cause) => new VerificationStoreError({ operation: "recover-list", cause })),
  );
  const interruptRunning = Effect.fn("VerificationStore.interruptRunning")(
    function* (input: { readonly serverInstanceId: string; readonly now: string }) {
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const rows = yield* sql<{
            readonly payload_json: string;
          }>`SELECT payload_json FROM orchestration_v2_verification_attempts WHERE execution = 'running' AND (server_instance_id IS NULL OR server_instance_id <> ${input.serverInstanceId})`;
          const records: VerificationRecord[] = [];
          for (const row of rows) {
            const old = yield* decode(row.payload_json);
            const record: VerificationRecord = {
              ...old,
              execution: "interrupted",
              currentness: old.currentness === "Historical only" ? "Historical only" : "Unknown",
              finishedAt: input.now,
              terminationReason: "server-restart",
            };
            yield* update(record, `${record.verificationId}:interrupted` as CommandId);
            records.push(record);
          }
          return records;
        }),
      );
    },
    Effect.mapError((cause) => new VerificationStoreError({ operation: "interrupt", cause })),
  );

  return VerificationStore.of({
    get,
    getByCommandId,
    latestForThread,
    listForCheckout,
    listForThread,
    listEvidence,
    failQueued,
    queue,
    claim,
    finish,
    requestCancel,
    isCancellationRequested,
    invalidate,
    putOutput,
    getOutput,
    listRecoverable,
    interruptRunning,
  });
});

export const layer = Layer.effect(VerificationStore, make);
