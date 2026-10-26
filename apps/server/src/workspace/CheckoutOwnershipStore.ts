import {
  AcquireCheckoutOwnershipResult,
  CheckoutOwnershipRecord,
  CheckoutOwnershipWait,
  CommandId,
  OwnershipHandle,
  type AcquireCheckoutOwnershipInput,
  type OwnershipOperation,
  type OwnershipOwner,
  type ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import { decideCheckoutAcquisition } from "./CheckoutOwnershipDecider.ts";

export class CheckoutOwnershipStoreError extends Schema.TaggedError<CheckoutOwnershipStoreError>()(
  "CheckoutOwnershipStoreError",
  {
    operation: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Checkout ownership storage failed during ${this.operation}.`;
  }
}

export class StaleOwnershipHandle extends Schema.TaggedError<StaleOwnershipHandle>()(
  "StaleOwnershipHandle",
  {
    checkoutKey: Schema.String,
    generation: Schema.Number,
  },
) {
  override get message(): string {
    return "The checkout ownership handle is no longer current.";
  }
}

export type OwnershipTransition = {
  readonly commandId: CommandId;
  readonly handle: OwnershipHandle;
  readonly deferNotifications?: boolean;
} & (
  | { readonly type: "uncertain" | "release" }
  | {
      readonly type: "transfer";
      readonly nextOwner: OwnershipOwner;
      readonly nextOperation: OwnershipOperation;
    }
  | { readonly type: "operation"; readonly nextOperation: OwnershipOperation }
);

const recordJson = Schema.fromJsonString(CheckoutOwnershipRecord);
const decodeRecord = Schema.decodeUnknownSync(recordJson);
const encodeRecord = Schema.encodeSync(recordJson);
const decodeAcquire = Schema.decodeUnknownSync(
  Schema.fromJsonString(AcquireCheckoutOwnershipResult),
);
const waiterJson = Schema.fromJsonString(CheckoutOwnershipWait);
const decodeWait = Schema.decodeUnknownSync(waiterJson);
const encodeWait = Schema.encodeSync(waiterJson);
const decodeHandle = Schema.decodeUnknownSync(Schema.fromJsonString(OwnershipHandle));
const encodeReceipt = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const isStaleOwnershipHandle = Schema.is(StaleOwnershipHandle);
const encodeOwnerKey = Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)));

export const ownerKey = (owner: OwnershipOwner): string =>
  encodeOwnerKey([
    owner.kind,
    owner.threadId,
    owner.kind === "run"
      ? owner.runId
      : owner.kind === "terminal"
        ? owner.terminalId
        : owner.verificationId,
  ]);

export class CheckoutOwnershipStore extends Context.Service<
  CheckoutOwnershipStore,
  {
    readonly read: (
      checkoutKey: string,
    ) => Effect.Effect<CheckoutOwnershipRecord | null, CheckoutOwnershipStoreError>;
    readonly listRecoverable: Effect.Effect<
      ReadonlyArray<CheckoutOwnershipRecord>,
      CheckoutOwnershipStoreError
    >;
    readonly acquire: (
      input: AcquireCheckoutOwnershipInput & {
        readonly ownershipId: string;
        readonly serverInstanceId: string;
      },
    ) => Effect.Effect<AcquireCheckoutOwnershipResult, CheckoutOwnershipStoreError>;
    readonly transition: (
      input: OwnershipTransition,
    ) => Effect.Effect<OwnershipHandle | null, CheckoutOwnershipStoreError | StaleOwnershipHandle>;
    readonly enqueueWait: (
      input: AcquireCheckoutOwnershipInput & { readonly waitId: string },
    ) => Effect.Effect<CheckoutOwnershipWait, CheckoutOwnershipStoreError>;
    readonly cancelWait: (input: {
      readonly commandId: CommandId;
      readonly waitId: string;
      readonly threadId: ThreadId;
    }) => Effect.Effect<CheckoutOwnershipWait | null, CheckoutOwnershipStoreError>;
    readonly getWait: (
      waitId: string,
    ) => Effect.Effect<CheckoutOwnershipWait | null, CheckoutOwnershipStoreError>;
    readonly listWaiters: (
      checkoutKey?: string,
    ) => Effect.Effect<ReadonlyArray<CheckoutOwnershipWait>, CheckoutOwnershipStoreError>;
    readonly changes: Stream.Stream<string>;
    readonly notify: (checkoutKey: string) => Effect.Effect<void>;
  }
>()("t3/workspace/CheckoutOwnershipStore") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const changes = yield* PubSub.unbounded<string>();

  const read = Effect.fn("CheckoutOwnershipStore.read")(
    function* (checkoutKey: string) {
      const rows = yield* sql<{
        readonly payload_json: string;
      }>`SELECT payload_json FROM orchestration_v2_checkout_claims WHERE checkout_key = ${checkoutKey}`;
      return rows[0] ? yield* Effect.try(() => decodeRecord(rows[0]!.payload_json)) : null;
    },
    Effect.mapError((cause) => new CheckoutOwnershipStoreError({ operation: "read", cause })),
  );

  const receipt = Effect.fn("CheckoutOwnershipStore.receipt")(function* (commandId: CommandId) {
    const rows = yield* sql<{
      readonly command_type: string;
      readonly result_json: string;
    }>`SELECT command_type, result_json FROM orchestration_v2_workspace_receipts WHERE command_id = ${commandId}`;
    return rows[0] ?? null;
  });

  const recordReceipt = Effect.fn("CheckoutOwnershipStore.recordReceipt")(function* (
    commandId: CommandId,
    threadId: ThreadId,
    type: string,
    result: unknown,
    sequence: number,
    now: string,
  ) {
    yield* sql`INSERT INTO orchestration_v2_workspace_receipts(command_id, thread_id, command_type, result_json) VALUES (${commandId}, ${threadId}, ${type}, ${encodeReceipt(result)})`;
    yield* sql`INSERT INTO orchestration_command_receipts(command_id, aggregate_kind, aggregate_id, command_type, accepted_at, result_sequence, status, error)
      VALUES (${commandId}, 'thread', ${threadId}, ${type}, ${now}, ${sequence}, 'accepted', NULL)`;
  });

  const write = Effect.fn("CheckoutOwnershipStore.write")(function* (
    record: CheckoutOwnershipRecord,
    commandId: CommandId,
    kind: string,
    now: string,
  ) {
    yield* sql`INSERT INTO orchestration_v2_checkout_claims(checkout_key, environment_id, generation, state, owner_thread_id, server_instance_id, payload_json)
      VALUES (${record.identity.checkoutKey}, ${record.identity.environmentId}, ${record.handle.generation}, ${record.state}, ${record.handle.owner.threadId}, ${record.handle.serverInstanceId}, ${encodeRecord(record)})
      ON CONFLICT(checkout_key) DO UPDATE SET generation = excluded.generation, state = excluded.state, owner_thread_id = excluded.owner_thread_id, server_instance_id = excluded.server_instance_id, payload_json = excluded.payload_json`;
    const rows = yield* sql<{
      readonly sequence: number;
    }>`INSERT INTO orchestration_v2_checkout_events(command_id, checkout_key, kind, payload_json, created_at)
      VALUES (${commandId}, ${record.identity.checkoutKey}, ${kind}, ${encodeRecord(record)}, ${now}) RETURNING sequence`;
    return rows[0]!.sequence;
  });

  const acquire = Effect.fn("CheckoutOwnershipStore.acquire")(
    function* (
      input: AcquireCheckoutOwnershipInput & {
        readonly ownershipId: string;
        readonly serverInstanceId: string;
      },
    ) {
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const previous = yield* receipt(input.commandId);
          if (previous !== null)
            return yield* Effect.try(() => decodeAcquire(previous.result_json));
          const current = yield* read(input.identity.checkoutKey);
          const now = DateTime.formatIso(yield* DateTime.now);
          const result = yield* Effect.try(() =>
            decideCheckoutAcquisition({
              current,
              request: input,
              ownershipId: input.ownershipId,
              serverInstanceId: input.serverInstanceId,
              now,
            }),
          );
          let sequence = 0;
          if (result._tag === "Acquired") {
            sequence = yield* write(
              {
                identity: input.identity,
                handle: result.handle,
                state: "held",
                acquiredAt: now,
                updatedAt: now,
              },
              input.commandId,
              "acquired",
              now,
            );
          }
          yield* recordReceipt(
            input.commandId,
            input.owner.threadId,
            "workspace.acquire",
            result,
            sequence,
            now,
          );
          return result;
        }),
      );
    },
    Effect.mapError((cause) => new CheckoutOwnershipStoreError({ operation: "acquire", cause })),
  );

  const transition = Effect.fn("CheckoutOwnershipStore.transition")(
    function* (input: OwnershipTransition) {
      const result = yield* sql.withTransaction(
        Effect.gen(function* () {
          const previous = yield* receipt(input.commandId);
          if (previous !== null)
            return previous.result_json === "null"
              ? null
              : yield* Effect.try(() => decodeHandle(previous.result_json));
          const current = yield* read(input.handle.checkoutKey);
          const sameFence =
            current !== null &&
            current.state !== "released" &&
            current.handle.ownershipId === input.handle.ownershipId &&
            current.handle.generation === input.handle.generation &&
            current.handle.serverInstanceId === input.handle.serverInstanceId &&
            ownerKey(current.handle.owner) === ownerKey(input.handle.owner);
          if (!sameFence || current === null)
            return yield* new StaleOwnershipHandle({
              checkoutKey: input.handle.checkoutKey,
              generation: input.handle.generation,
            });
          const now = DateTime.formatIso(yield* DateTime.now);
          const advances = input.type === "transfer" || input.type === "operation";
          if (advances && current.handle.generation >= Number.MAX_SAFE_INTEGER)
            return yield* new StaleOwnershipHandle({
              checkoutKey: input.handle.checkoutKey,
              generation: input.handle.generation,
            });
          const handle: OwnershipHandle = advances
            ? {
                ...current.handle,
                generation: current.handle.generation + 1,
                owner: input.type === "transfer" ? input.nextOwner : current.handle.owner,
                operation: input.nextOperation,
              }
            : current.handle;
          const state =
            input.type === "release"
              ? "released"
              : input.type === "uncertain"
                ? "uncertain"
                : "held";
          const sequence = yield* write(
            { ...current, handle, state, updatedAt: now },
            input.commandId,
            input.type,
            now,
          );
          if (state === "released") {
            const rows = yield* sql<{
              readonly payload_json: string;
            }>`SELECT payload_json FROM orchestration_v2_checkout_waiters WHERE checkout_key = ${handle.checkoutKey} AND state = 'waiting' ORDER BY sequence LIMIT 1`;
            if (rows[0]) {
              const waiter = yield* Effect.try(() => decodeWait(rows[0]!.payload_json));
              yield* sql`UPDATE orchestration_v2_checkout_waiters SET state = 'ready', payload_json = ${encodeWait({ ...waiter, state: "ready" })} WHERE wait_id = ${waiter.waitId} AND state = 'waiting'`;
            }
          }
          const value = state === "released" ? null : handle;
          yield* recordReceipt(
            input.commandId,
            input.handle.owner.threadId,
            `workspace.${input.type}`,
            value,
            sequence,
            now,
          );
          return value;
        }),
      );
      if (!input.deferNotifications) yield* PubSub.publish(changes, input.handle.checkoutKey);
      return result;
    },
    Effect.mapError((cause) =>
      isStaleOwnershipHandle(cause)
        ? cause
        : new CheckoutOwnershipStoreError({ operation: "transition", cause }),
    ),
  );

  const getWait = Effect.fn("CheckoutOwnershipStore.getWait")(
    function* (waitId: string) {
      const rows = yield* sql<{
        readonly payload_json: string;
      }>`SELECT payload_json FROM orchestration_v2_checkout_waiters WHERE wait_id = ${waitId}`;
      return rows[0] ? yield* Effect.try(() => decodeWait(rows[0]!.payload_json)) : null;
    },
    Effect.mapError((cause) => new CheckoutOwnershipStoreError({ operation: "get-wait", cause })),
  );

  const enqueueWait = Effect.fn("CheckoutOwnershipStore.enqueueWait")(
    function* (input: AcquireCheckoutOwnershipInput & { readonly waitId: string }) {
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const previous = yield* receipt(input.commandId);
          if (previous !== null) return yield* Effect.try(() => decodeWait(previous.result_json));
          const claim = yield* read(input.identity.checkoutKey);
          const state = claim === null || claim.state === "released" ? "ready" : "waiting";
          const rows = yield* sql<{
            readonly sequence: number;
          }>`INSERT INTO orchestration_v2_checkout_waiters(wait_id, command_id, checkout_key, thread_id, state, payload_json)
        VALUES (${input.waitId}, ${input.commandId}, ${input.identity.checkoutKey}, ${input.owner.threadId}, ${state}, '{}') RETURNING sequence`;
          const waiter: CheckoutOwnershipWait = {
            waitId: input.waitId,
            input,
            state,
            sequence: rows[0]!.sequence,
          };
          yield* sql`UPDATE orchestration_v2_checkout_waiters SET payload_json = ${encodeWait(waiter)} WHERE wait_id = ${input.waitId}`;
          yield* recordReceipt(
            input.commandId,
            input.owner.threadId,
            "workspace.wait",
            waiter,
            0,
            DateTime.formatIso(yield* DateTime.now),
          );
          return waiter;
        }),
      );
    },
    Effect.mapError((cause) => new CheckoutOwnershipStoreError({ operation: "wait", cause })),
  );

  const cancelWait = Effect.fn("CheckoutOwnershipStore.cancelWait")(
    function* (input: {
      readonly commandId: CommandId;
      readonly waitId: string;
      readonly threadId: ThreadId;
    }) {
      const result = yield* sql.withTransaction(
        Effect.gen(function* () {
          const previous = yield* receipt(input.commandId);
          if (previous !== null)
            return previous.result_json === "null"
              ? null
              : yield* Effect.try(() => decodeWait(previous.result_json));
          const current = yield* getWait(input.waitId);
          if (current !== null && current.input.owner.threadId === input.threadId) {
            const claim = yield* read(current.input.identity.checkoutKey);
            if (
              claim !== null &&
              claim.state !== "released" &&
              ownerKey(claim.handle.owner) === ownerKey(current.input.owner)
            ) {
              return yield* new CheckoutOwnershipStoreError({
                operation: "wait-already-admitted",
                cause: new StaleOwnershipHandle({
                  checkoutKey: claim.identity.checkoutKey,
                  generation: claim.handle.generation,
                }),
              });
            }
          }
          const waiter: CheckoutOwnershipWait | null =
            current === null || current.input.owner.threadId !== input.threadId
              ? null
              : { ...current, state: "cancelled" };
          if (waiter !== null)
            yield* sql`UPDATE orchestration_v2_checkout_waiters SET state = 'cancelled', payload_json = ${encodeWait(waiter)} WHERE wait_id = ${input.waitId}`;
          yield* recordReceipt(
            input.commandId,
            input.threadId,
            "workspace.cancel-wait",
            waiter,
            0,
            DateTime.formatIso(yield* DateTime.now),
          );
          return waiter;
        }),
      );
      if (result !== null) yield* PubSub.publish(changes, result.input.identity.checkoutKey);
      return result;
    },
    Effect.mapError(
      (cause) => new CheckoutOwnershipStoreError({ operation: "cancel-wait", cause }),
    ),
  );

  const listRecoverable = Effect.gen(function* () {
    const rows = yield* sql<{
      readonly payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_checkout_claims WHERE state <> 'released'`;
    return yield* Effect.try(() => rows.map((row) => decodeRecord(row.payload_json)));
  }).pipe(
    Effect.mapError((cause) => new CheckoutOwnershipStoreError({ operation: "recover", cause })),
  );

  const listWaiters = Effect.fn("CheckoutOwnershipStore.listWaiters")(
    function* (checkoutKey?: string) {
      const rows = yield* sql<{
        readonly payload_json: string;
      }>`SELECT payload_json FROM orchestration_v2_checkout_waiters WHERE ${checkoutKey === undefined ? sql`1 = 1` : sql`checkout_key = ${checkoutKey}`} ORDER BY sequence`;
      return yield* Effect.try(() => rows.map((row) => decodeWait(row.payload_json)));
    },
    Effect.mapError(
      (cause) => new CheckoutOwnershipStoreError({ operation: "list-waiters", cause }),
    ),
  );

  return CheckoutOwnershipStore.of({
    read,
    acquire,
    transition,
    enqueueWait,
    cancelWait,
    getWait,
    listRecoverable,
    listWaiters,
    changes: Stream.fromPubSub(changes),
    notify: (checkoutKey) => PubSub.publish(changes, checkoutKey).pipe(Effect.asVoid),
  });
});

export const layer = Layer.effect(CheckoutOwnershipStore, make);
