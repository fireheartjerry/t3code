import {
  AcquireCheckoutOwnershipResult,
  CheckoutIdentity,
  CommandId,
  OwnershipHandle,
  OwnershipOwner,
  type AcquireCheckoutOwnershipInput,
  type CheckoutOwnershipRecord,
  type CheckoutOwnershipWait,
  type OwnershipOperation,
  type ThreadId,
} from "@t3tools/contracts";
import { randomUuidV4 } from "@t3tools/provider-core/server/randomUuid";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { isSqlError } from "effect/sql/SqlError";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as Identity from "./CheckoutIdentity.ts";
import * as Store from "./CheckoutOwnershipStore.ts";
import * as Accounting from "./CheckoutProcessAccounting.ts";

export class CheckoutOwnershipServiceError extends Schema.TaggedError<CheckoutOwnershipServiceError>()(
  "CheckoutOwnershipServiceError",
  {
    checkoutKey: Schema.String,
    reason: Schema.Literals([
      "environment-mismatch",
      "identity-changed",
      "owner-live",
      "owner-unknown",
      "command-mismatch",
    ]),
  },
) {
  override get message(): string {
    return `Checkout ownership cannot proceed: ${this.reason}.`;
  }
}

type OwnershipError =
  | CheckoutOwnershipServiceError
  | Identity.CheckoutIdentityError
  | Store.CheckoutOwnershipStoreError
  | Store.StaleOwnershipHandle;

type TransferInput = {
  readonly commandId: CommandId;
  readonly handle: OwnershipHandle;
  readonly nextOwner: OwnershipOwner;
  readonly nextOperation: OwnershipOperation;
};

export class CheckoutOwnershipService extends Context.Service<
  CheckoutOwnershipService,
  {
    readonly serverInstanceId: string;
    readonly acquire: (
      input: AcquireCheckoutOwnershipInput,
    ) => Effect.Effect<AcquireCheckoutOwnershipResult, OwnershipError>;
    /** Server internal. persistIntent must perform SQL writes only, in the acquisition transaction. */
    readonly acquireWithIntent?: <A, E, R>(input: {
      readonly request: AcquireCheckoutOwnershipInput;
      readonly persistIntent: (handle: OwnershipHandle) => Effect.Effect<A, E, R>;
    }) => Effect.Effect<AcquireCheckoutOwnershipResult, E | OwnershipError, R>;
    readonly inspect: (input: {
      readonly identity: CheckoutIdentity;
    }) => Effect.Effect<CheckoutOwnershipRecord | null, OwnershipError>;
    readonly assertCurrent: (handle: OwnershipHandle) => Effect.Effect<void, OwnershipError>;
    readonly withCurrent: <A, E, R>(
      handle: OwnershipHandle,
      effect: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | OwnershipError, R>;
    readonly enqueueWait: (
      input: AcquireCheckoutOwnershipInput,
    ) => Effect.Effect<CheckoutOwnershipWait, OwnershipError>;
    readonly cancelWait: (input: {
      readonly commandId: CommandId;
      readonly waitId: string;
      readonly threadId: ThreadId;
    }) => Effect.Effect<CheckoutOwnershipWait | null, OwnershipError>;
    readonly advanceOperation: (input: {
      readonly commandId: CommandId;
      readonly handle: OwnershipHandle;
      readonly nextOperation: OwnershipOperation;
    }) => Effect.Effect<OwnershipHandle, OwnershipError>;
    readonly transfer: (input: TransferInput) => Effect.Effect<OwnershipHandle, OwnershipError>;
    /** Server internal. persistIntent must perform SQL writes only, in the supplied transaction. */
    readonly transferWithIntent: <A, E, R>(
      input: TransferInput & {
        readonly persistIntent: (nextHandle: OwnershipHandle) => Effect.Effect<A, E, R>;
      },
    ) => Effect.Effect<OwnershipHandle, E | OwnershipError, R>;
    readonly releaseWhenQuiescent: (input: {
      readonly commandId: CommandId;
      readonly handle: OwnershipHandle;
    }) => Effect.Effect<"released" | "uncertain", OwnershipError>;
    /** Recovery alone may reconcile a prior boot; it cannot adopt or start that owner's work. */
    readonly reconcileClaim: (handle: OwnershipHandle) => Effect.Effect<void, OwnershipError>;
  }
>()("t3/workspace/CheckoutOwnershipService") {}

const sameIdentity = Schema.toEquivalence(CheckoutIdentity);
const sameOwner = Schema.toEquivalence(OwnershipOwner);
const isReceiptDomainError = Schema.is(
  Schema.Union([CheckoutOwnershipServiceError, Store.StaleOwnershipHandle]),
);
const decodeTransition = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.NullOr(OwnershipHandle)),
);
const decodeAcquisition = Schema.decodeUnknownSync(
  Schema.fromJsonString(AcquireCheckoutOwnershipResult),
);

const make = Effect.gen(function* () {
  const store = yield* Store.CheckoutOwnershipStore;
  const resolver = yield* Identity.CheckoutIdentityResolver;
  const accounting = yield* Accounting.CheckoutProcessAccounting;
  const environment = yield* ServerEnvironment.ServerEnvironmentIdentity;
  const sql = yield* SqlClient.SqlClient;
  const environmentId = yield* environment.getEnvironmentId;
  const serverInstanceId = yield* randomUuidV4;
  const locks = yield* KeyedLock.make<string>();

  const validateIdentity = Effect.fn("CheckoutOwnershipService.validateIdentity")(function* (
    identity: CheckoutIdentity,
  ) {
    if (identity.environmentId !== environmentId) {
      return yield* new CheckoutOwnershipServiceError({
        checkoutKey: identity.checkoutKey,
        reason: "environment-mismatch",
      });
    }
    const resolved = yield* resolver.resolve({ environmentId, cwd: identity.canonicalCwd });
    if (!sameIdentity(identity, resolved)) {
      return yield* new CheckoutOwnershipServiceError({
        checkoutKey: identity.checkoutKey,
        reason: "identity-changed",
      });
    }
  });

  const stale = (handle: OwnershipHandle) =>
    new Store.StaleOwnershipHandle({
      checkoutKey: handle.checkoutKey,
      generation: handle.generation,
    });

  const sameFence = (record: CheckoutOwnershipRecord | null, handle: OwnershipHandle) =>
    record !== null &&
    record.state !== "released" &&
    record.identity.checkoutKey === handle.checkoutKey &&
    record.handle.ownershipId === handle.ownershipId &&
    record.handle.generation === handle.generation &&
    record.handle.serverInstanceId === handle.serverInstanceId &&
    record.handle.operation === handle.operation &&
    sameOwner(record.handle.owner, handle.owner);

  // Called only inside an operation's checkout lock. These helpers must never retake it.
  const current = Effect.fn("CheckoutOwnershipService.current")(function* (
    handle: OwnershipHandle,
    allowUncertain = false,
  ) {
    const record = yield* store.read(handle.checkoutKey);
    if (
      handle.serverInstanceId !== serverInstanceId ||
      !sameFence(record, handle) ||
      record === null ||
      (!allowUncertain && record.state !== "held")
    ) {
      return yield* stale(handle);
    }
    yield* validateIdentity(record.identity);
    return record;
  });

  // Replays do not rerun intent callbacks or require an already-advanced fence to remain held.
  const transitionReceipt = Effect.fn("CheckoutOwnershipService.transitionReceipt")(
    function* (commandId: CommandId, handle: OwnershipHandle, operation: string) {
      if (handle.serverInstanceId !== serverInstanceId) return yield* stale(handle);
      const rows = yield* sql<{
        readonly command_type: string;
        readonly result_json: string;
      }>`SELECT command_type, result_json FROM orchestration_v2_workspace_receipts WHERE command_id = ${commandId}`;
      const row = rows[0];
      if (row === undefined) return null;
      if (
        row.command_type !== `workspace.${operation}` &&
        row.command_type !== "workspace.uncertain"
      ) {
        return yield* new CheckoutOwnershipServiceError({
          checkoutKey: handle.checkoutKey,
          reason: "command-mismatch",
        });
      }
      const result = yield* Effect.try(() => decodeTransition(row.result_json));
      if (
        result !== null &&
        (result.checkoutKey !== handle.checkoutKey ||
          result.ownershipId !== handle.ownershipId ||
          result.serverInstanceId !== serverInstanceId)
      ) {
        return yield* stale(handle);
      }
      return { type: row.command_type, handle: result };
    },
    Effect.mapError((cause) =>
      isReceiptDomainError(cause)
        ? cause
        : new Store.CheckoutOwnershipStoreError({ operation: "receipt", cause }),
    ),
  );

  const acquire = Effect.fn("CheckoutOwnershipService.acquire")(
    function* (input: AcquireCheckoutOwnershipInput) {
      yield* validateIdentity(input.identity);
      const ownershipId = yield* randomUuidV4;
      return yield* store.acquire({ ...input, ownershipId, serverInstanceId });
    },
    (effect, input) => locks.withLock(input.identity.checkoutKey, effect),
  );

  const acquireWithIntent = Effect.fn("CheckoutOwnershipService.acquireWithIntent")(
    function* <A, E, R>(input: {
      readonly request: AcquireCheckoutOwnershipInput;
      readonly persistIntent: (handle: OwnershipHandle) => Effect.Effect<A, E, R>;
    }) {
      yield* validateIdentity(input.request.identity);
      const ownershipId = yield* randomUuidV4;
      const committed = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            // Check inside the transaction so concurrent retries cannot persist the intent twice.
            const rows = yield* sql<{
              readonly command_type: string;
              readonly result_json: string;
            }>`SELECT command_type, result_json FROM orchestration_v2_workspace_receipts WHERE command_id = ${input.request.commandId}`;
            const previous = rows[0];
            if (previous !== undefined) {
              if (previous.command_type !== "workspace.acquire") {
                return yield* new CheckoutOwnershipServiceError({
                  checkoutKey: input.request.identity.checkoutKey,
                  reason: "command-mismatch",
                });
              }
              const result = yield* Effect.try({
                try: () => decodeAcquisition(previous.result_json),
                catch: (cause) =>
                  new Store.CheckoutOwnershipStoreError({
                    operation: "acquire-intent-receipt",
                    cause,
                  }),
              });
              return { result, notify: false };
            }
            const result = yield* store.acquire({
              ...input.request,
              ownershipId,
              serverInstanceId,
            });
            if (result._tag === "Acquired") yield* input.persistIntent(result.handle);
            return { result, notify: result._tag === "Acquired" };
          }),
        )
        .pipe(
          Effect.mapError((cause) =>
            isSqlError(cause)
              ? new Store.CheckoutOwnershipStoreError({ operation: "acquire-intent", cause })
              : cause,
          ),
        );
      if (committed.notify) yield* store.notify(input.request.identity.checkoutKey);
      return committed.result;
    },
    (effect, input) => locks.withLock(input.request.identity.checkoutKey, effect),
  );

  const inspect = Effect.fn("CheckoutOwnershipService.inspect")(function* ({
    identity,
  }: {
    readonly identity: CheckoutIdentity;
  }) {
    yield* validateIdentity(identity);
    return yield* store.read(identity.checkoutKey);
  });

  const withCurrent = Effect.fn("CheckoutOwnershipService.withCurrent")(
    function* <A, E, R>(handle: OwnershipHandle, effect: Effect.Effect<A, E, R>) {
      yield* current(handle);
      return yield* effect;
    },
    (effect, handle) => locks.withLock(handle.checkoutKey, effect),
  );

  const assertCurrent = Effect.fn("CheckoutOwnershipService.assertCurrent")(function* (
    handle: OwnershipHandle,
  ) {
    yield* withCurrent(handle, Effect.void);
  });

  const enqueueWait = Effect.fn("CheckoutOwnershipService.enqueueWait")(
    function* (input: AcquireCheckoutOwnershipInput) {
      yield* validateIdentity(input.identity);
      const waiter = yield* store.enqueueWait({ ...input, waitId: yield* randomUuidV4 });
      // The store's first result carries internal allocation fields; receipts decode only the contract.
      return {
        ...waiter,
        input: {
          commandId: waiter.input.commandId,
          identity: waiter.input.identity,
          owner: waiter.input.owner,
          operation: waiter.input.operation,
        },
      };
    },
    (effect, input) => locks.withLock(input.identity.checkoutKey, effect),
  );

  const cancelWait = Effect.fn("CheckoutOwnershipService.cancelWait")(function* (input: {
    readonly commandId: CommandId;
    readonly waitId: string;
    readonly threadId: ThreadId;
  }) {
    return yield* store.cancelWait(input);
  });

  const advanceOperation = Effect.fn("CheckoutOwnershipService.advanceOperation")(
    function* (input: {
      readonly commandId: CommandId;
      readonly handle: OwnershipHandle;
      readonly nextOperation: OwnershipOperation;
    }) {
      const receipt = yield* transitionReceipt(input.commandId, input.handle, "operation");
      if (receipt !== null) {
        if (receipt.type !== "workspace.operation" || receipt.handle === null)
          return yield* stale(input.handle);
        return receipt.handle;
      }
      yield* current(input.handle);
      const handle = yield* store.transition({ ...input, type: "operation" });
      if (handle === null) return yield* stale(input.handle);
      return handle;
    },
    (effect, input) => locks.withLock(input.handle.checkoutKey, effect),
  );

  const transferWithIntent = Effect.fn("CheckoutOwnershipService.transferWithIntent")(
    function* <A, E, R>(
      input: TransferInput & {
        readonly persistIntent: (nextHandle: OwnershipHandle) => Effect.Effect<A, E, R>;
      },
    ) {
      const receipt = yield* transitionReceipt(input.commandId, input.handle, "transfer");
      if (receipt !== null) {
        if (receipt.type === "workspace.uncertain") {
          return yield* new CheckoutOwnershipServiceError({
            checkoutKey: input.handle.checkoutKey,
            reason: "owner-unknown",
          });
        }
        if (receipt.handle === null) return yield* stale(input.handle);
        return receipt.handle;
      }
      yield* current(input.handle, true);
      const observation = yield* accounting.observe(input.handle.owner);
      if (observation.state !== "stopped") {
        if (observation.state === "unknown") {
          yield* store.transition({
            type: "uncertain",
            commandId: input.commandId,
            handle: input.handle,
          });
        }
        return yield* new CheckoutOwnershipServiceError({
          checkoutKey: input.handle.checkoutKey,
          reason: observation.state === "live" ? "owner-live" : "owner-unknown",
        });
      }
      // Resolution and observation precede the transaction; only SQL intent persistence belongs here.
      const handle = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const nextHandle = yield* store.transition({
              type: "transfer",
              commandId: input.commandId,
              handle: input.handle,
              nextOwner: input.nextOwner,
              nextOperation: input.nextOperation,
              deferNotifications: true,
            });
            if (nextHandle === null) return yield* stale(input.handle);
            yield* input.persistIntent(nextHandle);
            return nextHandle;
          }),
        )
        .pipe(
          Effect.mapError((cause) =>
            isSqlError(cause)
              ? new Store.CheckoutOwnershipStoreError({ operation: "transfer-intent", cause })
              : cause,
          ),
        );
      yield* store.notify(handle.checkoutKey);
      return handle;
    },
    (effect, input) => locks.withLock(input.handle.checkoutKey, effect),
  );

  const transfer = Effect.fn("CheckoutOwnershipService.transfer")(function* (input: TransferInput) {
    return yield* transferWithIntent({ ...input, persistIntent: () => Effect.void });
  });

  const releaseWhenQuiescent = Effect.fn("CheckoutOwnershipService.releaseWhenQuiescent")(
    function* (input: { readonly commandId: CommandId; readonly handle: OwnershipHandle }) {
      const receipt = yield* transitionReceipt(input.commandId, input.handle, "release");
      if (receipt !== null)
        return receipt.type === "workspace.release"
          ? ("released" as const)
          : ("uncertain" as const);
      yield* current(input.handle, true);
      const observation = yield* accounting.observe(input.handle.owner);
      if (observation.state === "live") return "uncertain" as const;
      yield* store.transition({
        ...input,
        type: observation.state === "stopped" ? "release" : "uncertain",
      });
      return observation.state === "stopped" ? ("released" as const) : ("uncertain" as const);
    },
    (effect, input) => locks.withLock(input.handle.checkoutKey, effect),
  );

  const reconcileClaim = Effect.fn("CheckoutOwnershipService.reconcileClaim")(
    function* (handle: OwnershipHandle) {
      const record = yield* store.read(handle.checkoutKey);
      // A concurrent finalization or handoff may have superseded recovery's snapshot.
      if (record === null || !sameFence(record, handle)) return;
      const identityValid = yield* validateIdentity(record.identity).pipe(
        Effect.as(true),
        Effect.catchTags({
          CheckoutOwnershipServiceError: () => Effect.succeed(false),
          CheckoutIdentityError: () => Effect.succeed(false),
        }),
      );
      const observation = yield* accounting.observe(handle.owner);
      const release = identityValid && observation.state === "stopped";
      const uncertain =
        !identityValid ||
        handle.serverInstanceId !== serverInstanceId ||
        observation.state === "unknown";
      if (!release && (!uncertain || record.state === "uncertain")) return;
      yield* store.transition({
        type: release ? "release" : "uncertain",
        commandId: CommandId.make(`checkout-recovery:${yield* randomUuidV4}`),
        handle,
      });
    },
    (effect, handle) => locks.withLock(handle.checkoutKey, effect),
  );

  return CheckoutOwnershipService.of({
    serverInstanceId,
    acquire,
    acquireWithIntent,
    inspect,
    assertCurrent,
    withCurrent,
    enqueueWait,
    cancelWait,
    advanceOperation,
    transfer,
    transferWithIntent,
    releaseWhenQuiescent,
    reconcileClaim,
  });
});

export const layer = Layer.effect(CheckoutOwnershipService, make);
