import * as Schema from "effect/Schema";

import {
  CommandId,
  EnvironmentId,
  IsoDateTime,
  RunId,
  ThreadId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";

export const OwnershipGeneration = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
);
export const WorkspaceOwnershipPolicy = Schema.Literals(["off", "enforce"]);
export type WorkspaceOwnershipPolicy = typeof WorkspaceOwnershipPolicy.Type;

export const CheckoutIdentity = Schema.Struct({
  environmentId: EnvironmentId,
  checkoutKey: TrimmedNonEmptyString,
  canonicalCwd: Schema.String.check(Schema.isMinLength(1)),
  filesystemIdentity: Schema.Struct({ device: TrimmedNonEmptyString, file: TrimmedNonEmptyString }),
  gitDir: Schema.NullOr(Schema.String.check(Schema.isMinLength(1))),
  gitCommonDir: Schema.NullOr(Schema.String.check(Schema.isMinLength(1))),
});
export type CheckoutIdentity = typeof CheckoutIdentity.Type;

export const OwnershipOwner = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("run"), threadId: ThreadId, runId: RunId }),
  Schema.Struct({
    kind: Schema.Literal("terminal"),
    threadId: ThreadId,
    terminalId: TrimmedNonEmptyString,
  }),
  Schema.Struct({
    kind: Schema.Literal("verification"),
    threadId: ThreadId,
    verificationId: TrimmedNonEmptyString,
  }),
]);
export type OwnershipOwner = typeof OwnershipOwner.Type;

export const OwnershipOperation = Schema.Literals([
  "setup",
  "provider",
  "terminal",
  "finalization",
  "verification",
]);
export type OwnershipOperation = typeof OwnershipOperation.Type;

export const OwnershipHandle = Schema.Struct({
  ownershipId: TrimmedNonEmptyString,
  checkoutKey: TrimmedNonEmptyString,
  generation: OwnershipGeneration,
  owner: OwnershipOwner,
  operation: OwnershipOperation,
  serverInstanceId: TrimmedNonEmptyString,
});
export type OwnershipHandle = typeof OwnershipHandle.Type;

export const OwnershipConflict = Schema.Struct({
  identity: CheckoutIdentity,
  owner: OwnershipOwner,
  operation: OwnershipOperation,
  generation: OwnershipGeneration,
  state: Schema.Literals(["held", "uncertain"]),
  resolutions: Schema.Array(
    Schema.Struct({
      action: Schema.Literals(["open-owner", "wait", "create-worktree", "stop-owner"]),
      threadId: ThreadId,
      checkoutKey: TrimmedNonEmptyString,
    }),
  ),
});
export type OwnershipConflict = typeof OwnershipConflict.Type;

export const CheckoutOwnershipRecord = Schema.Struct({
  identity: CheckoutIdentity,
  handle: OwnershipHandle,
  state: Schema.Literals(["held", "uncertain", "released"]),
  acquiredAt: IsoDateTime,
  updatedAt: IsoDateTime,
});
export type CheckoutOwnershipRecord = typeof CheckoutOwnershipRecord.Type;

export const AcquireCheckoutOwnershipInput = Schema.Struct({
  commandId: CommandId,
  identity: CheckoutIdentity,
  owner: OwnershipOwner,
  operation: OwnershipOperation,
});
export type AcquireCheckoutOwnershipInput = typeof AcquireCheckoutOwnershipInput.Type;

export const AcquireCheckoutOwnershipResult = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("Acquired"), handle: OwnershipHandle }),
  Schema.Struct({ _tag: Schema.Literal("Conflict"), conflict: OwnershipConflict }),
]);
export type AcquireCheckoutOwnershipResult = typeof AcquireCheckoutOwnershipResult.Type;

export const CheckoutOwnershipWait = Schema.Struct({
  waitId: TrimmedNonEmptyString,
  input: AcquireCheckoutOwnershipInput,
  state: Schema.Literals(["waiting", "ready", "cancelled"]),
  sequence: OwnershipGeneration,
});
export type CheckoutOwnershipWait = typeof CheckoutOwnershipWait.Type;
