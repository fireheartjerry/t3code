import * as Schema from "effect/Schema";
import * as Rpc from "effect/rpc/Rpc";
import * as RpcGroup from "effect/rpc/RpcGroup";

import {
  CommandId,
  NonNegativeInt,
  RunId,
  ThreadId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";
import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  AuthTerminalOperateScope,
} from "./auth.ts";
import {
  CheckoutOwnershipRecord,
  CheckoutOwnershipWait,
  OwnershipGeneration,
  OwnershipConflict,
} from "./checkoutOwnership.ts";
import { VerificationRecord, VERIFICATION_MAX_OUTPUT_BYTES } from "./verification.ts";
import { RpcScopeAuthorization } from "./rpc.ts";

export const WORKSPACE_EXECUTION_METHODS = {
  inspect: "workspaceOwnership.inspect",
  wait: "workspaceOwnership.wait",
  cancelWait: "workspaceOwnership.cancelWait",
  request: "verification.request",
  cancel: "verification.cancel",
  get: "verification.get",
  latest: "verification.latest",
  output: "verification.output",
} as const;

export const WORKSPACE_EXECUTION_REQUIRED_SCOPES = {
  [WORKSPACE_EXECUTION_METHODS.inspect]: [AuthOrchestrationReadScope],
  [WORKSPACE_EXECUTION_METHODS.wait]: [AuthOrchestrationOperateScope],
  [WORKSPACE_EXECUTION_METHODS.cancelWait]: [AuthOrchestrationOperateScope],
  [WORKSPACE_EXECUTION_METHODS.request]: [AuthOrchestrationOperateScope, AuthTerminalOperateScope],
  [WORKSPACE_EXECUTION_METHODS.cancel]: [AuthOrchestrationOperateScope],
  [WORKSPACE_EXECUTION_METHODS.get]: [AuthOrchestrationReadScope],
  [WORKSPACE_EXECUTION_METHODS.latest]: [AuthOrchestrationReadScope],
  [WORKSPACE_EXECUTION_METHODS.output]: [AuthOrchestrationReadScope],
} as const;

export class WorkspaceExecutionRequestError extends Schema.TaggedError<WorkspaceExecutionRequestError>()(
  "WorkspaceExecutionRequestError",
  {
    reason: Schema.Literals([
      "not-configured",
      "not-found",
      "conflict",
      "policy",
      "unavailable",
      "invalid-input",
    ]),
    conflict: Schema.optionalKey(OwnershipConflict),
  },
) {
  override get message(): string {
    return this.reason === "conflict"
      ? `${this.conflict?.state === "uncertain" ? "Process liveness is uncertain; checkout remains" : "Checkout is"} owned by thread ${this.conflict?.owner.threadId ?? "unknown"}. Open the owner, wait, or create an isolated worktree.`
      : `Workspace execution request could not proceed: ${this.reason}.`;
  }
}

const workspaceError = WorkspaceExecutionRequestError.check(
  Schema.makeFilter((error) => error.reason !== "conflict" || error.conflict !== undefined, {
    expected: "ownership details for a checkout conflict",
  }),
);
const activeOwnership = CheckoutOwnershipRecord.check(
  Schema.makeFilter((record) => record.state !== "released", {
    expected: "active checkout ownership",
  }),
);
const cancelledWait = CheckoutOwnershipWait.check(
  Schema.makeFilter((waiter) => waiter.state === "cancelled", { expected: "a cancelled waiter" }),
);

const threadInput = Schema.Struct({ threadId: ThreadId });
const verificationInput = Schema.Struct({ verificationId: TrimmedNonEmptyString });

export const WorkspaceExecutionRpcGroup = RpcGroup.make(
  Rpc.make(WORKSPACE_EXECUTION_METHODS.inspect, {
    payload: threadInput,
    success: Schema.NullOr(activeOwnership),
    error: workspaceError,
  }),
  Rpc.make(WORKSPACE_EXECUTION_METHODS.wait, {
    payload: Schema.Struct({
      threadId: ThreadId,
      commandId: CommandId,
      checkoutKey: TrimmedNonEmptyString,
      generation: OwnershipGeneration,
    }),
    success: CheckoutOwnershipWait,
    error: workspaceError,
  }),
  Rpc.make(WORKSPACE_EXECUTION_METHODS.cancelWait, {
    payload: Schema.Struct({
      threadId: ThreadId,
      commandId: CommandId,
      waitId: TrimmedNonEmptyString,
    }),
    success: cancelledWait,
    error: workspaceError,
  }),
  Rpc.make(WORKSPACE_EXECUTION_METHODS.request, {
    payload: Schema.Struct({
      threadId: ThreadId,
      commandId: CommandId,
      sourceRunId: Schema.optionalKey(RunId),
    }),
    success: VerificationRecord,
    error: workspaceError,
  }),
  Rpc.make(WORKSPACE_EXECUTION_METHODS.cancel, {
    payload: Schema.Struct({ verificationId: TrimmedNonEmptyString, commandId: CommandId }),
    success: VerificationRecord,
    error: workspaceError,
  }),
  Rpc.make(WORKSPACE_EXECUTION_METHODS.get, {
    payload: verificationInput,
    success: Schema.NullOr(VerificationRecord),
    error: workspaceError,
  }),
  Rpc.make(WORKSPACE_EXECUTION_METHODS.latest, {
    payload: threadInput,
    success: Schema.NullOr(VerificationRecord),
    error: workspaceError,
  }),
  Rpc.make(WORKSPACE_EXECUTION_METHODS.output, {
    payload: Schema.Struct({
      verificationId: TrimmedNonEmptyString,
      referenceId: TrimmedNonEmptyString,
      offset: NonNegativeInt.check(Schema.isLessThanOrEqualTo(VERIFICATION_MAX_OUTPUT_BYTES)),
      limit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 65_536 })),
    }),
    success: Schema.Struct({
      text: Schema.String.check(Schema.isMaxLength(65_536)),
      truncated: Schema.Boolean,
      nextOffset: Schema.NullOr(NonNegativeInt),
    }),
    error: workspaceError,
  }),
).middleware(RpcScopeAuthorization);
