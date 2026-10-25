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
  OwnershipConflict,
} from "./checkoutOwnership.ts";
import { VerificationRecord } from "./verification.ts";
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
      ? `Checkout is owned by thread ${this.conflict?.owner.threadId ?? "unknown"}. Open the owner, wait, or create an isolated worktree.`
      : `Workspace execution request could not proceed: ${this.reason}.`;
  }
}

const threadInput = Schema.Struct({ threadId: ThreadId });
const verificationInput = Schema.Struct({ verificationId: TrimmedNonEmptyString });

export const WorkspaceExecutionRpcGroup = RpcGroup.make(
  Rpc.make(WORKSPACE_EXECUTION_METHODS.inspect, {
    payload: threadInput,
    success: Schema.NullOr(CheckoutOwnershipRecord),
    error: WorkspaceExecutionRequestError,
  }),
  Rpc.make(WORKSPACE_EXECUTION_METHODS.wait, {
    payload: Schema.Struct({ threadId: ThreadId, commandId: CommandId }),
    success: CheckoutOwnershipWait,
    error: WorkspaceExecutionRequestError,
  }),
  Rpc.make(WORKSPACE_EXECUTION_METHODS.cancelWait, {
    payload: Schema.Struct({
      threadId: ThreadId,
      commandId: CommandId,
      waitId: TrimmedNonEmptyString,
    }),
    success: CheckoutOwnershipWait,
    error: WorkspaceExecutionRequestError,
  }),
  Rpc.make(WORKSPACE_EXECUTION_METHODS.request, {
    payload: Schema.Struct({
      threadId: ThreadId,
      commandId: CommandId,
      sourceRunId: Schema.optionalKey(RunId),
    }),
    success: VerificationRecord,
    error: WorkspaceExecutionRequestError,
  }),
  Rpc.make(WORKSPACE_EXECUTION_METHODS.cancel, {
    payload: Schema.Struct({ verificationId: TrimmedNonEmptyString, commandId: CommandId }),
    success: VerificationRecord,
    error: WorkspaceExecutionRequestError,
  }),
  Rpc.make(WORKSPACE_EXECUTION_METHODS.get, {
    payload: verificationInput,
    success: Schema.NullOr(VerificationRecord),
    error: WorkspaceExecutionRequestError,
  }),
  Rpc.make(WORKSPACE_EXECUTION_METHODS.latest, {
    payload: threadInput,
    success: Schema.NullOr(VerificationRecord),
    error: WorkspaceExecutionRequestError,
  }),
  Rpc.make(WORKSPACE_EXECUTION_METHODS.output, {
    payload: Schema.Struct({
      verificationId: TrimmedNonEmptyString,
      referenceId: TrimmedNonEmptyString,
      offset: NonNegativeInt,
      limit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 65_536 })),
    }),
    success: Schema.Struct({
      text: Schema.String,
      truncated: Schema.Boolean,
      nextOffset: Schema.NullOr(NonNegativeInt),
    }),
    error: WorkspaceExecutionRequestError,
  }),
).middleware(RpcScopeAuthorization);
