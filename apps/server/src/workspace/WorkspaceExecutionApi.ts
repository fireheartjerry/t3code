import {
  WorkspaceExecutionRequestError,
  type CheckoutOwnershipRecord,
  type CheckoutOwnershipWait,
  type CommandId,
  type RunId,
  type ThreadId,
  type VerificationRecord,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

type ApiEffect<A> = Effect.Effect<A, WorkspaceExecutionRequestError>;

export class WorkspaceExecutionAvailable extends Context.Reference<boolean>(
  "t3/workspace/WorkspaceExecutionAvailable",
  { defaultValue: () => false },
) {}

export class WorkspaceExecutionApi extends Context.Reference<{
  readonly inspect: (threadId: ThreadId) => ApiEffect<CheckoutOwnershipRecord | null>;
  readonly wait: (input: {
    readonly threadId: ThreadId;
    readonly commandId: CommandId;
    readonly checkoutKey: string;
    readonly generation: number;
  }) => ApiEffect<CheckoutOwnershipWait>;
  readonly cancelWait: (input: {
    readonly threadId: ThreadId;
    readonly commandId: CommandId;
    readonly waitId: string;
  }) => ApiEffect<CheckoutOwnershipWait | null>;
  readonly request: (input: {
    readonly commandId: CommandId;
    readonly threadId: ThreadId;
    readonly sourceRunId?: RunId;
  }) => ApiEffect<VerificationRecord>;
  readonly cancel: (input: {
    readonly commandId: CommandId;
    readonly verificationId: string;
  }) => ApiEffect<VerificationRecord | null>;
  readonly get: (id: string) => ApiEffect<VerificationRecord | null>;
  readonly latest: (threadId: ThreadId) => ApiEffect<VerificationRecord | null>;
  readonly output: (input: {
    readonly verificationId: string;
    readonly referenceId: string;
    readonly offset: number;
    readonly limit: number;
  }) => ApiEffect<{
    readonly text: string;
    readonly truncated: boolean;
    readonly nextOffset: number | null;
  } | null>;
}>("t3/workspace/WorkspaceExecutionApi", {
  defaultValue: () => {
    const unavailable = () =>
      Effect.fail(new WorkspaceExecutionRequestError({ reason: "unavailable" }));
    return {
      inspect: unavailable,
      wait: unavailable,
      cancelWait: unavailable,
      request: unavailable,
      cancel: unavailable,
      get: unavailable,
      latest: unavailable,
      output: unavailable,
    };
  },
}) {}
