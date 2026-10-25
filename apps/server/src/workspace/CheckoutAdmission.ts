import {
  OwnershipConflict,
  ProjectId,
  RunId,
  ThreadId,
  type CheckoutIdentity,
  type CommandId,
  type OwnershipHandle,
  type OwnershipOperation,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export class CheckoutAdmissionError extends Schema.TaggedError<CheckoutAdmissionError>()(
  "CheckoutAdmissionError",
  {
    threadId: ThreadId,
    conflict: Schema.optionalKey(OwnershipConflict),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    const conflict = this.conflict;
    return conflict === undefined
      ? "Checkout admission could not establish safe ownership."
      : `Checkout ${conflict.identity.canonicalCwd} on environment ${conflict.identity.environmentId} is owned by thread ${conflict.owner.threadId} (${conflict.operation}, ${conflict.state}). Open the owning thread, wait, or create an isolated worktree. Stop the owner through the normal stop flow; ownership is retained until its writing processes have stopped.`;
  }
}

export interface RunAdmissionInput {
  readonly commandId: CommandId;
  readonly projectId: ProjectId;
  readonly threadId: ThreadId;
  readonly runId: RunId | null;
  readonly cwd: string;
  readonly operation: OwnershipOperation;
  readonly setupTerminalId?: string;
}

export class ActiveCheckoutOwnership extends Context.Reference<OwnershipHandle | null>(
  "t3/workspace/ActiveCheckoutOwnership",
  { defaultValue: () => null },
) {}

export class CheckoutVerificationHandoff extends Context.Reference<{
  readonly request: (input: {
    readonly commandId: CommandId;
    readonly threadId: ThreadId;
    readonly sourceRunId: RunId;
    readonly handle: OwnershipHandle;
    readonly checkpointRef: string;
  }) => Effect.Effect<boolean, CheckoutAdmissionError>;
  readonly invalidate: (checkoutKey: string) => Effect.Effect<void, CheckoutAdmissionError>;
}>("t3/workspace/CheckoutVerificationHandoff", {
  defaultValue: () => ({ request: () => Effect.succeed(false), invalidate: () => Effect.void }),
}) {}

export class CheckoutWaitEffects extends Context.Reference<{
  readonly resume: (input: {
    readonly threadId: ThreadId;
    readonly waitId: string;
  }) => Effect.Effect<void, CheckoutAdmissionError>;
}>("t3/workspace/CheckoutWaitEffects", {
  defaultValue: () => ({
    resume: (input) => Effect.fail(new CheckoutAdmissionError({ threadId: input.threadId })),
  }),
}) {}

export class CheckoutAdmission extends Context.Reference<{
  readonly admit: (
    input: RunAdmissionInput,
  ) => Effect.Effect<OwnershipHandle | null, CheckoutAdmissionError>;
  readonly withCurrent: <A, E, R>(
    handle: OwnershipHandle | null,
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | CheckoutAdmissionError, R>;
  readonly withVerificationAdmission: <A, E, R>(
    input: { readonly threadId: ThreadId; readonly identity: CheckoutIdentity },
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | CheckoutAdmissionError, R>;
  readonly providerDrained: (input: {
    readonly threadId: ThreadId;
    readonly runId: RunId;
    readonly observedStopped: boolean;
  }) => Effect.Effect<void>;
  readonly finalized: (input: {
    readonly threadId: ThreadId;
    readonly runId: RunId;
    readonly checkpointRef: string | null;
  }) => Effect.Effect<void>;
  readonly terminalOpen: (input: {
    readonly threadId: string;
    readonly terminalId: string;
    readonly cwd: string;
  }) => Effect.Effect<void, CheckoutAdmissionError>;
  readonly terminalWrite: (input: {
    readonly threadId: string;
    readonly terminalId: string;
    readonly cwd: string;
  }) => Effect.Effect<void, CheckoutAdmissionError>;
  readonly terminalClosed: (input: {
    readonly threadId: string;
    readonly terminalId: string;
    readonly confirmed: boolean;
  }) => Effect.Effect<void>;
  readonly terminalStarted: (input: {
    readonly threadId: string;
    readonly terminalId: string;
    readonly pid: number;
    readonly cwd: string;
  }) => Effect.Effect<void>;
}>("t3/workspace/CheckoutAdmission", {
  defaultValue: () => ({
    admit: () => Effect.succeed(null),
    withCurrent: (_handle, effect) => effect,
    withVerificationAdmission: (_input, effect) => effect,
    providerDrained: () => Effect.void,
    finalized: () => Effect.void,
    terminalOpen: () => Effect.void,
    terminalWrite: () => Effect.void,
    terminalClosed: () => Effect.void,
    terminalStarted: () => Effect.void,
  }),
}) {}
