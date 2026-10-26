import { WorkspaceExecutionRequestError, type VerificationRecord } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export class VerificationEffects extends Context.Reference<{
  readonly execute: (input: {
    readonly verificationId: string;
    readonly executionClaimId: string;
  }) => Effect.Effect<void, WorkspaceExecutionRequestError>;
}>("t3/verification/VerificationEffects", {
  defaultValue: () => ({
    execute: () => Effect.fail(new WorkspaceExecutionRequestError({ reason: "unavailable" })),
  }),
}) {}

export class VerificationResultObserver extends Context.Reference<{
  readonly publish: (record: VerificationRecord) => Effect.Effect<void>;
}>("t3/verification/VerificationResultObserver", {
  defaultValue: () => ({ publish: () => Effect.void }),
}) {}
