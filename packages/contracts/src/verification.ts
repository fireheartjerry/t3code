import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  CommandId,
  EnvironmentId,
  IsoDateTime,
  NonNegativeInt,
  ProjectId,
  RunId,
  ThreadId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";
import { CheckoutIdentity, OwnershipHandle } from "./checkoutOwnership.ts";

export const VERIFICATION_DEFAULT_TIMEOUT_MS = 900_000;
export const VERIFICATION_MAX_FILES = 100_000;
export const VERIFICATION_MAX_BYTES = 2_147_483_648;
export const VERIFICATION_MAX_OUTPUT_BYTES = 8 * 1024 * 1024;

export const VerificationInputPattern = TrimmedNonEmptyString.check(
  Schema.makeFilter(
    (value: string) =>
      !value.includes("\0") &&
      !/^(?:[\\/]|[A-Za-z]:)/.test(value) &&
      !value.replaceAll("\\", "/").split("/").includes(".."),
    { expected: "a relative input pattern without parent traversal" },
  ),
);
export const VerificationTimeout = Schema.Int.check(
  Schema.isBetween({ minimum: 1_000, maximum: 86_400_000 }),
);

export const VerificationConfig = Schema.Struct({
  actionId: TrimmedNonEmptyString,
  platformActions: Schema.optionalKey(
    Schema.Struct({
      darwin: Schema.optionalKey(TrimmedNonEmptyString),
      linux: Schema.optionalKey(TrimmedNonEmptyString),
      windows: Schema.optionalKey(TrimmedNonEmptyString),
    }),
  ),
  automatic: Schema.Boolean.pipe(Schema.withDecodingDefault(Effect.succeed(false))),
  timeoutMs: VerificationTimeout.pipe(
    Schema.withDecodingDefault(Effect.succeed(VERIFICATION_DEFAULT_TIMEOUT_MS)),
  ),
  inputs: Schema.Struct({
    include: Schema.Array(VerificationInputPattern),
    exclude: Schema.Array(VerificationInputPattern),
    nonGit: Schema.Literals(["fingerprint", "unsupported"]),
  }),
});
export type VerificationConfig = typeof VerificationConfig.Type;

export const VerificationExecution = Schema.Literals([
  "queued",
  "running",
  "passed",
  "failed",
  "cancelled",
  "timed out",
  "interrupted",
]);
export type VerificationExecution = typeof VerificationExecution.Type;
export const VerificationCurrentness = Schema.Literals([
  "Current",
  "Stale",
  "Unknown",
  "Historical only",
]);
export type VerificationCurrentness = typeof VerificationCurrentness.Type;

export const VerificationCoverage = Schema.Struct({
  complete: Schema.Boolean,
  policyRevision: TrimmedNonEmptyString,
  gaps: Schema.Array(TrimmedNonEmptyString),
});
export type VerificationCoverage = typeof VerificationCoverage.Type;

export const SourceIdentity = Schema.Struct({
  checkoutIdentity: CheckoutIdentity,
  checkpointRef: Schema.NullOr(TrimmedNonEmptyString),
  head: Schema.NullOr(TrimmedNonEmptyString),
  branch: Schema.NullOr(TrimmedNonEmptyString),
  indexDigest: Schema.NullOr(TrimmedNonEmptyString),
  digest: TrimmedNonEmptyString,
  coverage: VerificationCoverage,
  toolchainRevision: TrimmedNonEmptyString,
  configRevision: TrimmedNonEmptyString,
});
export type SourceIdentity = typeof SourceIdentity.Type;

export const VerificationActionSnapshot = Schema.Struct({
  actionId: TrimmedNonEmptyString,
  revision: TrimmedNonEmptyString,
  command: TrimmedNonEmptyString,
  shell: TrimmedNonEmptyString,
  cwd: TrimmedNonEmptyString,
  timeoutMs: VerificationTimeout,
});
export type VerificationActionSnapshot = typeof VerificationActionSnapshot.Type;

export const VerificationOutputReference = Schema.Struct({
  id: TrimmedNonEmptyString,
  kind: Schema.Literals(["stdout", "stderr", "diagnostic"]),
  bytes: NonNegativeInt,
});
export type VerificationOutputReference = typeof VerificationOutputReference.Type;

export const VerificationRecord = Schema.Struct({
  verificationId: TrimmedNonEmptyString,
  commandId: CommandId,
  projectId: ProjectId,
  threadId: ThreadId,
  sourceRunId: Schema.NullOr(RunId),
  environmentId: EnvironmentId,
  ownerHandle: Schema.NullOr(OwnershipHandle),
  actionSnapshot: VerificationActionSnapshot,
  execution: VerificationExecution,
  currentness: VerificationCurrentness,
  requestedAt: IsoDateTime,
  startedAt: Schema.NullOr(IsoDateTime),
  finishedAt: Schema.NullOr(IsoDateTime),
  exitCode: Schema.NullOr(Schema.Int),
  signal: Schema.NullOr(TrimmedNonEmptyString),
  terminationReason: Schema.NullOr(TrimmedNonEmptyString),
  startSource: Schema.NullOr(SourceIdentity),
  endSource: Schema.NullOr(SourceIdentity),
  startMutationGeneration: Schema.NullOr(NonNegativeInt),
  endMutationGeneration: Schema.NullOr(NonNegativeInt),
  observationState: Schema.Literals(["continuous", "gap", "unavailable"]),
  outputReferences: Schema.Array(VerificationOutputReference),
  truncated: Schema.Boolean,
});
export type VerificationRecord = typeof VerificationRecord.Type;

export function verificationMayClaimCurrentPass(
  record: Pick<
    VerificationRecord,
    "execution" | "currentness" | "startSource" | "endSource" | "observationState"
  >,
): boolean {
  return (
    record.execution === "passed" &&
    record.currentness === "Current" &&
    record.observationState === "continuous" &&
    record.startSource?.coverage.complete === true &&
    record.endSource?.coverage.complete === true
  );
}
