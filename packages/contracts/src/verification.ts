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

const VERIFICATION_DEFAULT_TIMEOUT_MS = 900_000;
export const VERIFICATION_MAX_OUTPUT_BYTES = 8 * 1024 * 1024;

export const VerificationInputPattern = Schema.String.check(Schema.isMinLength(1)).check(
  Schema.makeFilter(
    (value: string) =>
      !value.includes("\0") &&
      !/[{}\[\]]/.test(value) &&
      !/^(?:[\\/]|[A-Za-z]:)/.test(value) &&
      !value
        .replaceAll("\\", "/")
        .split("/")
        .some((part) => part === ".." || part === "." || part === ""),
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
    include: Schema.Array(VerificationInputPattern).check(Schema.isMinLength(1)),
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
  cwd: Schema.String.check(Schema.isMinLength(1)),
  timeoutMs: VerificationTimeout,
});
export type VerificationActionSnapshot = typeof VerificationActionSnapshot.Type;

export const VerificationOutputReference = Schema.Struct({
  id: TrimmedNonEmptyString,
  kind: Schema.Literals(["stdout", "stderr", "diagnostic"]),
  bytes: NonNegativeInt.check(Schema.isLessThanOrEqualTo(VERIFICATION_MAX_OUTPUT_BYTES)),
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
  outputReferences: Schema.Array(VerificationOutputReference).check(Schema.isMaxLength(16)),
  truncated: Schema.Boolean,
}).check(
  Schema.makeFilter(
    (record) =>
      record.execution !== "passed" ||
      record.currentness !== "Current" ||
      verificationMayClaimCurrentPass(record),
    { expected: "complete, unchanged execution evidence for a current pass" },
  ),
);
export type VerificationRecord = typeof VerificationRecord.Type;

export function verificationMayClaimCurrentPass(record: {
  readonly execution: VerificationExecution;
  readonly currentness: VerificationCurrentness;
  readonly startSource: SourceIdentity | null;
  readonly endSource: SourceIdentity | null;
  readonly observationState: "continuous" | "gap" | "unavailable";
  readonly exitCode: number | null;
  readonly signal: string | null;
  readonly startMutationGeneration: number | null;
  readonly endMutationGeneration: number | null;
  readonly startedAt: string | null;
  readonly finishedAt: string | null;
}): boolean {
  const start = record.startSource;
  const end = record.endSource;
  if (start === null || end === null) return false;
  return (
    record.execution === "passed" &&
    record.currentness === "Current" &&
    record.observationState === "continuous" &&
    record.exitCode === 0 &&
    record.signal === null &&
    record.startedAt !== null &&
    record.finishedAt !== null &&
    record.startMutationGeneration !== null &&
    record.startMutationGeneration === record.endMutationGeneration &&
    start.coverage.complete &&
    end.coverage.complete &&
    start.coverage.gaps.length === 0 &&
    end.coverage.gaps.length === 0 &&
    start.coverage.policyRevision === end.coverage.policyRevision &&
    start.checkoutIdentity.environmentId === end.checkoutIdentity.environmentId &&
    start.checkoutIdentity.checkoutKey === end.checkoutIdentity.checkoutKey &&
    start.digest === end.digest &&
    start.head === end.head &&
    start.branch === end.branch &&
    start.indexDigest === end.indexDigest &&
    start.configRevision === end.configRevision &&
    start.toolchainRevision === end.toolchainRevision
  );
}
