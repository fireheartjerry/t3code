import type {
  SourceIdentity,
  VerificationCurrentness,
  VerificationRecord,
} from "@t3tools/contracts";

/** Fresh authoritative state, not a client's cached evidence or a fingerprint alone. */
export interface VerificationCurrentnessObservation {
  readonly source: SourceIdentity | null;
  readonly mutationGeneration: number | null;
  readonly observationState: VerificationRecord["observationState"];
  readonly newerSourceRun: boolean;
  readonly newerVerification?: boolean;
  readonly configRevision: string | null;
  readonly toolchainRevision: string | null;
  readonly ownershipConflict: boolean;
}

export function classifyVerificationCurrentness(
  record: VerificationRecord,
  observation: VerificationCurrentnessObservation,
): VerificationCurrentness {
  if (
    record.currentness === "Historical only" ||
    observation.newerSourceRun ||
    observation.newerVerification
  )
    return "Historical only";
  if (record.currentness === "Stale") return "Stale";

  const start = record.startSource;
  const end = record.endSource;
  const source = observation.source;
  if (
    observation.ownershipConflict ||
    (start !== null && observation.configRevision !== start.configRevision) ||
    (start !== null &&
      observation.toolchainRevision !== null &&
      observation.toolchainRevision !== start.toolchainRevision) ||
    (start !== null && record.environmentId !== start.checkoutIdentity.environmentId) ||
    (start !== null && end !== null && !sameSourceMetadata(start, end)) ||
    (start !== null && source !== null && !sameSourceMetadata(start, source))
  )
    return "Stale";

  const generations = [
    record.startMutationGeneration,
    record.endMutationGeneration,
    observation.mutationGeneration,
  ];
  const validGenerations = generations.every(
    (generation) => generation !== null && Number.isSafeInteger(generation) && generation >= 0,
  );
  const knownGenerations = generations.filter(
    (generation) => generation !== null && Number.isSafeInteger(generation) && generation >= 0,
  );
  if (knownGenerations.some((generation) => generation !== knownGenerations[0])) return "Stale";

  if (
    start !== null &&
    [end, source].some(
      (identity) =>
        identity !== null &&
        start.coverage.complete &&
        start.coverage.gaps.length === 0 &&
        identity.coverage.complete &&
        identity.coverage.gaps.length === 0 &&
        start.digest !== identity.digest,
    )
  )
    return "Stale";

  // A gap persisted with the attempt cannot be repaired by a fresh, equal fingerprint.
  // Client-only Unknown may recover when the server's retained observation is continuous.
  if (
    record.observationState !== "continuous" ||
    observation.observationState !== "continuous" ||
    !validGenerations ||
    observation.toolchainRevision === null ||
    start === null ||
    end === null ||
    source === null ||
    ![start, end, source].every(
      (identity) => identity.coverage.complete && identity.coverage.gaps.length === 0,
    )
  )
    return "Unknown";

  if (
    record.execution === "queued" ||
    record.execution === "running" ||
    record.execution === "interrupted" ||
    (record.execution === "passed" &&
      (record.exitCode !== 0 || record.signal !== null || record.terminationReason !== null))
  )
    return "Unknown";
  return "Current";
}

function sameSourceMetadata(left: SourceIdentity, right: SourceIdentity): boolean {
  const a = left.checkoutIdentity;
  const b = right.checkoutIdentity;
  return (
    a.environmentId === b.environmentId &&
    a.checkoutKey === b.checkoutKey &&
    a.canonicalCwd === b.canonicalCwd &&
    a.filesystemIdentity.device === b.filesystemIdentity.device &&
    a.filesystemIdentity.file === b.filesystemIdentity.file &&
    a.gitDir === b.gitDir &&
    a.gitCommonDir === b.gitCommonDir &&
    left.head === right.head &&
    left.branch === right.branch &&
    left.checkpointRef === right.checkpointRef &&
    left.indexDigest === right.indexDigest &&
    left.coverage.policyRevision === right.coverage.policyRevision &&
    left.configRevision === right.configRevision &&
    left.toolchainRevision === right.toolchainRevision
  );
}
