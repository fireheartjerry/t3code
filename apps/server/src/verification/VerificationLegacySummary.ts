import { verificationMayClaimCurrentPass, type VerificationRecord } from "@t3tools/contracts";

export function formatVerificationLegacySummary(
  record: VerificationRecord,
  failedCheckText = "",
): string {
  const source = record.endSource ?? record.startSource;
  const coverage = source?.coverage;
  const claim = verificationMayClaimCurrentPass(record)
    ? "Checks passed for current changes"
    : `Verification: ${record.execution}; currentness: ${record.currentness}`;
  const lines = [
    claim,
    `Environment: ${record.environmentId}`,
    `Checkout: ${source?.checkoutIdentity.canonicalCwd ?? record.actionSnapshot.cwd}`,
    `Branch: ${source?.branch ?? "unavailable"}; HEAD: ${source?.head ?? "unavailable"}`,
    `Command: ${record.actionSnapshot.command}`,
    record.startedAt === null
      ? "The configured command was not observed executing."
      : `Started: ${record.startedAt}; finished: ${record.finishedAt ?? "pending"}`,
    `Covered source: ${source?.digest ?? "unavailable"}`,
    `Coverage: ${coverage?.complete && coverage.gaps.length === 0 ? "complete declared inputs" : "incomplete or unavailable"}`,
  ];
  if (record.terminationReason) lines.push(`Reason: ${record.terminationReason}`);
  if (coverage && coverage.gaps.length > 0)
    lines.push(`Coverage gaps: ${coverage.gaps.slice(0, 3).join("; ")}`);
  if (failedCheckText) lines.push(`Check output: ${failedCheckText.slice(0, 1200)}`);
  if (record.truncated) lines.push("Output was truncated; bounded output references are retained.");
  if (record.outputReferences.length > 0)
    lines.push(`Output: ${record.outputReferences.map((ref) => ref.id).join(", ")}`);
  return lines.join("\n").slice(0, 4000);
}
