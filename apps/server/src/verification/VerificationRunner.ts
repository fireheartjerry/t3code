import {
  VERIFICATION_MAX_OUTPUT_BYTES,
  type CheckoutIdentity,
  type OwnershipHandle,
  SourceIdentity,
  type VerificationActionSnapshot,
  type VerificationConfig,
  type VerificationRecord,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import * as ProcessRunner from "../processRunner.ts";
import * as Ownership from "../workspace/CheckoutOwnershipService.ts";
import * as Accounting from "../workspace/CheckoutProcessAccounting.ts";
import * as Fingerprint from "./VerificationFingerprint.ts";
import * as Observer from "./VerificationSourceObserver.ts";
import {
  sanitizeVerificationOutput,
  verificationCommandContainsSecrets,
} from "./VerificationOutput.ts";

// Lower-level causes can contain raw process output or credentials. Only normalized reasons escape.
export class VerificationRunnerError extends Schema.TaggedError<VerificationRunnerError>()(
  "VerificationRunnerError",
  {
    reason: Schema.Literals([
      "observer-failed",
      "fingerprint-failed",
      "ownership-lost",
      "identity-mismatch",
      "action-mismatch",
      "unsupported-shell",
      "secret-bearing-command",
      "invalid-output-limit",
    ]),
  },
) {
  override get message(): string {
    return `Cannot run verification (${this.reason}).`;
  }
}

export interface PreparedVerification {
  readonly lease: Observer.ObservationLease;
  readonly startSource: SourceIdentity;
  readonly startObservation: Observer.ObservationSnapshot;
  readonly identity: CheckoutIdentity;
  readonly config: VerificationConfig;
  readonly configRevision: string;
  readonly toolchainRevision: string;
  readonly checkpointRef?: string | null;
}

export interface VerificationRunnerResult {
  readonly completion: Pick<
    VerificationRecord,
    | "execution"
    | "currentness"
    | "finishedAt"
    | "exitCode"
    | "signal"
    | "terminationReason"
    | "endSource"
    | "endMutationGeneration"
    | "observationState"
    | "truncated"
  >;
  /** Null when no actual spawn was observed, including a missing shell executable. */
  readonly startedAt: VerificationRecord["startedAt"];
  readonly startSource: SourceIdentity;
  readonly startMutationGeneration: number;
  readonly stdout: string;
  readonly stderr: string;
  readonly diagnostic: string;
}

export const VerificationRunnerOutputLimit = Context.Reference<number>(
  "t3/verification/VerificationRunnerOutputLimit",
  {
    defaultValue: () => VERIFICATION_MAX_OUTPUT_BYTES,
  },
);

export class VerificationRunner extends Context.Service<
  VerificationRunner,
  {
    readonly prepare: (input: {
      readonly identity: CheckoutIdentity;
      readonly config: VerificationConfig;
      readonly configRevision: string;
      readonly toolchainRevision: string;
      readonly checkpointRef?: string | null;
    }) => Effect.Effect<PreparedVerification, VerificationRunnerError>;
    readonly run: (input: {
      readonly verificationId: string;
      readonly executionClaimId: string;
      readonly handle: OwnershipHandle;
      readonly actionSnapshot: VerificationActionSnapshot;
      readonly prepared: PreparedVerification;
      readonly secrets: readonly string[];
    }) => Effect.Effect<VerificationRunnerResult, VerificationRunnerError>;
  }
>()("t3/verification/VerificationRunner") {}

const sameSource = Schema.toEquivalence(SourceIdentity);

const make = Effect.gen(function* () {
  const process = yield* ProcessRunner.ProcessRunner;
  const ownership = yield* Ownership.CheckoutOwnershipService;
  const accounting = yield* Accounting.CheckoutProcessAccounting;
  const fingerprint = yield* Fingerprint.VerificationFingerprint;
  const observer = yield* Observer.VerificationSourceObserver;
  const platform = yield* HostProcess.Platform;
  const outputLimit = yield* VerificationRunnerOutputLimit;

  const prepare: VerificationRunner["Service"]["prepare"] = Effect.fn("VerificationRunner.prepare")(
    (input) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          // Install before the first fingerprint, and sample before capture so capture-time edits count.
          const lease = yield* observer
            .start(input)
            .pipe(
              Effect.mapError(() => new VerificationRunnerError({ reason: "observer-failed" })),
            );
          return yield* restore(
            Effect.gen(function* () {
              const startObservation = yield* lease.snapshot;
              const startSource = yield* fingerprint
                .capture(input)
                .pipe(
                  Effect.mapError(
                    () => new VerificationRunnerError({ reason: "fingerprint-failed" }),
                  ),
                );
              return { ...input, lease, startSource, startObservation };
            }),
          ).pipe(Effect.onExit((exit) => (Exit.isFailure(exit) ? lease.close : Effect.void)));
        }),
      ),
  );

  const run: VerificationRunner["Service"]["run"] = Effect.fn("VerificationRunner.run")(
    function* (input) {
      const { prepared, actionSnapshot: action, handle } = input;
      if (!Number.isSafeInteger(outputLimit) || outputLimit < 1)
        return yield* new VerificationRunnerError({ reason: "invalid-output-limit" });
      const maxBytes = Math.min(outputLimit, VERIFICATION_MAX_OUTPUT_BYTES);
      if (verificationCommandContainsSecrets(action.command, input.secrets))
        return yield* new VerificationRunnerError({ reason: "secret-bearing-command" });
      if (
        handle.checkoutKey !== prepared.identity.checkoutKey ||
        handle.owner.kind !== "verification" ||
        handle.owner.verificationId !== input.verificationId ||
        handle.operation !== "verification" ||
        input.executionClaimId.length === 0
      ) {
        return yield* new VerificationRunnerError({ reason: "identity-mismatch" });
      }
      const actionId =
        (platform === "win32"
          ? prepared.config.platformActions?.windows
          : platform === "darwin"
            ? prepared.config.platformActions?.darwin
            : prepared.config.platformActions?.linux) ?? prepared.config.actionId;
      if (action.actionId !== actionId || action.timeoutMs !== prepared.config.timeoutMs)
        return yield* new VerificationRunnerError({ reason: "action-mismatch" });
      const basename = action.shell.replace(/^.*[\\/]/, "");
      const shell = platform === "win32" ? basename.toLowerCase().replace(/\.exe$/, "") : basename;
      const args = ["sh", "zsh", "bash"].includes(shell)
        ? ["-lc", action.command]
        : platform === "win32" && shell === "cmd"
          ? ["/d", "/s", "/c", action.command]
          : ["powershell", "pwsh"].includes(shell) && (platform === "win32" || shell === "pwsh")
            ? ["-NoLogo", "-NoProfile", "-Command", action.command]
            : null;
      if (args === null) return yield* new VerificationRunnerError({ reason: "unsupported-shell" });

      // withCurrent owns the checkout-key lock and fence through spawn, child cleanup and final capture.
      return yield* ownership
        .withCurrent(
          handle,
          Effect.gen(function* () {
            let startedAt: VerificationRecord["startedAt"] = null;
            let processRef: Accounting.OwnedProcessIdentity | undefined;
            let registered = false;
            let accountingFailed = false;
            const onSpawn = Effect.fn("VerificationRunner.onSpawn")(function* (pid: number) {
              if (!Number.isSafeInteger(pid) || pid < 1 || processRef !== undefined) {
                accountingFailed = true;
                return yield* new ProcessRunner.ProcessSpawnError({
                  command: "verification",
                  argumentCount: 0,
                  cause: "invalid-spawn-identity",
                });
              }
              startedAt = DateTime.formatIso(yield* DateTime.now);
              processRef = {
                id: `verification:${input.verificationId}:${input.executionClaimId}`,
                creationMarker: input.executionClaimId,
                pid,
              };
              yield* accounting.register({ handle, processRef, kind: "verification" }).pipe(
                Effect.mapError(() => {
                  accountingFailed = true;
                  return new ProcessRunner.ProcessSpawnError({
                    command: "verification",
                    argumentCount: 0,
                    cause: "process-accounting-failed",
                  });
                }),
              );
              registered = true;
              yield* accounting.mark({ handle, processRef, state: "live" }).pipe(
                Effect.mapError(() => {
                  accountingFailed = true;
                  return new ProcessRunner.ProcessSpawnError({
                    command: "verification",
                    argumentCount: 0,
                    cause: "process-accounting-failed",
                  });
                }),
              );
            }, Effect.uninterruptible);

            const result = yield* process
              .run({
                command: action.shell,
                args,
                cwd: action.cwd,
                timeout: action.timeoutMs,
                maxOutputBytes: Math.max(1, Math.floor(maxBytes / 2)),
                outputMode: "truncate",
                truncatedMarker: "",
                timeoutBehavior: "timedOutResult",
                onSpawn,
              })
              .pipe(
                // ProcessRunner's scoped cleanup completes before this finalizer, including on interruption.
                Effect.onExit(() =>
                  registered && processRef !== undefined
                    ? accounting.mark({ handle, processRef, state: "unknown" }).pipe(
                        Effect.catch(() =>
                          Effect.sync(() => {
                            accountingFailed = true;
                          }),
                        ),
                      )
                    : Effect.void,
                ),
                Effect.result,
              );
            // Interruption is intentionally not caught by Effect.result. The caller persists cancellation.
            const { lease, startSource, startObservation, ...captureInput } = prepared;
            const endSource = yield* fingerprint
              .capture(captureInput)
              .pipe(Effect.orElseSucceed(() => null));
            const endObservation = yield* lease.snapshot;
            const observationState = worstObservation(
              startObservation.observationState,
              endObservation.observationState,
            );
            const finishedAt = DateTime.formatIso(yield* DateTime.now);
            let execution: VerificationRecord["execution"];
            let terminationReason: string;
            let output: ProcessRunner.ProcessRunOutput | undefined;
            let exitCode: number | null = null;
            if (result._tag === "Failure") {
              const error = result.failure;
              execution = "failed";
              terminationReason =
                error._tag === "ProcessSpawnError"
                  ? isMissingExecutable(error.cause)
                    ? "missing-executable"
                    : "spawn-failed"
                  : error._tag === "ProcessTimeoutError"
                    ? "timeout"
                    : error._tag === "ProcessReadError"
                      ? "process-read-failed"
                      : "process-failed";
              if (error._tag === "ProcessTimeoutError") execution = "timed out";
              if (error._tag === "ProcessReadError" && error.stream === "exitCode") {
                execution = "interrupted";
                terminationReason = "exit-status-unavailable";
              }
            } else {
              output = result.success;
              exitCode = output.code === null ? null : Number(output.code);
              execution = output.timedOut
                ? "timed out"
                : exitCode === null
                  ? "interrupted"
                  : exitCode === 0
                    ? "passed"
                    : "failed";
              terminationReason = output.timedOut
                ? "timeout"
                : exitCode === null
                  ? "exit-status-unavailable"
                  : exitCode === 0
                    ? "exited-descendants-unconfirmed"
                    : "nonzero-exit";
              if (startedAt === null) {
                execution = "interrupted";
                terminationReason = "missing-spawn-evidence";
              }
            }
            if (accountingFailed) {
              execution = "interrupted";
              terminationReason = "process-accounting-failed";
            }
            if (
              execution === "passed" &&
              (endSource === null || observationState !== "continuous")
            ) {
              execution = "failed";
              terminationReason = "observation-failed";
            }
            const sourceChanged =
              startObservation.mutationGeneration !== endObservation.mutationGeneration ||
              (endSource !== null && !sameSource(startSource, endSource));
            const incompleteCoverage =
              !startSource.coverage.complete ||
              startSource.coverage.gaps.length > 0 ||
              endSource === null ||
              !endSource.coverage.complete ||
              endSource.coverage.gaps.length > 0;
            const sanitized = sanitizeVerificationOutput({
              stdout: output?.stdout ?? "",
              stderr: output?.stderr ?? "",
              diagnostic: [
                `Verification ${terminationReason}.`,
                ...(sourceChanged ? ["Covered source changed during verification."] : []),
                ...(incompleteCoverage ? ["Source coverage is incomplete."] : []),
                "Process descendant termination is not established.",
              ].join(" "),
              stdoutTruncated: output?.stdoutTruncated ?? false,
              stderrTruncated: output?.stderrTruncated ?? false,
              secrets: input.secrets,
              maxBytes,
            });
            return {
              completion: {
                execution,
                // Root exit and pipe closure do not establish descendant quiescence, even with code zero.
                currentness: "Unknown" as const,
                finishedAt,
                exitCode,
                signal: null,
                terminationReason,
                endSource,
                endMutationGeneration: endObservation.mutationGeneration,
                observationState,
                truncated: sanitized.truncated,
              },
              startedAt,
              startSource: prepared.startSource,
              startMutationGeneration: prepared.startObservation.mutationGeneration,
              stdout: sanitized.stdout,
              stderr: sanitized.stderr,
              diagnostic: sanitized.diagnostic,
            } satisfies VerificationRunnerResult;
          }),
        )
        .pipe(Effect.mapError(() => new VerificationRunnerError({ reason: "ownership-lost" })));
    },
  );

  return VerificationRunner.of({ prepare, run });
});

function worstObservation(
  left: Observer.ObservationSnapshot["observationState"],
  right: Observer.ObservationSnapshot["observationState"],
): Observer.ObservationSnapshot["observationState"] {
  return left === "unavailable" || right === "unavailable"
    ? "unavailable"
    : left === "gap" || right === "gap"
      ? "gap"
      : "continuous";
}

function isMissingExecutable(cause: unknown): boolean {
  if (typeof cause !== "object" || cause === null) return false;
  if ("code" in cause && cause.code === "ENOENT") return true;
  return (
    "reason" in cause &&
    typeof cause.reason === "object" &&
    cause.reason !== null &&
    "_tag" in cause.reason &&
    cause.reason._tag === "NotFound"
  );
}

export const layer = Layer.effect(VerificationRunner, make);
