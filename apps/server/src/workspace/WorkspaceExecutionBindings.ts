import { CommandId, ThreadId, TurnItemId } from "@t3tools/contracts";
import * as Ids from "@t3tools/provider-core/server/IdAllocator";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as Events from "../orchestration-v2/EventSink.ts";
import * as Launch from "../orchestration-v2/ThreadLaunchService.ts";
import * as Verification from "../verification/VerificationService.ts";
import * as VerificationEffects from "../verification/VerificationEffects.ts";
import { formatVerificationLegacySummary } from "../verification/VerificationLegacySummary.ts";
import * as Admission from "./CheckoutAdmission.ts";
import * as Recovery from "./CheckoutOwnershipRecovery.ts";
import * as Wait from "./CheckoutWaitService.ts";
import * as Api from "./WorkspaceExecutionApi.ts";
import { workspaceExecutionError } from "./WorkspaceExecutionRpc.ts";

const make = Effect.gen(function* () {
  const verification = yield* Verification.VerificationService;
  const waits = yield* Wait.CheckoutWaitService;
  const recovery = yield* Recovery.CheckoutOwnershipRecovery;
  const launch = yield* Launch.ThreadLaunchService;
  const events = yield* Events.EventSinkV2;
  const ids = yield* Ids.IdAllocatorV2;

  const observer: Context.Service.Shape<typeof VerificationEffects.VerificationResultObserver> = {
    publish: Effect.fn("WorkspaceExecutionBindings.publish")(
      function* (record) {
        if (record.execution === "queued" || record.execution === "running") return;
        const reference =
          record.outputReferences.find((entry) => entry.kind === "stderr") ??
          record.outputReferences.find((entry) => entry.kind === "diagnostic");
        const output =
          reference === undefined
            ? null
            : yield* verification
                .output({
                  verificationId: record.verificationId,
                  referenceId: reference.id,
                  offset: 0,
                  limit: 4_000,
                })
                .pipe(Effect.orElseSucceed(() => null));
        const commandId = CommandId.make(
          `verification-result:${record.verificationId}:${record.execution}:${record.currentness}`,
        );
        const now = yield* DateTime.now;
        yield* events.commitCommand({
          commandId,
          threadId: record.threadId,
          commandType: "verification.result-notice",
          acceptedAt: now,
          effects: [],
          events: [
            {
              id: yield* ids.allocate.event({ threadId: record.threadId, commandId }),
              threadId: record.threadId,
              ...(record.sourceRunId === null ? {} : { runId: record.sourceRunId }),
              occurredAt: now,
              type: "turn-item.updated",
              payload: {
                id: TurnItemId.make(`${commandId}:notice`),
                threadId: record.threadId,
                runId: record.sourceRunId,
                nodeId: null,
                providerThreadId: null,
                providerTurnId: null,
                nativeItemRef: null,
                parentItemId: null,
                ordinal: 0,
                type: "system_notice",
                status: "completed",
                title: "Verification",
                message: formatVerificationLegacySummary(record, output?.text ?? ""),
                startedAt: now,
                completedAt: now,
                updatedAt: now,
              },
            },
          ],
        });
      },
      (effect, record) =>
        effect.pipe(
          Effect.catchCause(() =>
            Effect.logWarning("Could not persist verification result notice", {
              verificationId: record.verificationId.slice(0, 128),
              threadId: record.threadId.slice(0, 128),
            }),
          ),
        ),
    ),
  };
  // No capability or action reference is exported until both recovery boundaries have completed.
  yield* recovery.reconcile().pipe(Effect.mapError(workspaceExecutionError));
  yield* verification
    .recover()
    .pipe(
      Effect.provideService(VerificationEffects.VerificationResultObserver, observer),
      Effect.mapError(workspaceExecutionError),
    );

  // Provide the completed late context on each operation too: callers may own early-created services.
  const bound = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    effect.pipe(Effect.provide(bindings));
  const admissionError = (threadId: ThreadId, cause: unknown) =>
    new Admission.CheckoutAdmissionError({ threadId, cause: workspaceExecutionError(cause) });
  const api: Context.Service.Shape<typeof Api.WorkspaceExecutionApi> = {
    inspect: (threadId) =>
      bound(
        waits.inspect(threadId).pipe(
          Effect.map((result) => (result.owner?.state === "released" ? null : result.owner)),
          Effect.mapError(workspaceExecutionError),
        ),
      ),
    wait: (input) => bound(waits.wait(input).pipe(Effect.mapError(workspaceExecutionError))),
    cancelWait: (input) =>
      bound(waits.cancelWait(input).pipe(Effect.mapError(workspaceExecutionError))),
    request: (input) =>
      bound(verification.request(input).pipe(Effect.mapError(workspaceExecutionError))),
    cancel: (input) =>
      bound(verification.cancel(input).pipe(Effect.mapError(workspaceExecutionError))),
    get: (input) => bound(verification.get(input).pipe(Effect.mapError(workspaceExecutionError))),
    latest: (input) =>
      bound(verification.latest(input).pipe(Effect.mapError(workspaceExecutionError))),
    output: (input) =>
      bound(verification.output(input).pipe(Effect.mapError(workspaceExecutionError))),
  };
  const bindings: Context.Context<never> = Context.make(Api.WorkspaceExecutionApi, api).pipe(
    Context.add(Api.WorkspaceExecutionAvailable, true),
    Context.add(VerificationEffects.VerificationResultObserver, observer),
    Context.add(VerificationEffects.VerificationEffects, {
      execute: (input) =>
        bound(verification.executeQueued(input).pipe(Effect.mapError(workspaceExecutionError))),
    }),
    Context.add(Admission.CheckoutWaitEffects, {
      resume: (input) =>
        bound(
          waits
            .resumeWait(input)
            .pipe(Effect.mapError((cause) => admissionError(input.threadId, cause))),
        ),
    }),
    Context.add(Wait.CheckoutPreparedRunRetry, {
      retry: (input) =>
        bound(
          launch.retryPreparation(input).pipe(
            Effect.asVoid,
            Effect.mapError((cause) => admissionError(input.threadId, cause)),
          ),
        ),
    }),
    Context.add(Admission.CheckoutVerificationHandoff, {
      request: (input) =>
        bound(
          verification.requestAfterFinalization(input).pipe(
            Effect.map((record) => record !== null),
            Effect.mapError((cause) => admissionError(input.threadId, cause)),
          ),
        ),
      invalidate: (checkoutKey) =>
        bound(
          verification
            .invalidateCheckout(checkoutKey)
            .pipe(
              Effect.mapError((cause) =>
                admissionError(ThreadId.make("verification-invalidation"), cause),
              ),
            ),
        ),
    }),
  );
  return bindings;
});

export const layer = Layer.effectContext(make);
