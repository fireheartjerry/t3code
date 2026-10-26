import { CommandId, type ThreadId, type VerificationRecord } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import * as Store from "./VerificationStore.ts";
import type { ObservationLease } from "./VerificationSourceObserver.ts";
import {
  classifyVerificationCurrentness,
  type VerificationCurrentnessObservation,
} from "./verificationCurrentness.ts";

export interface VerificationInvalidationSignal {
  readonly verificationId?: string;
  readonly checkoutKey?: string;
  readonly threadId?: ThreadId;
  readonly checkpointRef?: string;
  readonly reason:
    | "source"
    | "tracked-edit"
    | "untracked-edit"
    | "index"
    | "head"
    | "submodule"
    | "restore"
    | "action-config"
    | "toolchain"
    | "ownership-conflict"
    | "server-gap"
    | "newer-run"
    | "output-only";
}

export class VerificationInvalidationService extends Context.Service<
  VerificationInvalidationService,
  {
    readonly retain: (input: {
      readonly verificationId: string;
      readonly lease: ObservationLease;
    }) => Effect.Effect<void>;
    readonly revalidate: (
      verificationId: string,
      observation?: VerificationCurrentnessObservation,
    ) => Effect.Effect<VerificationRecord | null, Store.VerificationStoreError>;
    readonly observeInvalidation: (
      signal: VerificationInvalidationSignal,
    ) => Effect.Effect<void, Store.VerificationStoreError>;
  }
>()("t3/verification/VerificationInvalidationService") {}

const make = Effect.gen(function* () {
  const store = yield* Store.VerificationStore;
  const scope = yield* Scope.Scope;
  const leases = new Map<string, ObservationLease>();
  yield* Effect.addFinalizer(() =>
    Effect.forEach(leases.values(), (lease) => lease.close, { discard: true }),
  );

  const revalidate: VerificationInvalidationService["Service"]["revalidate"] = Effect.fn(
    "VerificationInvalidationService.revalidate",
  )(function* (verificationId, observation) {
    const record = yield* store.get(verificationId);
    if (record === null || record.execution === "queued") return record;
    const lease = leases.get(verificationId);
    const sample = lease ? yield* lease.snapshot : null;
    const latest = yield* store.latestForThread(record.threadId);
    const source = record.endSource ?? record.startSource;
    const current = observation ?? {
      source,
      mutationGeneration: sample?.mutationGeneration ?? null,
      observationState: sample?.observationState ?? "gap",
      newerSourceRun: false,
      newerVerification: latest !== null && latest.verificationId !== record.verificationId,
      configRevision: source?.configRevision ?? null,
      toolchainRevision: source?.toolchainRevision ?? null,
      ownershipConflict: false,
    };
    const currentness = classifyVerificationCurrentness(record, current);
    if (
      currentness === "Current" ||
      (record.execution === "running" &&
        currentness === "Unknown" &&
        current.observationState === "continuous")
    )
      return record;
    return yield* store.invalidate({
      verificationId,
      currentness,
      observationGap: current.observationState !== "continuous",
      commandId: CommandId.make(
        `${verificationId}:observe:${currentness}:${current.mutationGeneration ?? "gap"}`,
      ),
    });
  });
  const retain: VerificationInvalidationService["Service"]["retain"] = Effect.fn(
    "VerificationInvalidationService.retain",
  )(function* (input) {
    const old = leases.get(input.verificationId);
    if (old === input.lease) return;
    if (old) yield* old.close;
    leases.set(input.verificationId, input.lease);
    yield* Stream.runForEach(input.lease.changes, () => revalidate(input.verificationId)).pipe(
      Effect.ignore,
      Effect.forkIn(scope),
    );
  });
  const observeInvalidation: VerificationInvalidationService["Service"]["observeInvalidation"] =
    Effect.fn("VerificationInvalidationService.observeInvalidation")(function* (signal) {
      if (signal.reason === "output-only") return;
      // Unscoped signals must not invalidate unrelated workspaces.
      const records =
        signal.verificationId !== undefined
          ? [yield* store.get(signal.verificationId)].filter(
              (record): record is VerificationRecord => record !== null,
            )
          : signal.checkoutKey !== undefined
            ? yield* store.listForCheckout(signal.checkoutKey)
            : signal.threadId !== undefined
              ? yield* store.listForThread(signal.threadId)
              : [];
      for (const record of records) {
        if (signal.threadId !== undefined && record.threadId !== signal.threadId) continue;
        if (
          signal.checkoutKey !== undefined &&
          record.ownerHandle?.checkoutKey !== signal.checkoutKey
        )
          continue;
        if (
          signal.checkpointRef !== undefined &&
          record.startSource?.checkpointRef !== signal.checkpointRef
        )
          continue;
        yield* store.invalidate({
          verificationId: record.verificationId,
          commandId: CommandId.make(`${record.verificationId}:invalidate:${signal.reason}`),
          currentness:
            signal.reason === "newer-run"
              ? "Historical only"
              : signal.reason === "server-gap"
                ? "Unknown"
                : "Stale",
          observationGap: signal.reason === "server-gap",
        });
      }
    });
  return VerificationInvalidationService.of({ retain, revalidate, observeInvalidation });
});

export const layer = Layer.effect(VerificationInvalidationService, make);
