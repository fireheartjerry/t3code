import type { OwnershipHandle, OwnershipOwner } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";

import { ownerKey } from "./CheckoutOwnershipStore.ts";

export interface OwnedProcessIdentity {
  readonly id: string;
  readonly creationMarker: string;
  readonly pid?: number;
}
export interface OwnershipProcessObservation {
  readonly state: "live" | "stopped" | "unknown";
  readonly accountingRevision: number;
  readonly processIdentities: ReadonlyArray<OwnedProcessIdentity>;
}

export class CheckoutProcessAccountingError extends Schema.TaggedError<CheckoutProcessAccountingError>()(
  "CheckoutProcessAccountingError",
  {
    reason: Schema.Literals(["identity-reused", "unregistered", "revision-exhausted"]),
  },
) {
  override get message(): string {
    return `Checkout process accounting is uncertain: ${this.reason}.`;
  }
}

export class CheckoutProcessAccounting extends Context.Service<
  CheckoutProcessAccounting,
  {
    readonly register: (input: {
      readonly handle: OwnershipHandle;
      readonly processRef: OwnedProcessIdentity;
      readonly kind: string;
    }) => Effect.Effect<void, CheckoutProcessAccountingError>;
    readonly observe: (owner: OwnershipOwner) => Effect.Effect<OwnershipProcessObservation>;
    readonly mark: (input: {
      readonly owner: OwnershipOwner;
      readonly processRef: OwnedProcessIdentity;
      readonly state: "live" | "stopped" | "unknown";
    }) => Effect.Effect<void, CheckoutProcessAccountingError>;
    readonly whenQuiescent: (owner: OwnershipOwner) => Effect.Effect<OwnershipProcessObservation>;
  }
>()("t3/workspace/CheckoutProcessAccounting") {}

const make = Effect.gen(function* () {
  const changes = yield* PubSub.unbounded<string>();
  const owners = new Map<
    string,
    {
      revision: number;
      uncertain: boolean;
      processes: Map<
        string,
        { identity: OwnedProcessIdentity; state: "live" | "stopped" | "unknown" }
      >;
    }
  >();
  const observe = Effect.fn("CheckoutProcessAccounting.observe")(function* (owner: OwnershipOwner) {
    const entry = owners.get(ownerKey(owner));
    const processes = [...(entry?.processes.values() ?? [])];
    const state =
      entry === undefined ||
      entry.uncertain ||
      processes.length === 0 ||
      processes.some((p) => p.state === "unknown")
        ? ("unknown" as const)
        : processes.some((p) => p.state === "live")
          ? ("live" as const)
          : ("stopped" as const);
    return {
      state,
      accountingRevision: entry?.revision ?? 0,
      processIdentities: processes.map((p) => p.identity),
    };
  });

  const register = Effect.fn("CheckoutProcessAccounting.register")(function* (input: {
    readonly handle: OwnershipHandle;
    readonly processRef: OwnedProcessIdentity;
    readonly kind: string;
  }) {
    const key = ownerKey(input.handle.owner);
    const entry = owners.get(key) ?? { revision: 0, uncertain: false, processes: new Map() };
    const existing = entry.processes.get(input.processRef.id);
    if (
      existing !== undefined &&
      existing.identity.creationMarker !== input.processRef.creationMarker
    ) {
      entry.uncertain = true;
      owners.set(key, entry);
      yield* PubSub.publish(changes, key);
      return yield* new CheckoutProcessAccountingError({ reason: "identity-reused" });
    }
    if (entry.revision >= Number.MAX_SAFE_INTEGER) {
      entry.uncertain = true;
      return yield* new CheckoutProcessAccountingError({ reason: "revision-exhausted" });
    }
    if (existing === undefined) {
      entry.processes.set(input.processRef.id, { identity: input.processRef, state: "unknown" });
      entry.revision++;
      owners.set(key, entry);
      yield* PubSub.publish(changes, key);
    }
  });

  const mark = Effect.fn("CheckoutProcessAccounting.mark")(function* (input: {
    readonly owner: OwnershipOwner;
    readonly processRef: OwnedProcessIdentity;
    readonly state: "live" | "stopped" | "unknown";
  }) {
    const key = ownerKey(input.owner);
    const entry = owners.get(key);
    const process = entry?.processes.get(input.processRef.id);
    if (entry === undefined || process === undefined)
      return yield* new CheckoutProcessAccountingError({ reason: "unregistered" });
    if (process.identity.creationMarker !== input.processRef.creationMarker) {
      entry.uncertain = true;
      yield* PubSub.publish(changes, key);
      return yield* new CheckoutProcessAccountingError({ reason: "identity-reused" });
    }
    if (entry.revision >= Number.MAX_SAFE_INTEGER) {
      entry.uncertain = true;
      return yield* new CheckoutProcessAccountingError({ reason: "revision-exhausted" });
    }
    process.state = input.state;
    entry.revision++;
    yield* PubSub.publish(changes, key);
  });

  const whenQuiescent = Effect.fn("CheckoutProcessAccounting.whenQuiescent")(function* (
    owner: OwnershipOwner,
  ) {
    const subscription = yield* PubSub.subscribe(changes);
    for (;;) {
      const observation = yield* observe(owner);
      if (observation.state !== "live") return observation;
      yield* PubSub.take(subscription);
    }
  }, Effect.scoped);

  return CheckoutProcessAccounting.of({ register, mark, observe, whenQuiescent });
});

export const layer = Layer.effect(CheckoutProcessAccounting, make);
