import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import * as Ownership from "./CheckoutOwnershipService.ts";
import * as Store from "./CheckoutOwnershipStore.ts";

export class CheckoutOwnershipRecoveryError extends Schema.TaggedError<CheckoutOwnershipRecoveryError>()(
  "CheckoutOwnershipRecoveryError",
  { cause: Schema.Defect() },
) {
  override get message(): string {
    return "Failed to reconcile checkout ownership.";
  }
}

export class CheckoutOwnershipRecovery extends Context.Service<
  CheckoutOwnershipRecovery,
  { readonly reconcile: () => Effect.Effect<void, CheckoutOwnershipRecoveryError> }
>()("t3/workspace/CheckoutOwnershipRecovery") {}

const make = Effect.gen(function* () {
  const store = yield* Store.CheckoutOwnershipStore;
  const ownership = yield* Ownership.CheckoutOwnershipService;

  const reconcile = Effect.fn("CheckoutOwnershipRecovery.reconcile")(
    function* () {
      const records = yield* store.listRecoverable;
      for (const record of records) yield* ownership.reconcileClaim(record.handle);
    },
    Effect.mapError((cause) => new CheckoutOwnershipRecoveryError({ cause })),
  );

  return CheckoutOwnershipRecovery.of({ reconcile });
});

export const layer = Layer.effect(CheckoutOwnershipRecovery, make);
