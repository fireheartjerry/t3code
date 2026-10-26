import type {
  AcquireCheckoutOwnershipInput,
  AcquireCheckoutOwnershipResult,
  CheckoutOwnershipRecord,
} from "@t3tools/contracts";

export function decideCheckoutAcquisition(input: {
  readonly current: CheckoutOwnershipRecord | null;
  readonly request: AcquireCheckoutOwnershipInput;
  readonly ownershipId: string;
  readonly serverInstanceId: string;
  readonly now: string;
}): AcquireCheckoutOwnershipResult {
  if (input.current !== null && input.current.state !== "released") {
    const { identity, handle, state } = input.current;
    return {
      _tag: "Conflict",
      conflict: {
        identity,
        owner: handle.owner,
        operation: handle.operation,
        generation: handle.generation,
        state,
        resolutions: (["open-owner", "wait", "create-worktree", "stop-owner"] as const).map(
          (action) => ({
            action,
            threadId: handle.owner.threadId,
            checkoutKey: identity.checkoutKey,
          }),
        ),
      },
    };
  }
  if ((input.current?.handle.generation ?? 0) >= Number.MAX_SAFE_INTEGER) {
    throw new RangeError("Checkout ownership generation exhausted.");
  }
  return {
    _tag: "Acquired",
    handle: {
      ownershipId: input.ownershipId,
      checkoutKey: input.request.identity.checkoutKey,
      generation: (input.current?.handle.generation ?? 0) + 1,
      owner: input.request.owner,
      operation: input.request.operation,
      serverInstanceId: input.serverInstanceId,
    },
  };
}
