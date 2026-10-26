import {
  OwnershipConflict,
  WorkspaceExecutionRequestError,
  WorkspaceExecutionRpcGroup,
  WORKSPACE_EXECUTION_METHODS,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";

import { WorkspaceExecutionApi } from "./WorkspaceExecutionApi.ts";

const decodeConflict = Schema.decodeUnknownOption(OwnershipConflict);
const isRequestError = Schema.is(WorkspaceExecutionRequestError);

export function workspaceExecutionError(cause: unknown): WorkspaceExecutionRequestError {
  if (isRequestError(cause)) return cause;
  if (Predicate.hasProperty(cause, "conflict")) {
    const conflict = decodeConflict(cause.conflict);
    if (Option.isSome(conflict))
      return new WorkspaceExecutionRequestError({ reason: "conflict", conflict: conflict.value });
  }
  if (Predicate.isTagged(cause, "VerificationNotConfigured"))
    return new WorkspaceExecutionRequestError({ reason: "not-configured" });
  if (Predicate.isTagged(cause, "StaleOwnershipHandle"))
    return new WorkspaceExecutionRequestError({ reason: "invalid-input" });
  if (Predicate.hasProperty(cause, "reason") && cause.reason === "policy-denied")
    return new WorkspaceExecutionRequestError({ reason: "policy" });
  return new WorkspaceExecutionRequestError({ reason: "unavailable" });
}

export const makeHandlers = Effect.gen(function* () {
  const waits = yield* WorkspaceExecutionApi;
  const verification = waits;
  return WorkspaceExecutionRpcGroup.of({
    [WORKSPACE_EXECUTION_METHODS.inspect]: ({ threadId }) =>
      waits.inspect(threadId).pipe(Effect.mapError(workspaceExecutionError)),
    [WORKSPACE_EXECUTION_METHODS.wait]: (input) =>
      waits.wait(input).pipe(Effect.mapError(workspaceExecutionError)),
    [WORKSPACE_EXECUTION_METHODS.cancelWait]: (input) =>
      waits.cancelWait(input).pipe(
        Effect.mapError(workspaceExecutionError),
        Effect.flatMap((result) =>
          result === null
            ? Effect.fail(new WorkspaceExecutionRequestError({ reason: "not-found" }))
            : Effect.succeed(result),
        ),
      ),
    [WORKSPACE_EXECUTION_METHODS.request]: (input) =>
      verification.request(input).pipe(Effect.mapError(workspaceExecutionError)),
    [WORKSPACE_EXECUTION_METHODS.cancel]: (input) =>
      verification.cancel(input).pipe(
        Effect.mapError(workspaceExecutionError),
        Effect.flatMap((result) =>
          result === null
            ? Effect.fail(new WorkspaceExecutionRequestError({ reason: "not-found" }))
            : Effect.succeed(result),
        ),
      ),
    [WORKSPACE_EXECUTION_METHODS.get]: ({ verificationId }) =>
      verification.get(verificationId).pipe(Effect.mapError(workspaceExecutionError)),
    [WORKSPACE_EXECUTION_METHODS.latest]: ({ threadId }) =>
      verification.latest(threadId).pipe(Effect.mapError(workspaceExecutionError)),
    [WORKSPACE_EXECUTION_METHODS.output]: (input) =>
      verification.output(input).pipe(
        Effect.mapError(workspaceExecutionError),
        Effect.flatMap((result) =>
          result === null
            ? Effect.fail(new WorkspaceExecutionRequestError({ reason: "not-found" }))
            : Effect.succeed(result),
        ),
      ),
  });
});
