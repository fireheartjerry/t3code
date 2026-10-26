import { expect, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import {
  CommandId,
  EnvironmentId,
  ProjectId,
  ThreadId,
  type AuthMcpClientAccess,
  type VerificationRecord,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import type { Tool } from "effect/ai";

import * as Threads from "../../../orchestration-v2/ThreadManagementService.ts";
import {
  WorkspaceExecutionApi,
  WorkspaceExecutionAvailable,
} from "../../../workspace/WorkspaceExecutionApi.ts";
import { idleThreadProjection, liveThreadShell } from "../../McpToolAccess.testkit.ts";
import * as Access from "../../McpToolAccess.ts";
import * as Invocation from "../../McpInvocationContext.ts";
import * as Handlers from "./handlers.ts";
import { ThreadToolkit } from "./tools.ts";

const threadId = ThreadId.make("selected-thread");
const evidence: VerificationRecord = {
  verificationId: "verification",
  commandId: CommandId.make("command"),
  projectId: ProjectId.make("project"),
  threadId,
  sourceRunId: null,
  environmentId: EnvironmentId.make("environment"),
  ownerHandle: null,
  actionSnapshot: {
    actionId: "tests",
    revision: "revision",
    command: "vp test run src/logic.test.ts",
    shell: "/bin/sh",
    cwd: "/repo",
    timeoutMs: 1000,
  },
  execution: "failed",
  currentness: "Unknown",
  requestedAt: "2026-10-25T00:00:00Z",
  startedAt: null,
  finishedAt: "2026-10-25T00:00:01Z",
  exitCode: null,
  signal: null,
  terminationReason: "missing-command",
  startSource: null,
  endSource: null,
  startMutationGeneration: null,
  endMutationGeneration: null,
  observationState: "unavailable",
  outputReferences: [],
  truncated: false,
};

const fixture = Effect.fn("test.workspaceMcpFixture")(function* (
  access: AuthMcpClientAccess = "full-access",
  available = true,
) {
  const requests: { readonly threadId: ThreadId; readonly commandId: CommandId }[] = [];
  const targetShell = (id: ThreadId) =>
    liveThreadShell(id, { runtimeMode: access === "read-only" ? "full-access" : access });
  const dependencies = Layer.mergeAll(
    NodeCrypto.layer,
    Layer.succeed(WorkspaceExecutionAvailable, available),
    Layer.succeed(WorkspaceExecutionApi, {
      ...WorkspaceExecutionApi.defaultValue(),
      request: (input) =>
        Effect.sync(() => {
          requests.push(input);
          return evidence;
        }),
      latest: () => Effect.succeed(evidence),
      get: () => Effect.succeed({ ...evidence, threadId: ThreadId.make("foreign-thread") }),
    }),
    Layer.succeed(Invocation.McpInvocationContext, {
      environmentId: EnvironmentId.make("environment"),
      requestNamespace: "client-session",
      issuedAt: 0,
      thread: undefined,
      client: { sessionId: "client", label: "test", access },
      capabilities: new Set(["orchestration" as const]),
    }),
    Layer.mock(Threads.ThreadManagementService)({
      getThreadShell: (id) => Effect.succeed(targetShell(id)),
      getProjectThreadRecords: (input) =>
        Effect.succeed(idleThreadProjection(targetShell(input.threadId))),
    }),
  );
  const toolkit = yield* ThreadToolkit.pipe(
    Effect.provide(Access.HandlersLayer.layer(Handlers.layer).pipe(Layer.provide(dependencies))),
  );
  const call = <Name extends keyof typeof ThreadToolkit.tools>(
    name: Name,
    input: Parameters<typeof toolkit.handle<Name>>[1],
  ) =>
    toolkit.handle(name, input).pipe(
      Stream.unwrap,
      Stream.runCollect,
      Effect.map(
        (events) => events.at(-1)!.result as Tool.Success<(typeof ThreadToolkit.tools)[Name]>,
      ),
      Effect.provide(dependencies),
    );
  return { requests, call };
});

it.effect.each(["read-only", "auto", "approval-required"] as const)(
  "%s MCP clients cannot execute verification",
  (access) =>
    Effect.gen(function* () {
      const f = yield* fixture(access);
      const result = yield* f.call("verify_thread", { threadId });
      expect(result).toMatchObject({ code: "capability_denied" });
      expect(f.requests).toHaveLength(0);
    }),
);

it.effect("evidence reads preserve the real outcome and never execute", () =>
  Effect.gen(function* () {
    const f = yield* fixture("read-only");
    expect(yield* f.call("get_thread_verification", { threadId })).toEqual(evidence);
    expect(f.requests).toHaveLength(0);
    const foreign = yield* f.call("get_thread_verification", {
      threadId,
      verificationId: "foreign",
    });
    expect(foreign).toMatchObject({ code: "invalid_request" });
    expect(f.requests).toHaveLength(0);
  }),
);

it.effect("explicit verification binds the destination and a stable namespaced request ID", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    expect(yield* f.call("verify_thread", { threadId, clientRequestId: "one-attempt" })).toEqual(
      evidence,
    );
    yield* f.call("verify_thread", { threadId, clientRequestId: "one-attempt" });
    expect(f.requests).toHaveLength(2);
    expect(f.requests[0]?.threadId).toBe(threadId);
    expect(f.requests[0]?.commandId).toBe(f.requests[1]?.commandId);
  }),
);

it.effect("a missing capability cannot invoke the action even for full-access callers", () =>
  Effect.gen(function* () {
    const f = yield* fixture("full-access", false);
    expect(yield* f.call("verify_thread", { threadId })).toMatchObject({
      code: "capability_denied",
    });
    expect(f.requests).toHaveLength(0);
  }),
);
