import {
  CommandId,
  type RuntimeRequestId,
  ThreadId,
  type OrchestrationV2ThreadProjection,
  type RunId,
  OrchestratorMcpFailure,
  type OrchestrationV2Command,
  type WorkspaceExecutionRequestError,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { modelSelectionCommandType } from "@t3tools/shared/model";
import { formatThreadLink } from "@t3tools/shared/threadLinks";

import * as McpToolAccess from "../../McpToolAccess.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import {
  WorkspaceExecutionApi,
  WorkspaceExecutionAvailable,
} from "../../../workspace/WorkspaceExecutionApi.ts";
import {
  dispatchFailure,
  newCommandId,
  readCaller,
  readThread,
  unavailable,
  assertFullAccess,
} from "../../threadAccess.ts";
import * as ThreadSearch from "../../../orchestration-v2/ThreadSearch.ts";
import * as ScheduledTasks from "../../../scheduledTasks/ScheduledTaskService.ts";
import { queuedRunsInDeliveryOrder } from "../../../orchestration-v2/QueuedRunOrder.ts";
import { ThreadToolkit } from "./tools.ts";

function queueEntry(
  projection: Pick<OrchestrationV2ThreadProjection, "runs" | "messages">,
  runId: RunId,
  limit: number,
) {
  const run = projection.runs.find((run) => run.id === runId && run.status === "queued");
  const message = projection.messages.find((message) => message.id === run?.userMessageId);
  if (run === undefined || message === undefined) return undefined;
  const characters = Array.from(message.text);
  return {
    queuedRunId: run.id,
    text: characters.slice(0, limit).join(""),
    truncated: characters.length > limit,
  };
}
const dispatch = Effect.fn("mcp.dispatchThreadCommand")(function* (
  threadId: ThreadId | undefined,
  command: (common: { commandId: CommandId; threadId: ThreadId }) => OrchestrationV2Command,
) {
  const { threads, projection } = yield* readThread(threadId);
  const result = yield* threads
    .dispatch(command({ commandId: yield* newCommandId(), threadId: projection.thread.id }))
    .pipe(Effect.mapError(dispatchFailure));
  return { sequence: result.sequence };
});

const readQuestion = Effect.fn("mcp.readQuestion")(function* (input: {
  threadId?: ThreadId | undefined;
  requestId: RuntimeRequestId;
}) {
  const context = yield* readThread(input.threadId, ["runtimeRequests", "turnItems"]);
  const request = context.projection.runtimeRequests.find(
    (request) => request.id === input.requestId && request.kind === "user_input",
  );
  const item = context.projection.turnItems.find(
    (item) => item.type === "user_input_request" && item.requestId === input.requestId,
  );
  if (request === undefined || item?.type !== "user_input_request")
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message: "The pending user-input request was not found.",
    });
  if (request.status !== "pending")
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message:
        request.status === "resolved"
          ? "The user-input request was already answered."
          : `The user-input request is no longer pending (${request.status}).`,
    });
  return { ...context, request, item };
});
const writesThread = <P extends { readonly threadId?: ThreadId | undefined }, A, E, R>(
  handle: (params: P) => Effect.Effect<A, E, R>,
) => McpToolAccess.writesThreads((params: P) => [params.threadId], handle);

const workspaceFailure = (error: WorkspaceExecutionRequestError) =>
  new OrchestratorMcpFailure({
    code: error.reason === "policy" ? "capability_denied" : "orchestration_error",
    message: error.message.slice(0, 1000),
  });
const workspaceApi = Effect.gen(function* () {
  if (!(yield* WorkspaceExecutionAvailable))
    return yield* new OrchestratorMcpFailure({
      code: "capability_denied",
      message: "This environment does not expose checkout ownership or verification evidence.",
    });
  return yield* WorkspaceExecutionApi;
});
const workspaceCommandId = Effect.fn("mcp.workspaceCommandId")(function* (
  operation: string,
  clientRequestId?: string,
) {
  if (clientRequestId === undefined) return yield* newCommandId();
  const scope = yield* McpInvocationContext.McpInvocationContext;
  return CommandId.make(`workspace:${scope.requestNamespace}:${operation}:${clientRequestId}`);
});

export const layer = McpToolAccess.toLayer(ThreadToolkit, {
  inspect_thread_workspace: McpToolAccess.reads((input) =>
    Effect.gen(function* () {
      const { projection } = yield* readThread(input.threadId);
      return yield* (yield* workspaceApi)
        .inspect(projection.thread.id)
        .pipe(Effect.mapError(workspaceFailure));
    }),
  ),
  wait_for_thread_workspace: writesThread((input) =>
    Effect.gen(function* () {
      const { projection } = yield* readThread(input.threadId);
      const api = yield* workspaceApi;
      const owner = yield* api
        .inspect(projection.thread.id)
        .pipe(Effect.mapError(workspaceFailure));
      const checkoutKey = input.checkoutKey ?? owner?.identity.checkoutKey;
      const generation = input.generation ?? owner?.handle.generation;
      if (checkoutKey === undefined || generation === undefined)
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message:
            "No active checkout owner was found. Retry the thread through its normal start flow.",
        });
      return yield* api
        .wait({
          threadId: projection.thread.id,
          commandId: yield* workspaceCommandId("wait", input.clientRequestId),
          checkoutKey,
          generation,
        })
        .pipe(Effect.mapError(workspaceFailure));
    }),
  ),
  cancel_workspace_wait: writesThread((input) =>
    Effect.gen(function* () {
      const { projection } = yield* readThread(input.threadId);
      return yield* (yield* workspaceApi)
        .cancelWait({
          threadId: projection.thread.id,
          waitId: input.waitId,
          commandId: yield* workspaceCommandId("cancel-wait", input.clientRequestId),
        })
        .pipe(Effect.mapError(workspaceFailure));
    }),
  ),
  verify_thread: writesThread((input) =>
    Effect.gen(function* () {
      yield* assertFullAccess(
        yield* readCaller(),
        "Verification executes a configured command and requires full-access caller authority.",
      );
      const { projection } = yield* readThread(input.threadId);
      return yield* (yield* workspaceApi)
        .request({
          threadId: projection.thread.id,
          commandId: yield* workspaceCommandId("verify", input.clientRequestId),
        })
        .pipe(Effect.mapError(workspaceFailure));
    }),
  ),
  get_thread_verification: McpToolAccess.reads((input) =>
    Effect.gen(function* () {
      const { projection } = yield* readThread(input.threadId);
      const api = yield* workspaceApi;
      const record = yield* (
        input.verificationId === undefined
          ? api.latest(projection.thread.id)
          : api.get(input.verificationId)
      ).pipe(Effect.mapError(workspaceFailure));
      if (record !== null && record.threadId !== projection.thread.id)
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "The verification does not belong to the selected thread.",
        });
      return record;
    }),
  ),
  cancel_thread_verification: writesThread((input) =>
    Effect.gen(function* () {
      const { projection } = yield* readThread(input.threadId);
      const api = yield* workspaceApi;
      const record = yield* (
        input.verificationId === undefined
          ? api.latest(projection.thread.id)
          : api.get(input.verificationId)
      ).pipe(Effect.mapError(workspaceFailure));
      if (record === null) return null;
      if (record.threadId !== projection.thread.id)
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "The verification does not belong to the selected thread.",
        });
      return yield* api
        .cancel({
          verificationId: record.verificationId,
          commandId: yield* workspaceCommandId("cancel-verification", input.clientRequestId),
        })
        .pipe(Effect.mapError(workspaceFailure));
    }),
  ),
  run_scheduled_task_now: McpToolAccess.writesEnvironment((input) =>
    Effect.gen(function* () {
      const scheduler = yield* ScheduledTasks.ScheduledTaskService;
      const { tasks } = yield* scheduler.list().pipe(Effect.mapError(unavailable));
      if (!tasks.some((task) => task.id === input.taskId))
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "The scheduled task was not found.",
        });
      const { task, threadId } = yield* scheduler
        .runNow({ id: input.taskId })
        .pipe(Effect.mapError(unavailable));
      return {
        taskId: task.id,
        threadId,
        lastRunStatus: task.lastRunStatus,
        runCount: task.runCount,
        nextRunAt: task.nextRunAt,
      };
    }),
  ),
  t3_thread_search: McpToolAccess.reads((input) =>
    Effect.gen(function* () {
      const { caller } = yield* readCaller();
      const threadSearch = yield* ThreadSearch.ThreadSearch;
      const { matches } = yield* threadSearch
        .searchThreads({ ...input, projectId: input.projectId ?? caller?.projectId })
        .pipe(Effect.mapError(unavailable));
      return {
        matches: matches.map((match) => ({
          ...match,
          link: formatThreadLink(match.threadId, match.title),
        })),
      };
    }),
  ),
  t3_thread_fork: writesThread((input) =>
    Effect.gen(function* () {
      const { threads, projection } = yield* readThread(input.threadId);
      const commandId = yield* newCommandId();
      const targetThreadId = ThreadId.make(`${commandId}:fork`);
      const result = yield* threads
        .dispatch({
          type: "thread.fork",
          commandId,
          sourceThreadId: projection.thread.id,
          targetThreadId,
          sourcePoint: input.sourcePoint,
          ...(input.title === undefined ? {} : { title: input.title }),
          createdBy: "agent",
          creationSource: "mcp",
        })
        .pipe(Effect.mapError(dispatchFailure));
      return { sequence: result.sequence, targetThreadId };
    }),
  ),
  t3_thread_merge_back: McpToolAccess.writesThreads(
    (input) => [input.targetThreadId, input.sourceThreadId],
    (input) =>
      Effect.gen(function* () {
        const context = yield* readThread(input.targetThreadId);
        const source = yield* readThread(input.sourceThreadId);
        const result = yield* context.threads
          .dispatch({
            type: "thread.merge_back",
            commandId: yield* newCommandId(),
            sourceThreadId: source.projection.thread.id,
            targetThreadId: input.targetThreadId,
            sourcePoint: input.sourcePoint,
            createdBy: "agent",
            creationSource: "mcp",
          })
          .pipe(Effect.mapError(dispatchFailure));
        return { sequence: result.sequence, targetThreadId: input.targetThreadId };
      }),
  ),
  t3_thread_transfers: McpToolAccess.reads((input) =>
    Effect.gen(function* () {
      const { projection } = yield* readThread(input.threadId, ["contextTransfers"]);
      return {
        transfers: projection.contextTransfers.map(
          ({ id, sourceThreadId, targetThreadId, status }) => ({
            id,
            sourceThreadId,
            targetThreadId,
            status,
          }),
        ),
      };
    }),
  ),
  t3_thread_configuration: McpToolAccess.reads((input) =>
    Effect.gen(function* () {
      const {
        projection: { thread },
      } = yield* readThread(input.threadId);
      return {
        threadId: thread.id,
        modelSelection: thread.modelSelection,
        runtimeMode: thread.runtimeMode,
        interactionMode: thread.interactionMode,
      };
    }),
  ),
  t3_thread_configure: writesThread((input) =>
    Effect.gen(function* () {
      const {
        threads,
        projection: { thread },
      } = yield* readThread(input.threadId);
      const type = modelSelectionCommandType(thread.providerInstanceId, input.modelSelection);
      const result = yield* threads
        .dispatch({
          type,
          threadId: thread.id,
          commandId: yield* newCommandId(),
          modelSelection: input.modelSelection,
        })
        .pipe(Effect.mapError(dispatchFailure));
      return { sequence: result.sequence };
    }),
  ),
  t3_pending_request_list: McpToolAccess.reads((input) =>
    Effect.gen(function* () {
      const { projection } = yield* readThread(input.threadId, ["runtimeRequests"]);
      return {
        requestIds: projection.runtimeRequests
          .filter((request) => request.kind === "user_input" && request.status === "pending")
          .map((request) => request.id),
      };
    }),
  ),
  t3_pending_request_read: McpToolAccess.reads((input) =>
    Effect.gen(function* () {
      const { item } = yield* readQuestion(input);
      return { requestId: input.requestId, questions: item.questions };
    }),
  ),
  t3_pending_request_respond: writesThread((input) =>
    Effect.gen(function* () {
      const { threads, projection } = yield* readQuestion(input);
      const result = yield* threads
        .dispatch({
          type: "runtime-request.respond",
          threadId: projection.thread.id,
          commandId: yield* newCommandId(),
          requestId: input.requestId,
          answers: input.answers,
        })
        .pipe(Effect.mapError(dispatchFailure));
      return { sequence: result.sequence };
    }),
  ),
  t3_queue_list: McpToolAccess.reads((input) =>
    Effect.gen(function* () {
      const { projection } = yield* readThread(input.threadId, ["runs", "messages"]);
      const runs = queuedRunsInDeliveryOrder(projection);
      const cursor = input.cursor ?? 0;
      const end = cursor + (input.limit ?? 20);
      return {
        items: runs.slice(cursor, end).flatMap((run) => {
          const entry = queueEntry(projection, run.id, 1000);
          return entry === undefined ? [] : [entry];
        }),
        nextCursor: end < runs.length ? end : null,
      };
    }),
  ),
  t3_queue_read: McpToolAccess.reads((input) =>
    Effect.gen(function* () {
      const { projection } = yield* readThread(input.threadId, ["runs", "messages"]);
      const entry = queueEntry(projection, input.queuedRunId, 16000);
      return (
        entry ??
        (yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "The queued message was not found.",
        }))
      );
    }),
  ),
  t3_queue_edit: writesThread((input) =>
    dispatch(input.threadId, (common) => ({
      ...common,
      type: "queued-run.edit",
      runId: input.queuedRunId,
      text: input.text,
    })),
  ),
  t3_queue_cancel: writesThread((input) =>
    dispatch(input.threadId, (common) => ({
      ...common,
      type: "queued-run.cancel",
      runId: input.queuedRunId,
    })),
  ),
  t3_queue_reorder: writesThread((input) =>
    dispatch(input.threadId, (common) => ({
      ...common,
      type: "queued-run.reorder",
      runId: input.queuedRunId,
      beforeRunId: input.beforeRunId,
    })),
  ),
  t3_queue_promote_to_steer: writesThread((input) =>
    dispatch(input.threadId, (common) => ({
      ...common,
      type: "queued-message.promote-to-steer",
      queuedRunId: input.queuedRunId,
      targetRunId: input.targetRunId,
    })),
  ),
  t3_thread_organize: writesThread((input) =>
    Effect.gen(function* () {
      const { threads, projection, caller } = yield* readThread(input.threadId);
      const common = { commandId: yield* newCommandId(), threadId: projection.thread.id };
      if (input.action === "settle") {
        return yield* threads
          .settleThread({ ...common, byOwnAgent: caller?.id === projection.thread.id })
          .pipe(Effect.mapError(dispatchFailure));
      }
      let command: OrchestrationV2Command;
      switch (input.action) {
        case "snooze":
          if (input.snoozedUntil === undefined) {
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "snooze requires snoozedUntil.",
            });
          }
          command = { ...common, type: "thread.snooze", snoozedUntil: input.snoozedUntil };
          break;
        case "unsnooze":
        case "unsettle":
          command = { ...common, type: `thread.${input.action}`, reason: "user" };
          break;
        case "mark_unread":
          command = { ...common, type: "thread.mark-unread" };
          break;
        default:
          command = { ...common, type: `thread.${input.action}` };
      }
      const result = yield* threads.dispatch(command).pipe(Effect.mapError(dispatchFailure));
      return { sequence: result.sequence };
    }),
  ),
});
