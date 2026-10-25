import { assert, it } from "@effect/vitest";
import { CommandId, EnvironmentId, EventId, ProjectId, RunId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

import * as Events from "../persistence/OrchestrationEventStore.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Ownership from "./CheckoutOwnershipStore.ts";

const layer = Layer.merge(Events.layer, Ownership.layer).pipe(
  Layer.provideMerge(Sqlite.layerMemory),
);

it.effect("keeps feature ledgers out of ordinary legacy replay and cursor sequencing", () =>
  Effect.gen(function* () {
    const events = yield* Events.OrchestrationEventStore;
    const ownership = yield* Ownership.CheckoutOwnershipStore;
    const projectId = ProjectId.make("project");
    const now = "2026-10-25T00:00:00Z";
    const common = {
      aggregateKind: "project" as const,
      aggregateId: projectId,
      occurredAt: now,
      commandId: null,
      causationEventId: null,
      correlationId: null,
      metadata: {},
    };
    const first = yield* events.appendProjectEvent({
      ...common,
      eventId: EventId.make("ordinary-create"),
      type: "project.created",
      payload: {
        projectId,
        title: "Project",
        workspaceRoot: "/repo",
        defaultModelSelection: null,
        scripts: [],
        createdAt: now,
        updatedAt: now,
      },
    });
    const acquired = yield* ownership.acquire({
      commandId: CommandId.make("claim"),
      ownershipId: "ownership",
      serverInstanceId: "server",
      identity: {
        environmentId: EnvironmentId.make("env"),
        checkoutKey: "checkout",
        canonicalCwd: "/repo",
        filesystemIdentity: { device: "1", file: "2" },
        gitDir: null,
        gitCommonDir: null,
      },
      owner: { kind: "run", threadId: ThreadId.make("thread"), runId: RunId.make("run") },
      operation: "provider",
    });
    assert.strictEqual(acquired._tag, "Acquired");
    if (acquired._tag !== "Acquired") throw new Error("expected acquired checkout");
    yield* ownership.transition({
      type: "release",
      commandId: CommandId.make("release"),
      handle: acquired.handle,
    });
    assert.strictEqual(yield* events.latestApplicationSequence, first.sequence);
    const second = yield* events.appendProjectEvent({
      ...common,
      eventId: EventId.make("ordinary-update"),
      type: "project.meta-updated",
      payload: { projectId, title: "Updated", updatedAt: now },
    });
    assert.strictEqual(second.sequence, first.sequence + 1);
    const replay = yield* events
      .readApplicationEvents({ afterSequence: 0, throughSequence: second.sequence })
      .pipe(Stream.runCollect);
    assert.deepStrictEqual(
      replay.map((stored) => ("event" in stored ? stored.event.type : stored.type)),
      ["project.created", "project.meta-updated"],
    );
  }).pipe(Effect.provide(layer)),
);
