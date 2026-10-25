import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  ProjectId,
  RunId,
  ThreadId,
  type CheckoutIdentity,
} from "@t3tools/contracts";
import * as Ids from "@t3tools/provider-core/server/IdAllocator";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Ref from "effect/Ref";
import * as Layer from "effect/Layer";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Settings from "../serverSettings.ts";
import * as Environment from "../environment/ServerEnvironment.ts";
import * as Projections from "../orchestration-v2/ProjectionStore.ts";
import * as Projects from "../orchestration-v2/ProjectStore.ts";
import * as Events from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as Outbox from "../orchestration-v2/EffectOutbox.ts";
import * as Receipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as Positions from "../orchestration-v2/TurnItemPositionStore.ts";
import * as Identity from "./CheckoutIdentity.ts";
import * as Ownership from "./CheckoutOwnershipService.ts";
import * as Store from "./CheckoutOwnershipStore.ts";
import * as Accounting from "./CheckoutProcessAccounting.ts";
import * as Recovery from "./CheckoutOwnershipRecovery.ts";
import * as Admission from "./CheckoutAdmission.ts";
import * as Live from "./CheckoutAdmissionLive.ts";

const environmentId = EnvironmentId.make("environment");
const identity: CheckoutIdentity = {
  environmentId,
  checkoutKey: "checkout",
  canonicalCwd: "/fixture",
  filesystemIdentity: { device: "1", file: "2" },
  gitDir: null,
  gitCommonDir: null,
};
const storage = Layer.mergeAll(
  Store.layer,
  Accounting.layer,
  Projections.layer,
  Projects.layer,
  EventStore.layer,
  Outbox.layer,
  Receipts.layer,
  Positions.layer,
).pipe(Layer.provideMerge(Sqlite.layerMemory));
const dependencies = Recovery.layer.pipe(
  Layer.provideMerge(Ownership.layer),
  Layer.provideMerge(Events.layerFromStores),
  Layer.provideMerge(storage),
  Layer.provideMerge(
    Layer.mergeAll(
      Ids.layer,
      Settings.ServerSettingsService.layerTest({ workspaceOwnership: "enforce" }),
      Layer.succeed(Environment.ServerEnvironmentIdentity, {
        getEnvironmentId: Effect.succeed(environmentId),
      }),
      Layer.succeed(Identity.CheckoutIdentityResolver, {
        resolve: ({ cwd }) =>
          Effect.succeed(
            cwd.startsWith("/other")
              ? {
                  ...identity,
                  checkoutKey: "other",
                  canonicalCwd: "/other",
                  filesystemIdentity: { device: "1", file: "3" },
                }
              : identity,
          ),
      }),
    ),
  ),
);
const layer = Live.layer.pipe(Layer.provideMerge(dependencies));
const input = (name: string): Admission.RunAdmissionInput => ({
  commandId: CommandId.make(name),
  projectId: ProjectId.make("project"),
  threadId: ThreadId.make(name),
  runId: RunId.make(name),
  cwd: "/fixture",
  operation: "provider",
});

describe("CheckoutAdmissionLive", () => {
  it.effect("preserves legacy off-policy respawn without forgetting unknown descendants", () =>
    Effect.gen(function* () {
      const settings = yield* Settings.ServerSettingsService;
      const admission = yield* Admission.CheckoutAdmission;
      yield* settings.updateSettings({ workspaceOwnership: "off" });
      yield* admission.terminalOpen({ threadId: "legacy", terminalId: "shell", cwd: "/synthetic" });
      yield* admission.terminalStarted({
        threadId: "legacy",
        terminalId: "shell",
        cwd: "/synthetic",
        pid: 123,
      });
      yield* admission.terminalClosed({ threadId: "legacy", terminalId: "shell", confirmed: true });
      yield* admission.terminalOpen({ threadId: "legacy", terminalId: "shell", cwd: "/synthetic" });
      assert.isNull(yield* admission.admit({ ...input("off-writer"), cwd: "/not-real" }));
      yield* settings.updateSettings({ workspaceOwnership: "enforce" });
      yield* admission.admit(input("protected")).pipe(Effect.flip);
      assert.isNull(yield* (yield* Store.CheckoutOwnershipStore).read(identity.checkoutKey));
    }).pipe(Effect.provide(layer)),
  );
  it.effect(
    "retains unknown descendants after root-shell exit and rejects competitors and respawn",
    () =>
      Effect.gen(function* () {
        const admission = yield* Admission.CheckoutAdmission;
        const request = {
          ...input("shell-owner"),
          runId: null,
          setupTerminalId: "interactive",
          operation: "terminal" as const,
        };
        const handle = yield* admission.admit(request);
        yield* admission
          .terminalOpen({ threadId: "shell-owner", terminalId: "interactive", cwd: "/fixture" })
          .pipe(Effect.provideService(Admission.ActiveCheckoutOwnership, handle));
        yield* admission.terminalStarted({
          threadId: "shell-owner",
          terminalId: "interactive",
          cwd: "/fixture",
          pid: 123,
        });
        yield* admission.terminalClosed({
          threadId: "shell-owner",
          terminalId: "interactive",
          confirmed: true,
        });
        const claim = yield* (yield* Store.CheckoutOwnershipStore).read(identity.checkoutKey);
        assert.strictEqual(claim?.state, "uncertain");
        assert.strictEqual(
          (yield* (yield* Accounting.CheckoutProcessAccounting).observe(handle!)).state,
          "unknown",
        );
        yield* admission.admit(input("competitor")).pipe(Effect.flip);
        yield* admission
          .terminalOpen({ threadId: "shell-owner", terminalId: "interactive", cwd: "/fixture" })
          .pipe(Effect.flip);
      }).pipe(Effect.provide(layer)),
  );
  it.effect("retains fresh-worktree setup uncertainty while releasing root unstarted setup", () =>
    Effect.gen(function* () {
      const admission = yield* Admission.CheckoutAdmission;
      const store = yield* Store.CheckoutOwnershipStore;
      const root = {
        ...input("root-no-script"),
        runId: null,
        operation: "setup" as const,
        setupTerminalId: "root-setup",
      };
      yield* admission.admit(root);
      yield* admission.setupUnstartedCompleted({
        threadId: root.threadId,
        setupTerminalId: root.setupTerminalId,
      });
      assert.strictEqual((yield* store.read(identity.checkoutKey))?.state, "released");
      const fresh = {
        ...input("fresh-no-script"),
        runId: null,
        operation: "setup" as const,
        setupTerminalId: "fresh-setup",
      };
      const handle = yield* admission.admit(fresh);
      yield* admission.setupUncertain({
        threadId: fresh.threadId,
        setupTerminalId: fresh.setupTerminalId,
      });
      yield* admission.setupUnstartedCompleted({
        threadId: fresh.threadId,
        setupTerminalId: fresh.setupTerminalId,
      });
      assert.strictEqual((yield* store.read(identity.checkoutKey))?.state, "uncertain");
      assert.strictEqual(
        (yield* (yield* Accounting.CheckoutProcessAccounting).observe(handle!)).state,
        "unknown",
      );
      const conflict = yield* admission.admit(input("other-writer")).pipe(Effect.flip);
      assert.strictEqual(conflict.conflict?.state, "uncertain");
      assert.strictEqual(conflict.conflict?.owner.threadId, fresh.threadId);
    }).pipe(Effect.provide(layer)),
  );
  it.effect(
    "re-registers every known process for a new operation generation and isolates stale marks",
    () =>
      Effect.gen(function* () {
        const admission = yield* Admission.CheckoutAdmission;
        const accounting = yield* Accounting.CheckoutProcessAccounting;
        const first = yield* admission.admit({ ...input("generation-owner"), operation: "setup" });
        yield* admission
          .terminalOpen({ threadId: "generation-owner", terminalId: "setup", cwd: "/fixture" })
          .pipe(Effect.provideService(Admission.ActiveCheckoutOwnership, first));
        yield* admission.terminalStarted({
          threadId: "generation-owner",
          terminalId: "setup",
          cwd: "/fixture",
          pid: 123,
        });
        const previous = yield* accounting.observe(first!);
        const next = yield* admission.admit({
          ...input("generation-owner"),
          commandId: CommandId.make("advance-operation"),
          operation: "provider",
        });
        const current = yield* accounting.observe(next!);
        assert.strictEqual(next!.generation, first!.generation + 1);
        assert.strictEqual(current.state, "live");
        assert.deepStrictEqual(current.processIdentities, previous.processIdentities);
        for (const processRef of previous.processIdentities)
          yield* accounting.mark({ handle: first!, processRef, state: "stopped" });
        assert.strictEqual((yield* accounting.observe(next!)).state, "live");
        yield* admission.providerDrained({
          threadId: ThreadId.make("generation-owner"),
          runId: RunId.make("generation-owner"),
          observedStopped: true,
        });
        yield* admission.finalized({
          threadId: ThreadId.make("generation-owner"),
          runId: RunId.make("generation-owner"),
          checkpointRef: null,
        });
        assert.strictEqual(
          (yield* (yield* Store.CheckoutOwnershipStore).read(identity.checkoutKey))?.state,
          "held",
        );
        yield* admission.terminalClosed({
          threadId: "generation-owner",
          terminalId: "setup",
          confirmed: true,
          descendantsStopped: true,
        });
        assert.strictEqual(
          (yield* (yield* Store.CheckoutOwnershipStore).read(identity.checkoutKey))?.state,
          "released",
        );
      }).pipe(Effect.provide(layer)),
  );

  it.effect(
    "releases a durably completed setup reservation only when no terminal ever opened",
    () =>
      Effect.gen(function* () {
        const admission = yield* Admission.CheckoutAdmission;
        const request = {
          ...input("no-script"),
          runId: null,
          operation: "setup" as const,
          setupTerminalId: "setup-no-script",
        };
        const handle = yield* admission.admit(request);
        assert.strictEqual(
          (yield* (yield* Accounting.CheckoutProcessAccounting).observe(handle!)).state,
          "live",
        );
        yield* admission.setupUnstartedCompleted({
          threadId: request.threadId,
          setupTerminalId: request.setupTerminalId,
        });
        yield* admission.setupUnstartedCompleted({
          threadId: request.threadId,
          setupTerminalId: request.setupTerminalId,
        });
        assert.strictEqual(
          (yield* (yield* Store.CheckoutOwnershipStore).read(identity.checkoutKey))?.state,
          "released",
        );
      }).pipe(Effect.provide(layer)),
  );

  it.effect("does not treat opened terminals or failed runs as unstarted setup completion", () =>
    Effect.gen(function* () {
      const admission = yield* Admission.CheckoutAdmission;
      const handle = yield* admission.admit({
        ...input("setup-owner"),
        runId: null,
        operation: "setup",
        setupTerminalId: "opened",
      });
      yield* admission
        .terminalOpen({ threadId: "setup-owner", terminalId: "opened", cwd: "/fixture" })
        .pipe(Effect.provideService(Admission.ActiveCheckoutOwnership, handle));
      yield* admission.setupUnstartedCompleted({
        threadId: ThreadId.make("setup-owner"),
        setupTerminalId: "opened",
      });
      assert.strictEqual(
        (yield* (yield* Store.CheckoutOwnershipStore).read(identity.checkoutKey))?.state,
        "held",
      );
      yield* admission.terminalClosed({
        threadId: "setup-owner",
        terminalId: "opened",
        confirmed: true,
      });
      yield* admission.admit({ ...input("failed-run"), operation: "setup" });
      yield* admission.setupUnstartedCompleted({
        threadId: ThreadId.make("failed-run"),
        setupTerminalId: "unseen",
      });
      assert.strictEqual(
        (yield* (yield* Store.CheckoutOwnershipStore).read(identity.checkoutKey))?.state,
        "held",
      );
    }).pipe(Effect.provide(layer)),
  );
  it.effect(
    "keeps default-off starts working past the passive budget and retains a sticky observation gap",
    () =>
      Effect.gen(function* () {
        const settings = yield* Settings.ServerSettingsService;
        const admission = yield* Admission.CheckoutAdmission;
        yield* settings.updateSettings({ workspaceOwnership: "off" });
        for (let index = 0; index < 257; index++) {
          const request = input(`passive-${index}`);
          assert.isNull(yield* admission.admit({ ...request, cwd: `/synthetic/${index}` }));
          yield* admission.providerDrained({
            threadId: request.threadId,
            runId: request.runId!,
            observedStopped: false,
          });
          yield* admission.finalized({
            threadId: request.threadId,
            runId: request.runId!,
            checkpointRef: null,
          });
        }
        yield* admission.terminalOpen({
          threadId: "untracked",
          terminalId: "terminal",
          cwd: "/synthetic/terminal",
        });
        yield* admission.terminalWrite({
          threadId: "untracked",
          terminalId: "terminal",
          cwd: "/synthetic/terminal",
        });
        for (let index = 0; index < 256; index++) {
          const request = input(`passive-${index}`);
          yield* admission.providerDrained({
            threadId: request.threadId,
            runId: request.runId!,
            observedStopped: true,
          });
        }
        yield* settings.updateSettings({ workspaceOwnership: "enforce" });
        yield* admission.admit(input("protected-after-gap")).pipe(Effect.flip);
        const started = yield* Ref.make(false);
        yield* admission
          .withVerificationAdmission(
            { threadId: ThreadId.make("verification"), identity },
            Ref.set(started, true),
          )
          .pipe(Effect.flip);
        assert.isFalse(yield* Ref.get(started));
        assert.isNull(yield* (yield* Store.CheckoutOwnershipStore).read(identity.checkoutKey));
        yield* settings.updateSettings({ workspaceOwnership: "off" });
        assert.isNull(yield* admission.admit({ ...input("legacy-after-gap"), cwd: "/not-real" }));
      }).pipe(Effect.provide(layer)),
  );
  it.effect("rejects enforced terminal writes without a captured live process", () =>
    Effect.gen(function* () {
      const admission = yield* Admission.CheckoutAdmission;
      const handle = yield* admission.admit(input("writer"));
      yield* admission
        .terminalOpen({ threadId: "writer", terminalId: "setup", cwd: "/fixture" })
        .pipe(Effect.provideService(Admission.ActiveCheckoutOwnership, handle));
      const result = yield* admission
        .terminalWrite({ threadId: "writer", terminalId: "setup", cwd: "/fixture" })
        .pipe(Effect.flip);
      assert.strictEqual(result._tag, "CheckoutAdmissionError");
    }).pipe(Effect.provide(layer)),
  );
  it.effect("honors project overrides for both opt-in and opt-out", () =>
    Effect.gen(function* () {
      const settings = yield* Settings.ServerSettingsService;
      const admission = yield* Admission.CheckoutAdmission;
      yield* settings.updateSettings({
        workspaceOwnership: "off",
        projectSettingsOverrides: {
          [ProjectId.make("project")]: { workspaceOwnership: "enforce" },
        },
      });
      assert.isNotNull(yield* admission.admit(input("project-enabled")));
      yield* admission.providerDrained({
        threadId: ThreadId.make("project-enabled"),
        runId: RunId.make("project-enabled"),
        observedStopped: true,
      });
      yield* admission.finalized({
        threadId: ThreadId.make("project-enabled"),
        runId: RunId.make("project-enabled"),
        checkpointRef: null,
      });
      yield* settings.updateSettings({
        workspaceOwnership: "enforce",
        projectSettingsOverrides: { [ProjectId.make("project")]: { workspaceOwnership: "off" } },
      });
      assert.isNull(yield* admission.admit(input("project-disabled")));
    }).pipe(Effect.provide(layer)),
  );

  it.effect("keeps independent physical checkouts concurrent", () =>
    Effect.gen(function* () {
      const admission = yield* Admission.CheckoutAdmission;
      const entered = yield* Deferred.make<void>();
      const proceed = yield* Deferred.make<void>();
      const gate = yield* admission
        .withVerificationAdmission(
          { threadId: ThreadId.make("verification"), identity },
          Effect.gen(function* () {
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(proceed);
          }),
        )
        .pipe(Effect.forkScoped);
      yield* Deferred.await(entered);
      const other = yield* admission.admit({ ...input("independent"), cwd: "/other" });
      assert.strictEqual(other?.checkoutKey, "other");
      yield* Deferred.succeed(proceed, undefined);
      yield* Fiber.join(gate);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("does not mix delimiter-bearing owner or terminal identifiers", () =>
    Effect.gen(function* () {
      const admission = yield* Admission.CheckoutAdmission;
      const accounting = yield* Accounting.CheckoutProcessAccounting;
      const first = { ...input("first"), threadId: ThreadId.make("a:b"), runId: RunId.make("c") };
      const second = {
        ...input("second"),
        threadId: ThreadId.make("a"),
        runId: RunId.make("b:c"),
        cwd: "/other",
      };
      yield* admission.admit(first);
      const next = yield* admission.admit(second);
      yield* admission.providerDrained({
        threadId: first.threadId,
        runId: first.runId,
        observedStopped: true,
      });
      yield* admission.finalized({
        threadId: second.threadId,
        runId: second.runId,
        checkpointRef: null,
      });
      assert.strictEqual((yield* accounting.observe(next!)).state, "live");
      assert.strictEqual(
        (yield* (yield* Store.CheckoutOwnershipStore).read("other"))?.state,
        "held",
      );
    }).pipe(Effect.provide(layer)),
  );

  it.effect("re-evaluates an existing passive terminal when enforcement is enabled", () =>
    Effect.gen(function* () {
      const settings = yield* Settings.ServerSettingsService;
      const admission = yield* Admission.CheckoutAdmission;
      yield* settings.updateSettings({ workspaceOwnership: "off" });
      yield* admission.terminalOpen({ threadId: "a", terminalId: "b:c", cwd: "/fixture" });
      yield* admission.terminalOpen({ threadId: "a:b", terminalId: "c", cwd: "/other" });
      yield* admission.terminalStarted({
        threadId: "a:b",
        terminalId: "c",
        cwd: "/other",
        pid: 123,
      });
      yield* admission.terminalClosed({ threadId: "a", terminalId: "b:c", confirmed: true });
      yield* settings.updateSettings({ workspaceOwnership: "enforce" });
      yield* admission.terminalWrite({ threadId: "a:b", terminalId: "c", cwd: "/other" });
      const conflict = yield* admission
        .admit({ ...input("new-writer"), cwd: "/other" })
        .pipe(Effect.flip);
      assert.strictEqual(conflict.conflict?.owner.threadId, "a:b");
    }).pipe(Effect.provide(layer)),
  );

  it.effect("retains a prior passive terminal after its associated provider has finalized", () =>
    Effect.gen(function* () {
      const settings = yield* Settings.ServerSettingsService;
      const admission = yield* Admission.CheckoutAdmission;
      yield* settings.updateSettings({ workspaceOwnership: "off" });
      yield* admission.admit(input("prior"));
      yield* admission.terminalOpen({ threadId: "prior", terminalId: "setup", cwd: "/fixture" });
      yield* admission.terminalStarted({
        threadId: "prior",
        terminalId: "setup",
        pid: 123,
        cwd: "/fixture",
      });
      yield* settings.updateSettings({ workspaceOwnership: "enforce" });
      yield* admission.admit(input("new-writer")).pipe(Effect.flip);
      yield* admission.providerDrained({
        threadId: ThreadId.make("prior"),
        runId: RunId.make("prior"),
        observedStopped: true,
      });
      yield* admission.finalized({
        threadId: ThreadId.make("prior"),
        runId: RunId.make("prior"),
        checkpointRef: null,
      });
      const conflict = yield* admission
        .admit({ ...input("new-writer"), commandId: CommandId.make("retry-new") })
        .pipe(Effect.flip);
      assert.strictEqual(conflict.conflict?.owner.kind, "terminal");
    }).pipe(Effect.provide(layer)),
  );

  it.effect("blocks unknown recovery projections before acquiring any new claim", () =>
    Effect.gen(function* () {
      const projections = yield* Projections.ProjectionStoreV2;
      const admission = yield* Admission.CheckoutAdmission.pipe(
        Effect.provide(Layer.fresh(Live.layer)),
        Effect.provideService(Projections.ProjectionStoreV2, {
          ...projections,
          getUnreadableThreadIds: () => Effect.succeed([ThreadId.make("unreadable")]),
        }),
      );
      yield* admission.admit(input("writer")).pipe(Effect.flip);
      assert.isNull(yield* (yield* Store.CheckoutOwnershipStore).read(identity.checkoutKey));
    }).pipe(Effect.provide(layer)),
  );
  it.effect("acquires a durable fenced handle before an enforced writer starts", () =>
    Effect.gen(function* () {
      const admission = yield* Admission.CheckoutAdmission;
      const store = yield* Store.CheckoutOwnershipStore;
      const handle = yield* admission.admit(input("writer"));
      assert.isNotNull(handle);
      assert.deepStrictEqual((yield* store.read("checkout"))?.handle, handle);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("preserves default-off synthetic paths without resolving filesystem identity", () =>
    Effect.gen(function* () {
      const settings = yield* Settings.ServerSettingsService;
      yield* settings.updateSettings({ workspaceOwnership: "off" });
      const admission = yield* Admission.CheckoutAdmission.pipe(
        Effect.provide(Layer.fresh(Live.layer)),
        Effect.provideService(Identity.CheckoutIdentityResolver, {
          resolve: () => Effect.die("default-off resolved a synthetic path"),
        }),
      );
      assert.isNull(yield* admission.admit({ ...input("passive"), cwd: "/not-real" }));
      yield* admission.terminalOpen({
        threadId: "synthetic-thread",
        terminalId: "terminal",
        cwd: "/not-real",
      });
      yield* admission.terminalWrite({
        threadId: "synthetic-thread",
        terminalId: "terminal",
        cwd: "/not-real",
      });
    }).pipe(Effect.provide(layer)),
  );

  it.effect(
    "reconciles a passive writer before enabling enforcement and never steals its checkout",
    () =>
      Effect.gen(function* () {
        const settings = yield* Settings.ServerSettingsService;
        const admission = yield* Admission.CheckoutAdmission;
        const store = yield* Store.CheckoutOwnershipStore;
        yield* settings.updateSettings({ workspaceOwnership: "off" });
        assert.isNull(yield* admission.admit(input("prior")));
        yield* settings.updateSettings({ workspaceOwnership: "enforce" });
        const conflict = yield* admission.admit(input("next")).pipe(Effect.flip);
        assert.strictEqual(conflict.conflict?.owner.threadId, "prior");
        assert.strictEqual(
          (yield* store.read(identity.checkoutKey))?.handle.owner.threadId,
          "prior",
        );
      }).pipe(Effect.provide(layer)),
  );

  it.effect("requires both provider-drained confirmation and finalization, in either order", () =>
    Effect.gen(function* () {
      const admission = yield* Admission.CheckoutAdmission;
      const store = yield* Store.CheckoutOwnershipStore;
      yield* admission.admit(input("first"));
      yield* admission.providerDrained({
        threadId: ThreadId.make("first"),
        runId: RunId.make("first"),
        observedStopped: true,
      });
      assert.strictEqual((yield* store.read(identity.checkoutKey))?.state, "held");
      yield* admission.finalized({
        threadId: ThreadId.make("first"),
        runId: RunId.make("first"),
        checkpointRef: null,
      });
      assert.strictEqual((yield* store.read(identity.checkoutKey))?.state, "released");
      yield* admission.admit(input("second"));
      yield* admission.finalized({
        threadId: ThreadId.make("second"),
        runId: RunId.make("second"),
        checkpointRef: null,
      });
      assert.strictEqual((yield* store.read(identity.checkoutKey))?.state, "held");
      yield* admission.providerDrained({
        threadId: ThreadId.make("second"),
        runId: RunId.make("second"),
        observedStopped: true,
      });
      assert.strictEqual((yield* store.read(identity.checkoutKey))?.state, "released");
    }).pipe(Effect.provide(layer)),
  );

  it.effect("advances same-owner operations and rejects a stale start fence", () =>
    Effect.gen(function* () {
      const admission = yield* Admission.CheckoutAdmission;
      const first = yield* admission.admit({ ...input("writer"), operation: "setup" });
      const next = yield* admission.admit({
        ...input("writer"),
        commandId: CommandId.make("provider"),
        operation: "provider",
      });
      assert.strictEqual(next?.generation, first!.generation + 1);
      const started = yield* Ref.make(false);
      yield* admission.withCurrent(first, Ref.set(started, true)).pipe(Effect.flip);
      assert.isFalse(yield* Ref.get(started));
      yield* admission.withCurrent(next, Ref.set(started, true));
      assert.isTrue(yield* Ref.get(started));
    }).pipe(Effect.provide(layer)),
  );

  it.effect("accounts setup terminal PIDs under the same run and waits for confirmed closure", () =>
    Effect.gen(function* () {
      const admission = yield* Admission.CheckoutAdmission;
      const store = yield* Store.CheckoutOwnershipStore;
      const accounting = yield* Accounting.CheckoutProcessAccounting;
      const handle = yield* admission.admit(input("writer"));
      yield* admission
        .terminalOpen({ threadId: "writer", terminalId: "setup", cwd: "/fixture" })
        .pipe(Effect.provideService(Admission.ActiveCheckoutOwnership, handle));
      yield* admission.terminalStarted({
        threadId: "writer",
        terminalId: "setup",
        pid: 123,
        cwd: "/fixture",
      });
      const observation = yield* accounting.observe(handle!);
      assert.strictEqual(observation.state, "live");
      assert.isTrue(
        observation.processIdentities.some(
          (process) =>
            process.pid === 123 && process.creationMarker.includes(handle!.serverInstanceId),
        ),
      );
      yield* admission.providerDrained({
        threadId: ThreadId.make("writer"),
        runId: RunId.make("writer"),
        observedStopped: true,
      });
      yield* admission.finalized({
        threadId: ThreadId.make("writer"),
        runId: RunId.make("writer"),
        checkpointRef: null,
      });
      yield* admission.terminalWrite({ threadId: "writer", terminalId: "setup", cwd: "/fixture" });
      assert.strictEqual((yield* store.read(identity.checkoutKey))?.state, "held");
      yield* admission.terminalClosed({
        threadId: "writer",
        terminalId: "setup",
        confirmed: false,
      });
      assert.strictEqual((yield* accounting.observe(handle!)).state, "unknown");
      yield* admission.terminalClosed({
        threadId: "writer",
        terminalId: "setup",
        confirmed: true,
        descendantsStopped: true,
      });
      assert.strictEqual((yield* store.read(identity.checkoutKey))?.state, "released");
    }).pipe(Effect.provide(layer)),
  );

  it.effect("uses the supplied setup terminal owner for launch without a run", () =>
    Effect.gen(function* () {
      const admission = yield* Admission.CheckoutAdmission;
      const handle = yield* admission.admit({
        ...input("launch"),
        runId: null,
        operation: "setup",
        setupTerminalId: "preferred-setup",
      });
      assert.deepStrictEqual(handle?.owner, {
        kind: "terminal",
        threadId: ThreadId.make("launch"),
        terminalId: "preferred-setup",
      });
      yield* admission
        .terminalOpen({ threadId: "launch", terminalId: "preferred-setup", cwd: "/fixture" })
        .pipe(Effect.provideService(Admission.ActiveCheckoutOwnership, handle));
      yield* admission.terminalStarted({
        threadId: "launch",
        terminalId: "preferred-setup",
        pid: 124,
        cwd: "/fixture",
      });
      yield* admission.terminalClosed({
        threadId: "launch",
        terminalId: "preferred-setup",
        confirmed: true,
        descendantsStopped: true,
      });
      assert.strictEqual(
        (yield* (yield* Store.CheckoutOwnershipStore).read(identity.checkoutKey))?.state,
        "released",
      );
    }).pipe(Effect.provide(layer)),
  );

  it.effect("does not exempt a child thread carrying its parent's ownership context", () =>
    Effect.gen(function* () {
      const admission = yield* Admission.CheckoutAdmission;
      const handle = yield* admission.admit(input("parent"));
      const conflict = yield* admission
        .admit(input("child"))
        .pipe(Effect.provideService(Admission.ActiveCheckoutOwnership, handle), Effect.flip);
      assert.strictEqual(conflict.conflict?.owner.threadId, "parent");
    }).pipe(Effect.provide(layer)),
  );

  it.effect(
    "reads handoff and invalidation hooks at operation time and transfers without a release gap",
    () =>
      Effect.gen(function* () {
        const admission = yield* Admission.CheckoutAdmission;
        const ownership = yield* Ownership.CheckoutOwnershipService;
        const store = yield* Store.CheckoutOwnershipStore;
        const handle = yield* admission.admit(input("writer"));
        yield* admission.providerDrained({
          threadId: ThreadId.make("writer"),
          runId: RunId.make("writer"),
          observedStopped: true,
        });
        const called = yield* Ref.make(0);
        yield* admission
          .finalized({
            threadId: ThreadId.make("writer"),
            runId: RunId.make("writer"),
            checkpointRef: "checkpoint",
          })
          .pipe(
            Effect.provideService(Admission.CheckoutVerificationHandoff, {
              invalidate: () => Ref.update(called, (n) => n + 1),
              request: (request) =>
                ownership
                  .transfer({
                    commandId: request.commandId,
                    handle: request.handle,
                    nextOwner: {
                      kind: "verification",
                      threadId: request.threadId,
                      verificationId: "verification",
                    },
                    nextOperation: "verification",
                  })
                  .pipe(
                    Effect.as(true),
                    Effect.mapError(
                      (cause) =>
                        new Admission.CheckoutAdmissionError({ threadId: request.threadId, cause }),
                    ),
                  ),
            }),
          );
        const claim = yield* store.read(identity.checkoutKey);
        assert.strictEqual(claim?.handle.owner.kind, "verification");
        assert.strictEqual(claim?.handle.ownershipId, handle?.ownershipId);
        yield* admission.admit(input("competitor")).pipe(
          Effect.provideService(Admission.CheckoutVerificationHandoff, {
            request: () => Effect.succeed(false),
            invalidate: () => Ref.update(called, (n) => n + 1),
          }),
          Effect.flip,
        );
        assert.strictEqual(yield* Ref.get(called), 1);
      }).pipe(Effect.provide(layer)),
  );

  it.effect(
    "holds the verification admission gate through acquisition against policy-off starts",
    () =>
      Effect.gen(function* () {
        const settings = yield* Settings.ServerSettingsService;
        yield* settings.updateSettings({ workspaceOwnership: "off" });
        const admission = yield* Admission.CheckoutAdmission;
        const ownership = yield* Ownership.CheckoutOwnershipService;
        const entered = yield* Deferred.make<void>();
        const proceed = yield* Deferred.make<void>();
        const verifier = yield* admission
          .withVerificationAdmission(
            { threadId: ThreadId.make("verification"), identity },
            Effect.gen(function* () {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(proceed);
              return yield* ownership.acquire({
                commandId: CommandId.make("verify"),
                identity,
                owner: {
                  kind: "verification",
                  threadId: ThreadId.make("verification"),
                  verificationId: "verify",
                },
                operation: "verification",
              });
            }),
          )
          .pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        const writer = yield* admission
          .admit(input("off-writer"))
          .pipe(Effect.flip, Effect.forkScoped({ startImmediately: true }));
        yield* Deferred.succeed(proceed, undefined);
        assert.strictEqual((yield* Fiber.join(verifier))._tag, "Acquired");
        assert.strictEqual((yield* Fiber.join(writer)).conflict?.owner.kind, "verification");
      }).pipe(Effect.provide(layer)),
  );

  it.effect("keeps unknown process observations held after finalization", () =>
    Effect.gen(function* () {
      const admission = yield* Admission.CheckoutAdmission;
      yield* admission.admit(input("unknown"));
      yield* admission.providerDrained({
        threadId: ThreadId.make("unknown"),
        runId: RunId.make("unknown"),
        observedStopped: false,
      });
      yield* admission.finalized({
        threadId: ThreadId.make("unknown"),
        runId: RunId.make("unknown"),
        checkpointRef: null,
      });
      const conflict = yield* admission.admit(input("new")).pipe(Effect.flip);
      assert.strictEqual(conflict.conflict?.state, "uncertain");
    }).pipe(Effect.provide(layer)),
  );
});
