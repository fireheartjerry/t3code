// @effect-diagnostics nodeBuiltinImport:off -- Fixtures use lossless IDs matching CheckoutIdentityResolver.
import * as NodeFS from "node:fs";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { EnvironmentId, type CheckoutIdentity, type VerificationConfig } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import * as VerificationSourceObserver from "./VerificationSourceObserver.ts";

const fixture = Effect.fn("VerificationSourceObserver.test.fixture")(function* (git = false) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const temporary = yield* fs.makeTempDirectoryScoped({ prefix: "t3-verification-observer-" });
  const root = yield* fs.realPath(temporary);
  const cwd = path.join(root, "checkout");
  yield* fs.makeDirectory(path.join(cwd, "src/nested"), { recursive: true });
  const gitDir = git ? path.join(root, "common/worktrees/test") : null;
  const gitCommonDir = git ? path.join(root, "common") : null;
  if (gitDir !== null) yield* fs.makeDirectory(gitDir, { recursive: true });
  const identity: CheckoutIdentity = {
    environmentId: EnvironmentId.make("test"),
    checkoutKey: cwd,
    canonicalCwd: cwd,
    filesystemIdentity: {
      device: NodeFS.statSync(cwd, { bigint: true }).dev.toString(),
      file: NodeFS.statSync(cwd, { bigint: true }).ino.toString(),
    },
    gitDir,
    gitCommonDir,
  };
  const config: VerificationConfig = {
    actionId: "test",
    automatic: false,
    timeoutMs: 1_000,
    inputs: {
      include: ["src/**/*.ts", "package.json", "t3.json", ".env"],
      exclude: ["**/dist/**", "reports/**"],
      nonGit: "fingerprint",
    },
  };
  const input = { identity, config, configRevision: "config-v1" };
  const opened: {
    directory: string;
    callbacks: VerificationSourceObserver.VerificationWatchCallbacks;
    closed: boolean;
  }[] = [];
  const watch = (
    directory: string,
    callbacks: VerificationSourceObserver.VerificationWatchCallbacks,
  ) => {
    const watcher = { directory, callbacks, closed: false };
    opened.push(watcher);
    const close = () => {
      watcher.closed = true;
      callbacks.close();
    };
    return { close, ready: Effect.succeed("continuous" as const) };
  };
  const scope = yield* Scope.make();
  yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
  const observer = yield* Layer.build(VerificationSourceObserver.layer).pipe(
    Effect.provideService(Scope.Scope, scope),
    Effect.provideService(VerificationSourceObserver.VerificationWatch, watch),
    Effect.map((context) =>
      Context.get(context, VerificationSourceObserver.VerificationSourceObserver),
    ),
  );
  function emit(directory: string, filename: string | null, event = "change") {
    const watcher = opened.findLast((item) => item.directory === directory && !item.closed);
    expect(watcher, `active watcher for ${directory}`).toBeDefined();
    watcher!.callbacks.change(event, filename);
  }
  return { fs, path, cwd, identity, input, observer, opened, emit, scope, watch };
});

const subscribe = Effect.fn("VerificationSourceObserver.test.subscribe")(function* (
  lease: VerificationSourceObserver.ObservationLease,
) {
  const queue = yield* Queue.unbounded<VerificationSourceObserver.ObservationSnapshot>();
  yield* lease.changes.pipe(
    Stream.runForEach((snapshot) => Queue.offer(queue, snapshot)),
    Effect.forkChild,
  );
  const initial = yield* Queue.take(queue);
  return { queue, initial };
});

it.layer(NodeServices.layer)("VerificationSourceObserver", (it) => {
  it.effect("observes edits and edit/revert generations without rereading contents", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const lease = yield* f.observer.start(f.input);
      expect(f.opened.map((watcher) => watcher.directory)).toContain(f.cwd);
      const before = yield* lease.snapshot;
      yield* f.fs.writeFileString(f.path.join(f.cwd, "src/nested/main.ts"), "original");
      f.emit(f.cwd, "src/nested/main.ts");
      yield* f.fs.writeFileString(f.path.join(f.cwd, "src/nested/main.ts"), "edited");
      f.emit(f.cwd, "src/nested/main.ts");
      yield* f.fs.writeFileString(f.path.join(f.cwd, "src/nested/main.ts"), "original");
      f.emit(f.cwd, "src/nested/main.ts");
      expect(yield* lease.snapshot).toEqual({
        mutationGeneration: before.mutationGeneration + 3,
        observationState: "continuous",
      });
    }),
  );

  it.effect("excludes declared output, uncovered files and unrelated Git internals", () =>
    Effect.gen(function* () {
      const f = yield* fixture(true);
      const lease = yield* f.observer.start(f.input);
      for (const name of [
        "src/dist/output.ts",
        "reports/result.json",
        "uncovered.txt",
        ".git/objects/object",
        "src/nested/.git/config",
      ])
        f.emit(f.cwd, name);
      for (const directory of [f.identity.gitDir!, f.identity.gitCommonDir!]) {
        for (const name of ["objects/ab/object", "logs/HEAD", "index.lock", "worktrees/other/HEAD"])
          f.emit(directory, name);
      }
      for (const name of ["HEAD", "index", "config.worktree"])
        f.emit(f.identity.gitCommonDir!, name);
      expect(yield* lease.snapshot).toEqual({
        mutationGeneration: 0,
        observationState: "continuous",
      });
      for (const name of [".env", "package.json", "t3.json"]) f.emit(f.cwd, name);
      expect((yield* lease.snapshot).mutationGeneration).toBe(3);
    }),
  );

  it.effect("observes index, HEAD, refs, common refs and Git configuration changes", () =>
    Effect.gen(function* () {
      const f = yield* fixture(true);
      const lease = yield* f.observer.start(f.input);
      for (const name of ["HEAD", "index", "config.worktree", "refs/heads/main"])
        f.emit(f.identity.gitDir!, name);
      for (const name of [
        "packed-refs",
        "refs/heads/main",
        "config",
        "info/sparse-checkout",
        "sharedindex.abc123",
        "reftable/tables.list",
      ])
        f.emit(f.identity.gitCommonDir!, name);
      expect((yield* lease.snapshot).mutationGeneration).toBe(10);
    }),
  );

  it.effect("does not treat a literal POSIX filename backslash as an excluded output path", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const lease = yield* f.observer.start({
        ...f.input,
        config: { ...f.input.config, inputs: { ...f.input.config.inputs, include: ["**/*"] } },
      });
      f.emit(f.cwd, "src\\dist\\source.ts");
      expect((yield* lease.snapshot).mutationGeneration).toBe(f.path.sep === "/" ? 1 : 0);
    }),
  );

  it.effect("observes refs with internal-looking branch names and submodule source metadata", () =>
    Effect.gen(function* () {
      const f = yield* fixture(true);
      const lease = yield* f.observer.start(f.input);
      for (const name of [
        "refs/heads/logs",
        "refs/heads/objects/topic",
        "refs/heads/worktrees/topic",
        "modules/submodule/HEAD",
        "modules/submodule/index",
        "modules/submodule/refs/heads/logs",
      ])
        f.emit(f.identity.gitCommonDir!, name);
      expect((yield* lease.snapshot).mutationGeneration).toBe(6);
      f.emit(f.identity.gitCommonDir!, "modules/submodule/logs/HEAD");
      f.emit(f.identity.gitCommonDir!, "modules/submodule/objects/ab/object");
      expect((yield* lease.snapshot).mutationGeneration).toBe(6);
    }),
  );

  it.effect("rejects a replaced filesystem identity without retaining native resources", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const lease = yield* f.observer.start({
        ...f.input,
        identity: {
          ...f.identity,
          filesystemIdentity: { ...f.identity.filesystemIdentity, file: "replacement-inode" },
        },
      });
      expect((yield* lease.snapshot).observationState).toBe("unavailable");
      expect(f.opened.every((watcher) => watcher.closed)).toBe(true);
    }),
  );

  it.effect("covers newly created, moved and deleted source directories", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const lease = yield* f.observer.start(f.input);
      f.emit(f.cwd, "src/new", "rename");
      f.emit(f.cwd, "src/new/module.ts");
      f.emit(f.cwd, "src/new", "rename");
      expect((yield* lease.snapshot).mutationGeneration).toBe(3);
    }),
  );

  it.effect("shares one ref-counted observer and retains a lease after another caller closes", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const [first, retained] = yield* Effect.all(
        [f.observer.start(f.input), f.observer.start(f.input)],
        { concurrency: "unbounded" },
      );
      expect(f.opened).toHaveLength(1);
      yield* first.close;
      yield* first.close;
      expect(f.opened[0]?.closed).toBe(false);
      f.emit(f.cwd, "src/main.ts");
      expect((yield* retained.snapshot).mutationGeneration).toBe(1);
      yield* retained.close;
      expect(f.opened.every((watcher) => watcher.closed)).toBe(true);
      expect((yield* retained.snapshot).observationState).toBe("gap");
    }),
  );

  it.effect(
    "uses separate coverage/config revisions and closes every watcher at runtime teardown",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture(true);
        const first = yield* f.observer.start(f.input);
        const same = yield* f.observer.start(f.input);
        const count = f.opened.length;
        yield* f.observer.start({ ...f.input, configRevision: "config-v2" });
        yield* f.observer.start({
          ...f.input,
          config: {
            ...f.input.config,
            inputs: { ...f.input.config.inputs, include: ["package.json"] },
          },
        });
        expect(f.opened.length).toBe(count * 3);
        yield* first.close;
        yield* Scope.close(f.scope, Exit.void);
        expect(f.opened.every((watcher) => watcher.closed)).toBe(true);
        expect((yield* same.snapshot).observationState).toBe("gap");
        const error = yield* f.observer.start(f.input).pipe(Effect.flip);
        expect(error.reason).toBe("closed");
      }),
  );

  it.effect("publishes snapshots and makes a missing filename a sticky gap", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const lease = yield* f.observer.start(f.input);
      const events = yield* subscribe(lease);
      expect(events.initial).toEqual({ mutationGeneration: 0, observationState: "continuous" });
      f.emit(f.cwd, "src/main.ts");
      expect(yield* Queue.take(events.queue)).toEqual({
        mutationGeneration: 1,
        observationState: "continuous",
      });
      f.emit(f.cwd, null);
      expect((yield* Queue.take(events.queue)).observationState).toBe("gap");
      f.emit(f.cwd, "src/main.ts");
      expect((yield* lease.snapshot).observationState).toBe("gap");
      const fresh = yield* f.observer.start(f.input);
      expect((yield* fresh.snapshot).observationState).toBe("continuous");
      expect((yield* lease.snapshot).observationState).toBe("gap");
      yield* fresh.close;
      expect(f.opened[0]?.closed).toBe(false);
      yield* lease.close;
    }),
  );

  it.effect(
    "treats errors, overflow, unexpected close and malformed events as monitoring gaps",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        for (const fail of [
          (callbacks: VerificationSourceObserver.VerificationWatchCallbacks) => callbacks.error(),
          (callbacks: VerificationSourceObserver.VerificationWatchCallbacks) => callbacks.close(),
          (callbacks: VerificationSourceObserver.VerificationWatchCallbacks) =>
            callbacks.change("overflow", "src/main.ts"),
          (callbacks: VerificationSourceObserver.VerificationWatchCallbacks) =>
            callbacks.change("change", "../escape"),
        ]) {
          const lease = yield* f.observer.start(f.input);
          fail(f.opened.at(-1)!.callbacks);
          expect((yield* lease.snapshot).observationState).toBe("gap");
          yield* lease.close;
        }
      }),
  );

  it.effect("unsupported recursive observation and exhausted resources are unavailable", () =>
    Effect.gen(function* () {
      const f = yield* fixture(true);
      for (const failure of ["ERR_FEATURE_UNAVAILABLE_ON_PLATFORM", "ENOSPC", "EMFILE"]) {
        const context = yield* Layer.build(VerificationSourceObserver.layer).pipe(
          Effect.provideService(VerificationSourceObserver.VerificationWatch, () => {
            throw Object.assign(new Error("native watch failed"), { code: failure });
          }),
        );
        const observer = Context.get(
          context,
          VerificationSourceObserver.VerificationSourceObserver,
        );
        const lease = yield* observer.start(f.input);
        expect((yield* lease.snapshot).observationState).toBe("unavailable");
      }
      const context = yield* Layer.build(VerificationSourceObserver.layer).pipe(
        Effect.provideService(VerificationSourceObserver.VerificationWatch, f.watch),
        Effect.provideService(VerificationSourceObserver.VerificationObserverLimits, {
          maxWatchers: 1,
        }),
      );
      const observer = Context.get(context, VerificationSourceObserver.VerificationSourceObserver);
      const lease = yield* observer.start(f.input);
      expect((yield* lease.snapshot).observationState).toBe("unavailable");
      expect(f.opened.every((watcher) => watcher.closed)).toBe(true);
    }),
  );

  it.effect(
    "native callbacks received during establishment are reflected in the first snapshot",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const context = yield* Layer.build(VerificationSourceObserver.layer).pipe(
          Effect.provideService(
            VerificationSourceObserver.VerificationWatch,
            (directory, callbacks) => {
              const close = f.watch(directory, callbacks);
              callbacks.change("change", "src/main.ts");
              return close;
            },
          ),
        );
        const observer = Context.get(
          context,
          VerificationSourceObserver.VerificationSourceObserver,
        );
        expect(yield* (yield* observer.start(f.input)).snapshot).toEqual({
          mutationGeneration: 1,
          observationState: "continuous",
        });
      }),
  );

  it.effect("interrupting establishment closes native observers and permits a fresh attempt", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const entered = yield* Deferred.make<void>();
      const blocked = yield* Deferred.make<string>();
      let block = true;
      const fileSystem = FileSystem.FileSystem.of({
        ...f.fs,
        realPath: (filename) =>
          filename === f.cwd && block
            ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(blocked)))
            : f.fs.realPath(filename),
      });
      const context = yield* Layer.build(VerificationSourceObserver.layer).pipe(
        Effect.provideService(FileSystem.FileSystem, fileSystem),
        Effect.provideService(VerificationSourceObserver.VerificationWatch, f.watch),
      );
      const observer = Context.get(context, VerificationSourceObserver.VerificationSourceObserver);
      const pending = yield* Effect.forkChild(observer.start(f.input));
      yield* Deferred.await(entered);
      expect(f.opened).toHaveLength(1);
      yield* Fiber.interrupt(pending);
      expect(f.opened.every((watcher) => watcher.closed)).toBe(true);
      block = false;
      const fresh = yield* observer.start(f.input);
      expect((yield* fresh.snapshot).observationState).toBe("continuous");
      yield* fresh.close;
    }),
  );

  it.effect("closing a lease ends its streams while other retained subscribers stay live", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const lease = yield* f.observer.start(f.input);
      const retained = yield* f.observer.start(f.input);
      const registered = yield* Deferred.make<void>();
      const completed = yield* Deferred.make<void>();
      yield* lease.changes.pipe(
        Stream.runForEach(() => Deferred.succeed(registered, undefined)),
        Effect.andThen(Deferred.succeed(completed, undefined)),
        Effect.forkChild,
      );
      yield* Deferred.await(registered);
      const events = yield* subscribe(retained);
      yield* lease.close;
      yield* Deferred.await(completed);
      f.emit(f.cwd, "src/main.ts");
      expect((yield* Queue.take(events.queue)).mutationGeneration).toBe(1);
      expect(f.opened[0]?.closed).toBe(false);
      yield* retained.close;
    }),
  );

  it.effect("waits for registration acknowledgement before returning continuous observation", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const establishing = yield* Deferred.make<void>();
      const established = yield* Deferred.make<"continuous" | "unavailable">();
      const returned = yield* Deferred.make<void>();
      const context = yield* Layer.build(VerificationSourceObserver.layer).pipe(
        Effect.provideService(
          VerificationSourceObserver.VerificationWatch,
          (directory, callbacks) => ({
            ...f.watch(directory, callbacks),
            ready: Deferred.succeed(establishing, undefined).pipe(
              Effect.andThen(Deferred.await(established)),
            ),
          }),
        ),
      );
      const observer = Context.get(context, VerificationSourceObserver.VerificationSourceObserver);
      const pending = yield* observer.start(f.input).pipe(
        Effect.tap(() => Deferred.succeed(returned, undefined)),
        Effect.forkChild,
      );
      yield* Deferred.await(establishing);
      expect(yield* Deferred.isDone(returned)).toBe(false);
      f.emit(f.cwd, "src/nested/main.ts");
      yield* Deferred.succeed(established, "continuous");
      const lease = yield* Fiber.join(pending);
      expect(yield* lease.snapshot).toEqual({
        mutationGeneration: 1,
        observationState: "continuous",
      });
      yield* lease.close;
    }),
  );

  it.effect(
    "a native gap during registration cannot be cleared by a readiness acknowledgement",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const context = yield* Layer.build(VerificationSourceObserver.layer).pipe(
          Effect.provideService(
            VerificationSourceObserver.VerificationWatch,
            (directory, callbacks) => ({
              ...f.watch(directory, callbacks),
              ready: Effect.sync(() => {
                callbacks.change("overflow", null);
                return "continuous" as const;
              }),
            }),
          ),
        );
        const observer = Context.get(
          context,
          VerificationSourceObserver.VerificationSourceObserver,
        );
        const lease = yield* observer.start(f.input);
        expect((yield* lease.snapshot).observationState).toBe("gap");
        yield* lease.close;
      }),
  );

  it.effect("runtime teardown cancels registration and closes every allocated watcher", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const establishing = yield* Deferred.make<void>();
      const established = yield* Deferred.make<"continuous" | "unavailable">();
      const context = yield* Layer.build(VerificationSourceObserver.layer).pipe(
        Effect.provideService(Scope.Scope, f.scope),
        Effect.provideService(
          VerificationSourceObserver.VerificationWatch,
          (directory, callbacks) => ({
            ...f.watch(directory, callbacks),
            ready: Deferred.succeed(establishing, undefined).pipe(
              Effect.andThen(Deferred.await(established)),
            ),
          }),
        ),
      );
      const observer = Context.get(context, VerificationSourceObserver.VerificationSourceObserver);
      const pending = yield* observer.start(f.input).pipe(Effect.flip, Effect.forkChild);
      yield* Deferred.await(establishing);
      yield* Scope.close(f.scope, Exit.void);
      expect((yield* Fiber.join(pending)).reason).toBe("closed");
      expect(f.opened.every((watcher) => watcher.closed)).toBe(true);
    }),
  );

  it.effect(
    "the native adapter cannot claim continuity without registration and loss guarantees",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const filename = f.path.join(f.cwd, "src/nested/main.ts");
        yield* f.fs.writeFileString(filename, "before");
        const context = yield* Layer.build(VerificationSourceObserver.layer);
        const observer = Context.get(
          context,
          VerificationSourceObserver.VerificationSourceObserver,
        );
        const lease = yield* observer.start(f.input);
        const events = yield* subscribe(lease);
        expect(events.initial.observationState).toBe("unavailable");
        yield* f.fs.writeFileString(filename, "after");
        expect((yield* lease.snapshot).observationState).toBe("unavailable");
        yield* lease.close;
      }),
  );
});
