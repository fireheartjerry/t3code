// @effect-diagnostics nodeBuiltinImport:off -- FileSystem.watch does not expose recursive watching, missing filenames, or native watcher errors.
// @effect-diagnostics preferSchemaOverJson:off -- JSON only frames an in-memory observer-sharing key, not decoded data.
import * as NodeFS from "node:fs";
import type { CheckoutIdentity, VerificationConfig } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import { compileVerificationInputPolicy } from "./VerificationInputs.ts";

export interface ObservationSnapshot {
  readonly mutationGeneration: number;
  readonly observationState: "continuous" | "gap" | "unavailable";
}

export interface ObservationLease {
  readonly snapshot: Effect.Effect<ObservationSnapshot>;
  readonly changes: Stream.Stream<ObservationSnapshot>;
  readonly close: Effect.Effect<void>;
}

export interface VerificationWatchCallbacks {
  readonly change: (event: string, filename: string | null) => void;
  readonly error: () => void;
  readonly close: () => void;
}

export interface VerificationWatchHandle {
  readonly close: () => void;
  /** Continuous certifies established registration and notification of any subsequent observation loss. */
  readonly ready: Effect.Effect<"continuous" | "unavailable">;
}

/** Only the native filesystem boundary is replaceable; returning a handle alone does not certify readiness. */
export const VerificationWatch = Context.Reference<
  (directory: string, callbacks: VerificationWatchCallbacks) => VerificationWatchHandle
>("t3/verification/VerificationWatch", { defaultValue: () => watchDirectory });

function watchDirectory(
  directory: string,
  callbacks: VerificationWatchCallbacks,
): VerificationWatchHandle {
  // fs.watch cannot certify continuity: macOS asynchronously reschedules and drops loss flags;
  // Linux omits global inotify overflow. Keep best-effort events without current-pass authority.
  // https://github.com/libuv/libuv/blob/v1.53.0/src/unix/fsevents.c
  // https://github.com/libuv/libuv/blob/v1.53.0/src/unix/linux.c
  const watcher = NodeFS.watch(directory, { recursive: true, encoding: "utf8" }, callbacks.change);
  watcher.on("error", callbacks.error);
  watcher.on("close", callbacks.close);
  return { close: () => watcher.close(), ready: Effect.succeed("unavailable") };
}

export const VerificationObserverLimits = Context.Reference<{ readonly maxWatchers: number }>(
  "t3/verification/VerificationObserverLimits",
  { defaultValue: () => ({ maxWatchers: 256 }) },
);

export class VerificationSourceObserverError extends Schema.TaggedError<VerificationSourceObserverError>()(
  "VerificationSourceObserverError",
  {
    reason: Schema.Literals(["invalid-inputs", "invalid-root", "invalid-limits", "closed"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Cannot observe verification source (${this.reason}).`;
  }
}

export class VerificationSourceObserver extends Context.Service<
  VerificationSourceObserver,
  {
    readonly start: (input: {
      readonly identity: CheckoutIdentity;
      readonly config: VerificationConfig;
      readonly configRevision: string;
    }) => Effect.Effect<ObservationLease, VerificationSourceObserverError>;
  }
>()("t3/verification/VerificationSourceObserver") {}

interface Subscriber {
  readonly publish: (snapshot: ObservationSnapshot) => void;
  readonly end: () => void;
}

interface Observation {
  readonly key: string;
  readonly stopWatching: Set<() => void>;
  readonly subscribers: Set<Subscriber>;
  readonly readiness: Array<VerificationWatchHandle["ready"]>;
  snapshot: ObservationSnapshot;
  references: number;
  closed: boolean;
}

const make = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const watch = yield* VerificationWatch;
  const limits = yield* VerificationObserverLimits;
  const semaphore = yield* Semaphore.make(1);
  const shutdown = yield* Deferred.make<void>();
  const current = new Map<string, Observation>();
  // Superseded observations can still belong to retained evidence leases.
  const observations = new Set<Observation>();
  let nativeWatchers = 0;
  let closed = false;

  // Native adapter callbacks update synchronously so snapshots cannot lag behind queued events.
  function publish(observation: Observation): void {
    for (const subscriber of observation.subscribers) subscriber.publish(observation.snapshot);
  }

  function gap(observation: Observation, state: "gap" | "unavailable" = "gap"): void {
    if (observation.closed || observation.snapshot.observationState === "unavailable") return;
    if (observation.snapshot.observationState === state) return;
    observation.snapshot = { ...observation.snapshot, observationState: state };
    publish(observation);
  }

  function mutation(observation: Observation): void {
    if (observation.closed) return;
    if (observation.snapshot.mutationGeneration === Number.MAX_SAFE_INTEGER) {
      gap(observation);
      return;
    }
    observation.snapshot = {
      ...observation.snapshot,
      mutationGeneration: observation.snapshot.mutationGeneration + 1,
    };
    publish(observation);
  }

  function dispose(observation: Observation): void {
    if (observation.closed) return;
    gap(observation);
    observation.closed = true;
    observation.readiness.length = 0;
    for (const stop of observation.stopWatching) stop();
    observation.stopWatching.clear();
    for (const subscriber of observation.subscribers) subscriber.end();
    observation.subscribers.clear();
    observations.delete(observation);
    if (current.get(observation.key) === observation) current.delete(observation.key);
  }

  function lease(observation: Observation): ObservationLease {
    observation.references++;
    let released = false;
    const subscribers = new Set<Subscriber>();
    const snapshot = Effect.sync((): ObservationSnapshot =>
      released ? { ...observation.snapshot, observationState: "gap" } : observation.snapshot,
    );
    const changes = Stream.callback<ObservationSnapshot>(
      (mailbox) =>
        Effect.acquireRelease(
          Effect.sync(() => {
            const subscriber: Subscriber = {
              publish: (value) => {
                Queue.offerUnsafe(mailbox, value);
              },
              end: () => {
                Queue.endUnsafe(mailbox);
              },
            };
            // Registration and initial emission are atomic with respect to native callbacks.
            if (!released && !observation.closed) {
              subscribers.add(subscriber);
              observation.subscribers.add(subscriber);
            }
            subscriber.publish(
              released
                ? { ...observation.snapshot, observationState: "gap" }
                : observation.snapshot,
            );
            if (released || observation.closed) subscriber.end();
            return subscriber;
          }),
          (subscriber) =>
            Effect.sync(() => {
              subscribers.delete(subscriber);
              observation.subscribers.delete(subscriber);
            }),
        ),
      // Coalescing delivery is safe: the monotonic generation and sticky gap retain every invalidation.
      { bufferSize: 1, strategy: "sliding" },
    );
    const close = Effect.sync(() => {
      if (released) return;
      released = true;
      for (const subscriber of subscribers) {
        subscriber.publish({ ...observation.snapshot, observationState: "gap" });
        subscriber.end();
        observation.subscribers.delete(subscriber);
      }
      subscribers.clear();
      observation.references--;
      if (observation.references === 0) dispose(observation);
    });
    return { snapshot, changes, close };
  }

  function install(
    observation: Observation,
    directory: string,
    onChange: VerificationWatchCallbacks["change"],
  ): boolean {
    if (nativeWatchers >= limits.maxWatchers) {
      gap(observation, "unavailable");
      return false;
    }
    try {
      const handle = watch(directory, {
        change: onChange,
        error: () => gap(observation),
        close: () => gap(observation),
      });
      observation.readiness.push(handle.ready);
      nativeWatchers++;
      let stopped = false;
      observation.stopWatching.add(() => {
        if (stopped) return;
        stopped = true;
        nativeWatchers--;
        handle.close();
      });
      return true;
    } catch {
      // Unsupported recursion, inaccessible paths, ENOSPC and EMFILE cannot imply continuity.
      gap(observation, "unavailable");
      return false;
    }
  }

  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      yield* Effect.sync(() => {
        closed = true;
        for (const observation of observations) dispose(observation);
      });
      yield* Deferred.succeed(shutdown, undefined);
    }),
  );

  const start: VerificationSourceObserver["Service"]["start"] = Effect.fn(
    "VerificationSourceObserver.start",
  )(
    function* (input) {
      if (closed) return yield* new VerificationSourceObserverError({ reason: "closed" });
      if (!Number.isSafeInteger(limits.maxWatchers) || limits.maxWatchers < 1)
        return yield* new VerificationSourceObserverError({ reason: "invalid-limits" });
      const policy = yield* compileVerificationInputPolicy(input.config.inputs).pipe(
        Effect.mapError(
          (cause) => new VerificationSourceObserverError({ reason: "invalid-inputs", cause }),
        ),
      );
      const identity = input.identity;
      const root = identity.canonicalCwd;
      if (input.configRevision.length === 0)
        return yield* new VerificationSourceObserverError({ reason: "invalid-inputs" });
      if (!path.isAbsolute(root))
        return yield* new VerificationSourceObserverError({ reason: "invalid-root" });
      const key = JSON.stringify([
        identity.environmentId,
        identity.checkoutKey,
        root,
        identity.filesystemIdentity.device,
        identity.filesystemIdentity.file,
        identity.gitDir,
        identity.gitCommonDir,
        policy.revision,
        input.configRevision,
      ]);
      const existing = current.get(key);
      if (existing && !existing.closed && existing.snapshot.observationState === "continuous")
        return lease(existing);

      const observation: Observation = {
        key,
        stopWatching: new Set(),
        subscribers: new Set(),
        readiness: [],
        snapshot: { mutationGeneration: 0, observationState: "continuous" },
        references: 0,
        closed: false,
      };
      observations.add(observation);
      current.set(key, observation);
      // Allocate every observer before source inspection; await each adapter's establishment below.
      const gitDirectories = [...new Set([identity.gitDir, identity.gitCommonDir])].filter(
        (directory): directory is string => directory !== null,
      );
      const gitWithinRoot = gitDirectories.map((directory) =>
        path.relative(root, directory).split(path.sep).join("/"),
      );
      function workspaceChange(event: string, filename: string | null): void {
        const relative = eventPath(event, filename, path.sep);
        if (relative === null) {
          gap(observation);
          return;
        }
        if (relative === ".git" || (relative.endsWith("/.git") && event === "rename")) {
          mutation(observation);
          gap(observation);
          return;
        }
        if (event === "rename" && relative === path.basename(root)) {
          mutation(observation);
          gap(observation);
          return;
        }
        if (
          gitWithinRoot.some(
            (gitPath) => relative === gitPath || relative.startsWith(`${gitPath}/`),
          )
        )
          return;
        if (policy.excluded(relative)) return;
        if (policy.includes(relative) || (event === "rename" && policy.mayContain(relative)))
          mutation(observation);
      }
      function gitChange(directory: string, event: string, filename: string | null): void {
        const relative = eventPath(event, filename, path.sep);
        if (relative === null) {
          gap(observation);
          return;
        }
        if (event === "rename" && relative === path.basename(directory)) {
          mutation(observation);
          gap(observation);
          return;
        }
        if (isSourceMetadata(relative, directory === identity.gitDir)) mutation(observation);
      }
      let available = install(observation, root, workspaceChange);
      for (const directory of gitDirectories) {
        if (available)
          available = install(observation, directory, (event, filename) =>
            gitChange(directory, event, filename),
          );
      }
      if (!available) {
        for (const stop of observation.stopWatching) stop();
        observation.stopWatching.clear();
      }
      // Parent/runtime teardown can race asynchronous canonicalization. Nothing may survive it.
      const validRoot = yield* Effect.gen(function* () {
        if ((yield* fs.realPath(root)) !== root) return false;
        const sameDirectory = yield* Effect.try({
          // FileSystem.stat uses numeric inode IDs, which lose precision on some filesystems.
          try: () => {
            const info = NodeFS.statSync(root, { bigint: true });
            return (
              info.isDirectory() &&
              info.dev.toString() === identity.filesystemIdentity.device &&
              info.ino.toString() === identity.filesystemIdentity.file
            );
          },
          catch: (cause) => new VerificationSourceObserverError({ reason: "invalid-root", cause }),
        });
        if (!sameDirectory) return false;
        for (const directory of gitDirectories)
          if (!path.isAbsolute(directory) || (yield* fs.realPath(directory)) !== directory)
            return false;
        if (available) {
          for (const ready of observation.readiness) {
            if ((yield* ready) !== "continuous") {
              gap(observation, "unavailable");
              break;
            }
          }
          observation.readiness.length = 0;
        }
        return true;
      }).pipe(
        Effect.orElseSucceed(() => false),
        Effect.onExit((exit) =>
          Exit.isSuccess(exit) ? Effect.void : Effect.sync(() => dispose(observation)),
        ),
      );
      if (!validRoot) {
        gap(observation, "unavailable");
        for (const stop of observation.stopWatching) stop();
        observation.stopWatching.clear();
      }
      if (closed) {
        dispose(observation);
        return yield* new VerificationSourceObserverError({ reason: "closed" });
      }
      return lease(observation);
    },
    semaphore.withPermit,
    (effect) =>
      Effect.raceFirst(
        effect,
        Deferred.await(shutdown).pipe(
          Effect.andThen(Effect.fail(new VerificationSourceObserverError({ reason: "closed" }))),
        ),
      ),
  );

  return VerificationSourceObserver.of({ start });
});

function eventPath(event: string, filename: string | null, separator: string): string | null {
  if ((event !== "change" && event !== "rename") || !filename || filename.includes("\0"))
    return null;
  // Backslashes are legal filename characters on POSIX, not output-directory separators.
  const relative = separator === "\\" ? filename.replaceAll("\\", "/") : filename;
  if (
    /^(?:\/|[A-Za-z]:)/.test(relative) ||
    relative.split("/").some((part) => part === ".." || part === "." || part === "")
  )
    return null;
  return relative;
}

function isSourceMetadata(relative: string, ownWorktree: boolean): boolean {
  const pieces = relative.split("/");
  // Linked worktrees share objects and logs, but only their own HEAD/index and common refs matter.
  // These names are legal within refs, so filter the ref namespace before internal directories.
  if (relative.startsWith("refs/")) return true;
  if (relative.startsWith("reftable/") || /^sharedindex\.[a-f0-9]+$/.test(relative)) return true;
  if (relative.startsWith("modules/")) {
    if (pieces.includes("refs") || pieces.includes("reftable")) return true;
    if (/\/(?:objects|logs)\//.test(relative)) return false;
    const name = pieces.at(-1);
    return name === "HEAD" || name === "index" || name === "config" || name === "packed-refs";
  }
  if (relative === "HEAD" || relative === "index" || relative === "config.worktree")
    return ownWorktree;
  return [
    "packed-refs",
    "config",
    "shallow",
    "commondir",
    "gitdir",
    "refs",
    "reftable",
    "info",
    "info/sparse-checkout",
    "info/attributes",
  ].includes(relative);
}

export const layer = Layer.effect(VerificationSourceObserver, make);
