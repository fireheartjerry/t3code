// @effect-diagnostics preferSchemaOverJson:off -- JSON only frames hash inputs; it is not a decoder or persisted plaintext.
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Effect Crypto has no incremental hash API.
import * as NodeCrypto from "node:crypto";
import {
  VerificationInputPattern,
  type CheckoutIdentity,
  type VerificationConfig,
  type VerificationCoverage,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import { VERIFICATION_MAX_BYTES, VERIFICATION_MAX_FILES } from "./VerificationLimits.ts";

/** Discovery and Git output have separate budgets so uncovered trees cannot cause unbounded work. */
export const VerificationInputLimits = Context.Reference<{
  readonly maxFiles: number;
  readonly maxBytes: number;
  readonly maxEntries: number;
  readonly maxDepth: number;
  readonly maxGitOutputBytes: number;
  readonly maxPatterns: number;
  readonly maxPatternLength: number;
}>("t3/verification/VerificationInputLimits", {
  defaultValue: () => ({
    maxFiles: VERIFICATION_MAX_FILES,
    maxBytes: VERIFICATION_MAX_BYTES,
    maxEntries: VERIFICATION_MAX_FILES * 2,
    maxDepth: 128,
    maxGitOutputBytes: 16 * 1024 * 1024,
    maxPatterns: 256,
    maxPatternLength: 1_024,
  }),
});

export class VerificationInputError extends Schema.TaggedError<VerificationInputError>()(
  "VerificationInputError",
  {
    reason: Schema.Literals([
      "invalid-pattern",
      "invalid-root",
      "invalid-limits",
      "unreadable-root",
      "non-git-unsupported",
    ]),
  },
) {
  override get message(): string {
    return `Cannot resolve verification inputs (${this.reason}).`;
  }
}

type Pattern = {
  readonly source: string;
  readonly matches: (relativePath: string, partial?: boolean) => boolean;
};

export interface VerificationInputPolicy {
  readonly include: ReadonlyArray<string>;
  readonly exclude: ReadonlyArray<string>;
  readonly revision: string;
  readonly includes: (relativePath: string) => boolean;
  readonly mayContain: (relativeDirectory: string) => boolean;
  readonly excluded: (relativePath: string) => boolean;
}

export interface VerificationInputEntry {
  readonly relativePath: string;
  readonly absolutePath: string;
  readonly info: FileSystem.File.Info;
  readonly linkTarget: string | null;
}

export interface VerificationInputSet {
  readonly identity: CheckoutIdentity;
  readonly config: VerificationConfig;
  readonly policy: VerificationInputPolicy;
  readonly paths: ReadonlyArray<string>;
  readonly entries: ReadonlyArray<VerificationInputEntry>;
  readonly directories: ReadonlyArray<string>;
  readonly coverage: VerificationCoverage;
}

type SegmentToken = "*" | "?" | { readonly literal: string };

// The contract permits literal paths, *, ?, and whole-segment **. Dynamic programming keeps
// repeated wildcards bounded; a backtracking regexp can take exponential time on ordinary filenames.
function compileGlob(pattern: string): Pattern["matches"] {
  const compiled = pattern.split("/").map((segment) => {
    if (segment === "**") return "**";
    if (segment.includes("**")) throw new Error("Globstar must occupy a segment");
    return [...segment].map((character): SegmentToken =>
      character === "*" || character === "?" ? character : { literal: character },
    );
  });
  return (relative, partial = false) => {
    const segments = relative.split("/");
    let reachable = new Set([0]);
    for (const segment of compiled) {
      if (segment === "**") {
        const first = Math.min(...reachable);
        reachable = new Set(
          Array.from({ length: segments.length - first + 1 }, (_, index) => first + index),
        );
      } else {
        const next = new Set<number>();
        for (const offset of reachable) {
          const value = segments[offset];
          if (value === undefined) continue;
          const characters = [...value];
          let previous = Array.from({ length: characters.length + 1 }, () => false);
          previous[0] = true;
          for (const token of segment) {
            const current = Array.from({ length: characters.length + 1 }, () => false);
            if (token === "*") current[0] = previous[0]!;
            for (let index = 1; index <= characters.length; index++) {
              current[index] =
                token === "*"
                  ? previous[index]! || current[index - 1]!
                  : previous[index - 1]! &&
                    (token === "?" || token.literal === characters[index - 1]);
            }
            previous = current;
          }
          if (previous[characters.length]) next.add(offset + 1);
        }
        reachable = next;
      }
      if (reachable.size === 0) return false;
      if (partial && reachable.has(segments.length)) return true;
    }
    // A matching directory includes its descendants, anchored at the checkout root.
    return reachable.size > 0;
  };
}

const decodeInputPatterns = Schema.decodeEffect(Schema.Array(VerificationInputPattern));

export const compileVerificationInputPolicy = Effect.fn("VerificationInputs.compilePolicy")(
  function* (inputs: VerificationConfig["inputs"]) {
    const limits = yield* VerificationInputLimits;
    if (Object.values(limits).some((value) => !Number.isSafeInteger(value) || value < 1)) {
      return yield* new VerificationInputError({ reason: "invalid-limits" });
    }
    if (inputs.include.length + inputs.exclude.length > limits.maxPatterns) {
      return yield* new VerificationInputError({ reason: "invalid-pattern" });
    }
    const patterns = yield* decodeInputPatterns([...inputs.include, ...inputs.exclude]).pipe(
      Effect.mapError(() => new VerificationInputError({ reason: "invalid-pattern" })),
    );
    const compiled = yield* Effect.try({
      try: () =>
        patterns.map((raw): Pattern => {
          if (raw.length > limits.maxPatternLength) throw new Error("Pattern length limit");
          const source = raw.replaceAll("\\", "/");
          if (!source || source.split("/").some((segment) => segment === "" || segment === ".")) {
            throw new Error("Invalid relative pattern");
          }
          return {
            source,
            matches: compileGlob(source),
          };
        }),
      catch: () => new VerificationInputError({ reason: "invalid-pattern" }),
    });
    const included = compiled.slice(0, inputs.include.length);
    const excluded = compiled.slice(inputs.include.length);
    const isExcluded = (relative: string) =>
      relative.split("/").some((segment) => segment.toLowerCase() === ".git") ||
      excluded.some((pattern) => pattern.matches(relative));
    const include = [...new Set(included.map((pattern) => pattern.source))].sort();
    const exclude = [...new Set(excluded.map((pattern) => pattern.source))].sort();
    return {
      include,
      exclude,
      revision: NodeCrypto.createHash("sha256")
        .update(JSON.stringify(["verification-inputs-v1", include, exclude, inputs.nonGit]))
        .digest("hex"),
      includes: (relative: string) =>
        !isExcluded(relative) && included.some((pattern) => pattern.matches(relative)),
      excluded: isExcluded,
      mayContain: (relative: string) =>
        !isExcluded(relative) && included.some((pattern) => pattern.matches(relative, true)),
    } satisfies VerificationInputPolicy;
  },
);

export const resolveVerificationInputs = Effect.fn("VerificationInputs.resolve")(function* (input: {
  readonly identity: CheckoutIdentity;
  readonly config: VerificationConfig;
}): Effect.fn.Return<
  VerificationInputSet,
  VerificationInputError,
  FileSystem.FileSystem | Path.Path
> {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const limits = yield* VerificationInputLimits;
  if (Object.values(limits).some((value) => !Number.isSafeInteger(value) || value < 1)) {
    return yield* new VerificationInputError({ reason: "invalid-limits" });
  }
  const policy = yield* compileVerificationInputPolicy(input.config.inputs);
  if (input.identity.gitDir === null && input.config.inputs.nonGit === "unsupported") {
    return yield* new VerificationInputError({ reason: "non-git-unsupported" });
  }
  const root = yield* fs
    .realPath(input.identity.canonicalCwd)
    .pipe(Effect.mapError(() => new VerificationInputError({ reason: "unreadable-root" })));
  if (root !== input.identity.canonicalCwd || !path.isAbsolute(root)) {
    return yield* new VerificationInputError({ reason: "invalid-root" });
  }
  const rootInfo = yield* fs
    .stat(root)
    .pipe(Effect.mapError(() => new VerificationInputError({ reason: "unreadable-root" })));
  if (rootInfo.type !== "Directory") {
    return yield* new VerificationInputError({ reason: "invalid-root" });
  }
  const entries: VerificationInputEntry[] = [];
  const directories: string[] = [];
  const gaps = new Set<string>();
  const matched = new Set<string>();
  const matchers = policy.include.map((source) => ({
    source,
    matches: compileGlob(source),
  }));
  const pending = [{ relative: "", depth: 0 }];
  let visited = 0;
  const isGitPath = (absolute: string) =>
    [input.identity.gitDir, input.identity.gitCommonDir].some(
      (gitDir) =>
        gitDir !== null && (absolute === gitDir || absolute.startsWith(`${gitDir}${path.sep}`)),
    );
  while (pending.length > 0) {
    const directory = pending.pop()!;
    if (directory.depth > limits.maxDepth) {
      gaps.add("directory-depth-limit");
      continue;
    }
    const absoluteDirectory = path.join(root, directory.relative);
    const actualDirectory = yield* fs
      .realPath(absoluteDirectory)
      .pipe(Effect.orElseSucceed(() => null));
    if (actualDirectory !== absoluteDirectory) {
      gaps.add("input-changed-during-capture");
      continue;
    }
    const names = yield* fs.readDirectory(absoluteDirectory).pipe(
      Effect.catch(() => {
        gaps.add("unreadable-directory");
        return Effect.succeed([] as string[]);
      }),
    );
    // FileSystem exposes a non-recursive per-directory listing; don't retain oversized listings.
    const candidates = names.filter((name) => {
      const relative = directory.relative ? `${directory.relative}/${name}` : name;
      return (
        !policy.excluded(relative) &&
        !isGitPath(path.join(root, relative)) &&
        policy.mayContain(relative)
      );
    });
    if (candidates.length > limits.maxEntries - visited) {
      gaps.add("entry-limit");
      break;
    }
    visited += candidates.length;
    for (const name of candidates.sort()) {
      const relative = directory.relative ? `${directory.relative}/${name}` : name;
      const absolute = path.join(root, relative);
      if (policy.excluded(relative) || isGitPath(absolute) || !policy.mayContain(relative))
        continue;
      const linkTarget = yield* fs.readLink(absolute).pipe(Effect.orElseSucceed(() => null));
      const info = yield* fs.stat(absolute).pipe(Effect.orElseSucceed(() => null));
      if (info === null) {
        gaps.add(linkTarget === null ? "unreadable-input" : "unreadable-symlink");
        continue;
      }
      if (info.type !== "Directory" && !policy.includes(relative)) continue;
      const real = yield* fs.realPath(absolute).pipe(Effect.orElseSucceed(() => null));
      if (real === null) {
        gaps.add(linkTarget === null ? "unreadable-input" : "unreadable-symlink");
        continue;
      }
      const resolvedRelative = path.relative(root, real).replaceAll("\\", "/");
      const escaped =
        resolvedRelative === ".." ||
        resolvedRelative.startsWith("../") ||
        path.isAbsolute(resolvedRelative);
      if (
        escaped ||
        isGitPath(real) ||
        (linkTarget !== null && !policy.includes(resolvedRelative))
      ) {
        gaps.add(escaped ? "escaped-symlink" : "symlink-target-outside-coverage");
        continue;
      }
      if (linkTarget === null && real !== absolute) {
        gaps.add("input-changed-during-capture");
        continue;
      }
      for (const matcher of matchers) if (matcher.matches(relative)) matched.add(matcher.source);
      if (info.type === "Directory" && linkTarget === null) {
        directories.push(relative);
        pending.push({ relative, depth: directory.depth + 1 });
      } else if (policy.includes(relative)) {
        if (info.type !== "File" && linkTarget === null) {
          gaps.add("unsupported-file-type");
          continue;
        }
        if (entries.length === limits.maxFiles) {
          gaps.add("file-limit");
          pending.length = 0;
          break;
        }
        entries.push({ relativePath: relative, absolutePath: absolute, info, linkTarget });
      }
    }
  }
  if (policy.include.length === 0) gaps.add("no-inputs-declared");
  if (matched.size !== policy.include.length) gaps.add("missing-input");
  entries.sort((a, b) =>
    a.relativePath < b.relativePath ? -1 : a.relativePath > b.relativePath ? 1 : 0,
  );
  return {
    ...input,
    policy,
    entries,
    directories: directories.sort(),
    paths: entries.map((entry) => entry.relativePath),
    coverage: {
      complete: gaps.size === 0,
      policyRevision: policy.revision,
      gaps: [...gaps].sort(),
    },
  };
});
