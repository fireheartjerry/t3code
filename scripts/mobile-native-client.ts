import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { isCommandAvailable, resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Console from "effect/Console";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { Argument, Command } from "effect/cli";
import { FetchHttpClient, HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

export type NativePlatform = "ios" | "android";
const NativeClientRecord = Schema.Struct({ fingerprint: Schema.String, binary: Schema.String });
const encodeRecord = Schema.encodeEffect(Schema.fromJsonString(NativeClientRecord));
const decodeRecord = Schema.decodeUnknownEffect(Schema.fromJsonString(NativeClientRecord));
const encodeOutput = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const isNotLink = Schema.is(Schema.Struct({ code: Schema.Literal("EINVAL") }));
const decodeSimulators = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({
      devices: Schema.Record(
        Schema.String,
        Schema.Array(Schema.Struct({ udid: Schema.String, state: Schema.String })),
      ),
    }),
  ),
);
export type NativeClientRecord = typeof NativeClientRecord.Type;
export type NativeClientStatus = "compatible" | "missing" | "unknown" | "stale";
export class NativeClientError extends Schema.TaggedError<NativeClientError>()(
  "NativeClientError",
  { message: Schema.String },
) {}

export function clientStatus(
  fingerprint: string,
  binary: string | null,
  record: NativeClientRecord | null,
): NativeClientStatus {
  if (binary === null) return "missing";
  if (!record || record.binary !== binary) return "unknown";
  return record.fingerprint === fingerprint ? "compatible" : "stale";
}

export type ArtifactSource = "local" | "remote" | "build";

/** Keep native sources stable during ensure, as with a normal build; endpoint checks reject detected edits. */
export const ensureClient = Effect.fn("ensureClient")(function* <E, R, E2, R2>(operations: {
  fingerprint: Effect.Effect<string, E, R>;
  installedBinary: Effect.Effect<string | null, E, R>;
  readRecord: Effect.Effect<NativeClientRecord | null, E, R>;
  install: (fingerprint: string) => Effect.Effect<ArtifactSource, E, R>;
  saveRecord: (record: NativeClientRecord) => Effect.Effect<void, E2, R2>;
}) {
  const fingerprint = yield* operations.fingerprint;
  const status = clientStatus(
    fingerprint,
    yield* operations.installedBinary,
    yield* operations.readRecord,
  );
  const verifyInputs = Effect.gen(function* () {
    if ((yield* operations.fingerprint) !== fingerprint) {
      return yield* new NativeClientError({
        message:
          "Native inputs changed during verification. Run ensure again; this build was not recorded.",
      });
    }
  });
  yield* verifyInputs;
  if (status === "compatible") {
    return { status, installed: false, fingerprint };
  }
  const source = yield* operations.install(fingerprint);
  const binary = yield* operations.installedBinary;
  if (binary === null)
    return yield* new NativeClientError({
      message: "Install finished but the development client is not on the device.",
    });
  yield* verifyInputs;
  yield* operations.saveRecord({ fingerprint, binary });
  return { status: "compatible" as const, installed: true, source, fingerprint };
});

const digest = Effect.fn("nativeClient.digest")(function* (value: string | Uint8Array) {
  const crypto = yield* Crypto.Crypto;
  const bytes = yield* crypto.digest(
    "SHA-256",
    typeof value === "string" ? new TextEncoder().encode(value) : value,
  );
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
});

/** Hash fixed-size chunks to bound memory, including resources and symlink targets. */
export const hashBundle = Effect.fn("hashBundle")(function* (root: string) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const entries: string[] = [];
  const visit = Effect.fn("mobile-native-client.visit")(function* (
    relative: string,
  ): Effect.fn.Return<void, FileSystemError, FileSystem.FileSystem | Crypto.Crypto> {
    const names = (yield* fs.readDirectory(path.join(root, relative))).sort((a, b) =>
      a.localeCompare(b, "en"),
    );
    for (const name of names) {
      const key = path.join(relative, name);
      const absolute = path.join(root, key);
      // FileSystem.stat follows links; readLink distinguishes them without following a cycle.
      const link = yield* fs.readLink(absolute).pipe(
        Effect.catchIf(
          (error) => isNotLink(error.reason.cause),
          () => Effect.succeed(null),
        ),
      );
      if (link !== null) {
        entries.push(`link:${key}:${link}`);
        continue;
      }
      const info = yield* fs.stat(absolute);
      if (info.type === "Directory") {
        entries.push(`directory:${key}`);
        yield* visit(key);
      } else {
        const chunks = yield* fs.stream(absolute, { chunkSize: 65536 }).pipe(
          Stream.mapEffect((chunk) => digest(chunk)),
          Stream.runCollect,
        );
        entries.push(`file:${key}:${yield* digest(chunks.join("\n"))}`);
      }
    }
  });
  yield* visit("");
  return yield* digest(entries.map((entry) => `${entry.length}:${entry}`).join(""));
});
type FileSystemError = import("effect/PlatformError").PlatformError;

const bundleId = "com.t3tools.t3code.dev";
const roots = Effect.gen(function* () {
  const path = yield* Path.Path;
  const repo = yield* path.fromFileUrl(new URL("../", import.meta.url));
  return { repo, mobile: path.join(repo, "apps/mobile") };
});
const collect = <E>(stream: Stream.Stream<Uint8Array, E>) =>
  stream.pipe(
    Stream.decodeText(),
    Stream.runFold(
      () => "",
      (a, b) => a + b,
    ),
  );
export const resolveAdb = Effect.gen(function* () {
  if (yield* isCommandAvailable("adb")) return "adb";
  const environment = yield* HostProcess.Environment;
  const path = yield* Path.Path;
  const executable = (yield* HostProcess.Platform) === "win32" ? "adb.exe" : "adb";
  for (const sdk of [environment.ANDROID_SDK_ROOT, environment.ANDROID_HOME]) {
    if (!sdk) continue;
    const candidate = path.join(sdk, "platform-tools", executable);
    if (yield* isCommandAvailable(candidate)) return candidate;
  }
  return yield* new NativeClientError({
    message:
      "adb was not found on PATH or in ANDROID_SDK_ROOT/ANDROID_HOME. Install Android SDK platform-tools.",
  });
});

const command = Effect.fn("nativeClient.command")(function* (
  program: string,
  args: string[],
  inherit = false,
  cwd?: string,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const environment = yield* HostProcess.Environment;
  const spawn = yield* resolveSpawnCommand(program === "adb" ? yield* resolveAdb : program, args);
  const child = yield* spawner.spawn(
    ChildProcess.make(spawn.command, spawn.args, {
      shell: spawn.shell,
      cwd: cwd ?? (yield* roots).mobile,
      env: {
        ...environment,
        APP_VARIANT: "development",
        MOBILE_VERSION_POLICY: "appVersion",
        T3CODE_IOS_PERSONAL_TEAM: "0",
        CI: "1",
        EXPO_NO_GIT_STATUS: "1",
        // macOS tar otherwise adds AppleDouble entries that change the extracted bundle.
        COPYFILE_DISABLE: "1",
      },
      stdin: "ignore",
      stdout: inherit ? "inherit" : "pipe",
      stderr: inherit ? "inherit" : "pipe",
    }),
  );
  const [stdout, stderr, code] = yield* Effect.all(
    [collect(child.stdout), collect(child.stderr), child.exitCode],
    { concurrency: "unbounded" },
  );
  if (code !== 0)
    return yield* new NativeClientError({
      message: `${program} ${args[0]} failed (${code}): ${stderr || "see build output"}`,
    });
  return stdout.trim();
}, Effect.scoped);

const fingerprint = Effect.fn("nativeClient.fingerprint")(function* (platform: NativePlatform) {
  const output = yield* command(yield* HostProcess.ExecutablePath, [
    "--eval",
    `require('expo/fingerprint').createFingerprintAsync(process.cwd(), { platforms: [process.argv[1]], silent: true }).then(fp => console.log('T3_NATIVE_FINGERPRINT=' + fp.hash)).catch(e => { console.error(e); process.exitCode = 1; });`,
    platform,
  ]);
  const hash = output
    .split("\n")
    .find((line) => line.startsWith("T3_NATIVE_FINGERPRINT="))
    ?.split("=")[1];
  if (!hash || !/^[a-f0-9]{40,64}$/.test(hash))
    return yield* new NativeClientError({ message: "Expo did not return a native fingerprint." });
  return hash;
});

const validateDevice = Effect.fn("nativeClient.validateDevice")(function* (
  platform: NativePlatform,
  device: string,
) {
  if (platform === "ios") {
    if ((yield* HostProcess.Platform) !== "darwin")
      return yield* new NativeClientError({
        message: "Run iOS check/ensure on the Mac that hosts the simulator.",
      });
    const listing = yield* decodeSimulators(
      yield* command("xcrun", ["simctl", "list", "devices", "available", "--json"]),
    );
    const simulator = Object.values(listing.devices)
      .flat()
      .find((entry) => entry.udid === device);
    if (!simulator)
      return yield* new NativeClientError({
        message: `No available iOS simulator with UDID ${device}.`,
      });
    if (simulator.state !== "Booted")
      return yield* new NativeClientError({
        message: `Boot the selected simulator first: xcrun simctl boot ${device}`,
      });
  } else {
    if ((yield* command("adb", ["-s", device, "get-state"])) !== "device")
      return yield* new NativeClientError({ message: "Android device is not connected." });
    if ((yield* command("adb", ["-s", device, "shell", "getprop", "ro.kernel.qemu"])) !== "1")
      return yield* new NativeClientError({
        message: "Select an Android emulator, not a physical device.",
      });
  }
});

export const installedBinary = Effect.fn("installedBinary")(function* (
  platform: NativePlatform,
  device: string,
  run: typeof command = command,
) {
  if (platform === "ios") {
    const apps = yield* run("xcrun", ["simctl", "listapps", device]);
    if (!apps.includes(`"${bundleId}"`)) return null;
    return yield* hashBundle(
      yield* run("xcrun", ["simctl", "get_app_container", device, bundleId, "app"]),
    );
  }
  const installed = yield* run("adb", ["-s", device, "shell", "pm", "list", "packages", bundleId]);
  if (!installed.split("\n").some((line) => line.trim() === `package:${bundleId}`)) return null;
  const packages = yield* run("adb", ["-s", device, "shell", "pm", "path", bundleId]);
  const apks = packages
    .split("\n")
    .filter((line) => line.startsWith("package:"))
    .map((line) => line.slice(8).trim())
    .sort();
  if (apks.length === 0) return null;
  const hashes = yield* Effect.forEach(apks, (apk) =>
    Effect.gen(function* () {
      if (!/^\/[\w/+=.~-]+\.apk$/.test(apk))
        return yield* new NativeClientError({ message: "Unexpected installed APK path." });
      const hash = (yield* run("adb", ["-s", device, "shell", "sha256sum", apk])).split(/\s/)[0];
      if (!hash || !/^[a-f0-9]{64}$/.test(hash))
        return yield* new NativeClientError({ message: "Could not hash installed APK." });
      return hash;
    }),
  );
  return yield* digest(hashes.sort().join("\n"));
});

/** Preserve input-invalidated Gradle/.cxx caches; exclude app/build because its tasks omit inputs. Ignore cache failures. */
export const prebuildAndroid = Effect.fn("nativeClient.prebuildAndroid")(function* <E, R>(
  mobile: string,
  prebuild: Effect.Effect<void, E, R>,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const android = path.join(mobile, "android");
  // Sibling renames stay on one filesystem; interrupted runs leave this gitignored cache for next-run cleanup.
  const saved = path.join(mobile, ".android-cache");
  const caches = [".gradle", "app/.cxx"];
  const clear = fs.remove(saved, { recursive: true, force: true }).pipe(Effect.ignore);
  const move = (from: string, to: string) =>
    Effect.forEach(
      caches,
      (cache) =>
        Effect.gen(function* () {
          const source = path.join(from, cache);
          const target = path.join(to, cache);
          if (!(yield* fs.exists(source)) || (yield* fs.exists(target))) return;
          yield* fs.makeDirectory(path.dirname(target), { recursive: true });
          yield* fs.rename(source, target);
        }).pipe(Effect.ignore),
      { discard: true },
    );
  yield* Effect.acquireUseRelease(
    clear.pipe(Effect.andThen(move(android, saved))),
    () => prebuild,
    () => move(saved, android).pipe(Effect.andThen(clear)),
  );
});

const artifactNames = { ios: "T3CodeDev.app", android: "app-debug.apk" } as const;
// R2 requires equal-sized parts and Workers cap request bodies at 100 MB.
const partSize = 64 * 1024 * 1024;
const RemoteCredentials = Schema.fromJsonString(
  Schema.Record(Schema.String, Schema.Struct({ name: Schema.String, token: Schema.String })),
);
const decodeCredentials = Schema.decodeUnknownEffect(RemoteCredentials);
const decodeEndpoint = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Struct({ endpoint: Schema.String })),
);
const decodeUpload = Schema.decodeUnknownEffect(
  Schema.Union([
    Schema.Struct({ exists: Schema.Literal(true) }),
    Schema.Struct({ uploadId: Schema.String }),
  ]),
);
const decodePart = Schema.decodeUnknownEffect(Schema.Struct({ etag: Schema.String }));

interface RemoteCache {
  readonly url: string;
  readonly name: string;
  readonly token: string;
}

/** The shared task-cache login (infra/task-cache/install.sh) also authorizes native clients. */
const remoteCache = Effect.fn("nativeClient.remoteCache")(
  function* (home: string) {
    const environment = yield* HostProcess.Environment;
    if (environment.T3_NATIVE_CLIENT_CACHE === "off") return null;
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const { endpoint } = yield* decodeEndpoint(
      yield* fs.readFileString(path.join((yield* roots).repo, ".task-cache.json")),
    );
    const credentials = yield* decodeCredentials(
      yield* fs.readFileString(path.join(home, ".config/t3-task-cache/credentials.json")),
    );
    const login = credentials[endpoint];
    if (!login) return null;
    return { url: `${endpoint.replace(/\/$/, "")}/native-clients`, ...login } satisfies RemoteCache;
  },
  Effect.catch(() => Effect.succeed(null)),
);

const authorized = (remote: RemoteCache, request: HttpClientRequest.HttpClientRequest) =>
  request.pipe(HttpClientRequest.basicAuth(remote.name, remote.token));

/** Download into a sibling staging directory and publish only after the digest matches. */
const download = Effect.fn("nativeClient.download")(function* (
  remote: RemoteCache,
  platform: NativePlatform,
  key: string,
  target: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const client = yield* HttpClient.HttpClient;
  const response = yield* client.execute(
    authorized(remote, HttpClientRequest.get(`${remote.url}/${key}`)),
  );
  if (response.status === 404) return false;
  yield* HttpClientResponse.filterStatusOk(response);
  const expected = response.headers["x-t3-digest"] ?? "";
  if (!/^[a-f0-9]{64}$/.test(expected))
    return yield* new NativeClientError({ message: "Cached native client has no digest." });
  yield* Console.error(`Downloading native client ${key} from the shared cache...`);
  const staging = yield* fs.makeTempDirectory({
    directory: path.dirname(target),
    prefix: ".download-",
  });
  return yield* Effect.gen(function* () {
    if (platform === "ios") {
      const archive = `${staging}.tgz`;
      yield* response.stream.pipe(Stream.run(fs.sink(archive)));
      yield* command("tar", ["-xzf", archive, "-C", staging]).pipe(
        Effect.ensuring(fs.remove(archive, { force: true }).pipe(Effect.ignore)),
      );
    } else {
      yield* response.stream.pipe(Stream.run(fs.sink(path.join(staging, artifactNames.android))));
    }
    if ((yield* hashBundle(staging)) !== expected)
      return yield* new NativeClientError({ message: "Cached native client failed verification." });
    yield* publish(staging, target);
    return true;
  }).pipe(
    Effect.ensuring(fs.remove(staging, { recursive: true, force: true }).pipe(Effect.ignore)),
  );
});

/** Upload in fixed-size parts; any failure aborts the upload and leaves the cache unchanged. */
const upload = Effect.fn("nativeClient.upload")(function* (
  remote: RemoteCache,
  platform: NativePlatform,
  key: string,
  artifact: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const client = (yield* HttpClient.HttpClient).pipe(HttpClient.filterStatusOk);
  const created = yield* client
    .execute(
      authorized(remote, HttpClientRequest.post(`${remote.url}/${key}/uploads`)).pipe(
        HttpClientRequest.setHeader("x-t3-digest", yield* hashBundle(artifact)),
      ),
    )
    .pipe(
      Effect.flatMap((response) => response.json),
      Effect.flatMap(decodeUpload),
    );
  if ("exists" in created) return;
  const uploadUrl = `${remote.url}/${key}/uploads/${encodeURIComponent(created.uploadId)}`;
  yield* Console.error(`Uploading native client ${key} to the shared cache...`);
  yield* Effect.gen(function* () {
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-native-upload-" });
    const file =
      platform === "ios"
        ? path.join(directory, "client.tgz")
        : path.join(artifact, artifactNames.android);
    if (platform === "ios")
      yield* command("tar", ["-czf", file, "-C", artifact, artifactNames.ios]);
    const handle = yield* fs.open(file);
    const parts: Array<{ partNumber: number; etag: string }> = [];
    for (let partNumber = 1; ; partNumber++) {
      const buffer = new Uint8Array(partSize);
      let filled = 0;
      while (filled < partSize) {
        const read = Number(yield* handle.read(buffer.subarray(filled)));
        if (read === 0) break;
        filled += read;
      }
      if (filled === 0) break;
      const { etag } = yield* client
        .execute(
          authorized(remote, HttpClientRequest.put(`${uploadUrl}/${partNumber}`)).pipe(
            HttpClientRequest.bodyUint8Array(buffer.subarray(0, filled)),
          ),
        )
        .pipe(
          Effect.flatMap((response) => response.json),
          Effect.flatMap(decodePart),
        );
      parts.push({ partNumber, etag });
      if (filled < partSize) break;
    }
    yield* client.execute(
      authorized(remote, HttpClientRequest.post(`${uploadUrl}/complete`)).pipe(
        HttpClientRequest.bodyJsonUnsafe({ parts }),
      ),
    );
  }).pipe(
    Effect.scoped,
    Effect.onError(() =>
      client.execute(authorized(remote, HttpClientRequest.delete(uploadUrl))).pipe(Effect.ignore),
    ),
  );
});

const publish = Effect.fn("nativeClient.publish")(function* (staging: string, target: string) {
  const fs = yield* FileSystem.FileSystem;
  // A concurrent ensure may have published the same fingerprint first; either copy is valid.
  if (yield* fs.exists(target)) return;
  yield* fs.rename(staging, target);
});

const build = Effect.fn("nativeClient.build")(function* (
  platform: NativePlatform,
  abi: string | null,
  target: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const { repo, mobile } = yield* roots;
  yield* Console.error("No cached native client for this fingerprint. Building one...");
  const tracked = yield* command("git", ["ls-files", `apps/mobile/${platform}`], false, repo);
  if (tracked)
    return yield* new NativeClientError({
      message: "Native directory contains tracked files; clean prebuild would overwrite them.",
    });
  const prebuild = command(
    "vp",
    ["exec", "expo", "prebuild", "--clean", "--platform", platform, "--no-install"],
    true,
  ).pipe(Effect.asVoid);
  yield* platform === "android" ? prebuildAndroid(mobile, prebuild) : prebuild;
  const staging = yield* fs.makeTempDirectory({
    directory: path.dirname(target),
    prefix: ".build-",
  });
  yield* Effect.gen(function* () {
    if (platform === "ios") {
      const output = yield* fs.makeTempDirectoryScoped({ prefix: "t3-native-client-" });
      yield* command("pod", ["install"], true, path.join(mobile, "ios"));
      // One device-independent arm64 simulator build installs on every iPhone and iPad simulator.
      yield* command(
        "xcrun",
        [
          "xcodebuild",
          "-workspace",
          path.join(mobile, "ios/T3CodeDev.xcworkspace"),
          "-scheme",
          "T3CodeDev",
          "-configuration",
          "Debug",
          "-destination",
          "generic/platform=iOS Simulator",
          "-derivedDataPath",
          output,
          "ARCHS=arm64",
          "ONLY_ACTIVE_ARCH=NO",
          "build",
        ],
        true,
      );
      yield* command("ditto", [
        path.join(output, "Build/Products/Debug-iphonesimulator", artifactNames.ios),
        path.join(staging, artifactNames.ios),
      ]);
    } else {
      const android = path.join(mobile, "android");
      yield* command(
        path.join(android, (yield* HostProcess.Platform) === "win32" ? "gradlew.bat" : "gradlew"),
        ["app:assembleDebug", `-PreactNativeArchitectures=${abi}`],
        true,
        android,
      );
      yield* fs.copyFile(
        path.join(android, "app/build/outputs/apk/debug", artifactNames.android),
        path.join(staging, artifactNames.android),
      );
    }
    yield* publish(staging, target);
  }).pipe(
    Effect.scoped,
    Effect.ensuring(fs.remove(staging, { recursive: true, force: true }).pipe(Effect.ignore)),
  );
});

/** Keep the newest few fingerprints per platform; each artifact is a few hundred megabytes. */
const prune = Effect.fn("nativeClient.prune")(
  function* (parent: string, keep: string) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const entries = yield* Effect.forEach(
      (yield* fs.readDirectory(parent)).filter((name) => !name.startsWith(".")),
      (name) =>
        fs.stat(path.join(parent, name)).pipe(
          Effect.map((info) => ({
            name,
            modified: Option.getOrUndefined(info.mtime)?.getTime() ?? 0,
          })),
        ),
    );
    const stale = entries
      .filter(({ name }) => name !== keep)
      .sort((a, b) => b.modified - a.modified)
      .slice(2);
    yield* Effect.forEach(
      stale,
      ({ name }) => fs.remove(path.join(parent, name), { recursive: true, force: true }),
      { discard: true },
    );
  },
  (effect) => Effect.ignore(effect),
);

/** Local artifact, then the shared cache, then a build that is shared back when credentials allow. */
export const resolveArtifact = Effect.fn("nativeClient.resolveArtifact")(function* (
  home: string,
  platform: NativePlatform,
  abi: string | null,
  fingerprint: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const key = `${platform}/${abi ? `development-${abi}` : "development"}/${fingerprint}`;
  const directory = path.join(home, ".cache/t3code/native-clients/artifacts", key);
  const artifact = { directory, file: path.join(directory, artifactNames[platform]) };
  if (yield* fs.exists(artifact.file)) return { ...artifact, source: "local" as const };
  yield* fs.makeDirectory(path.dirname(directory), { recursive: true });
  const remote = yield* remoteCache(home);
  if (remote) {
    const downloaded = yield* download(remote, platform, key, directory).pipe(
      Effect.catch((error) =>
        Console.error(`Shared native client cache unavailable (${describe(error)}).`).pipe(
          Effect.as(false),
        ),
      ),
    );
    if (downloaded) {
      yield* prune(path.dirname(directory), fingerprint);
      return { ...artifact, source: "remote" as const };
    }
  }
  yield* build(platform, abi, directory);
  yield* prune(path.dirname(directory), fingerprint);
  if (remote && (yield* HostProcess.Environment).T3_NATIVE_CLIENT_CACHE !== "read") {
    yield* upload(remote, platform, key, directory).pipe(
      Effect.catch((error) => Console.error(`Native client was not shared (${describe(error)}).`)),
    );
  }
  return { ...artifact, source: "build" as const };
});

const describe = (error: unknown) =>
  typeof error === "object" &&
  error !== null &&
  "message" in error &&
  typeof error.message === "string"
    ? error.message
    : String(error);

const install = Effect.fn("nativeClient.install")(function* (
  platform: NativePlatform,
  device: string,
  file: string,
) {
  if (platform === "ios") {
    yield* command("xcrun", ["simctl", "install", device, file], true);
    return;
  }
  yield* command("adb", ["-s", device, "install", "-r", file]).pipe(
    Effect.catchTags({
      NativeClientError: (error) =>
        Effect.fail(
          error.message.includes("INSTALL_FAILED_UPDATE_INCOMPATIBLE")
            ? new NativeClientError({
                message: `The installed client has a different signature. Remove it with adb -s ${device} uninstall ${bundleId} (this clears its pairings), then run ensure again.`,
              })
            : error,
        ),
    }),
  );
});

const main = Command.make(
  "mobile-native-client",
  {
    mode: Argument.Literals("mode", ["check", "ensure"]),
    platform: Argument.Literals("platform", ["ios", "android"]),
    device: Argument.String("device"),
  },
  Effect.fn("nativeClient.main")(function* ({ mode, platform, device }) {
    yield* validateDevice(platform, device);
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const environment = yield* HostProcess.Environment;
    const home = environment.HOME ?? environment.USERPROFILE;
    if (!home)
      return yield* new NativeClientError({
        message: "HOME or USERPROFILE must be set to store native client records.",
      });
    const recordPath = path.join(
      home,
      ".cache/t3code/native-clients",
      platform,
      `${yield* digest(device)}.json`,
    );
    // An iOS simulator build is arm64 for every device; an APK carries only the emulator's own ABI.
    const emulatorAbi = Effect.gen(function* () {
      if (platform === "ios") return null;
      const abi = yield* command("adb", ["-s", device, "shell", "getprop", "ro.product.cpu.abi"]);
      if (abi !== "x86_64" && abi !== "arm64-v8a")
        return yield* new NativeClientError({ message: `Unsupported emulator ABI ${abi}.` });
      return abi;
    });
    const operations = {
      fingerprint: fingerprint(platform),
      installedBinary: installedBinary(platform, device),
      readRecord: fs.readFileString(recordPath).pipe(
        Effect.flatMap(decodeRecord),
        Effect.catchTags({ SchemaError: () => Effect.succeed(null) }),
        Effect.catchIf(
          (error) => error.reason._tag === "NotFound",
          () => Effect.succeed(null),
        ),
      ),
      install: Effect.fn(function* (fingerprint: string) {
        const artifact = yield* resolveArtifact(home, platform, yield* emulatorAbi, fingerprint);
        yield* install(platform, device, artifact.file);
        return artifact.source;
      }),
      saveRecord: Effect.fn(function* (record: NativeClientRecord) {
        yield* fs.makeDirectory(path.dirname(recordPath), { recursive: true });
        yield* fs.writeFileString(recordPath, yield* encodeRecord(record));
      }),
    };
    if (mode === "ensure") {
      yield* Console.log(yield* encodeOutput(yield* ensureClient(operations)));
    } else {
      const current = yield* operations.fingerprint;
      const status = clientStatus(
        current,
        yield* operations.installedBinary,
        yield* operations.readRecord,
      );
      yield* Console.log(
        yield* encodeOutput({
          status,
          fingerprint: current,
          next:
            status === "compatible"
              ? "Start Metro with vp run dev:client"
              : `node scripts/mobile-native-client.ts ensure ${platform} ${device}`,
        }),
      );
      process.exitCode = status === "compatible" ? 0 : 2;
    }
  }),
);

if (import.meta.main) {
  Command.run(main, { version: "0.0.0" }).pipe(
    Effect.scoped,
    Effect.provide([NodeServices.layer, FetchHttpClient.layer]),
    NodeRuntime.runMain,
  );
}
