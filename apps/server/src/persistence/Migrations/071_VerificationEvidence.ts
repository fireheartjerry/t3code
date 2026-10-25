import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE orchestration_v2_verification_attempts (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    verification_id TEXT NOT NULL UNIQUE,
    command_id TEXT NOT NULL UNIQUE,
    thread_id TEXT NOT NULL,
    checkout_key TEXT NOT NULL,
    source_run_id TEXT,
    source_run_ordinal INTEGER NOT NULL,
    execution TEXT NOT NULL,
    currentness TEXT NOT NULL,
    server_instance_id TEXT,
    execution_claim_id TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    invalidated INTEGER NOT NULL DEFAULT 0,
    payload_json TEXT NOT NULL
  )`;
  yield* sql`CREATE INDEX orchestration_v2_verification_thread_sequence
    ON orchestration_v2_verification_attempts(thread_id, source_run_ordinal DESC, sequence DESC)`;
  yield* sql`CREATE INDEX orchestration_v2_verification_checkout
    ON orchestration_v2_verification_attempts(checkout_key, currentness)`;
  yield* sql`CREATE TABLE orchestration_v2_verification_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    verification_id TEXT NOT NULL,
    command_id TEXT NOT NULL UNIQUE,
    payload_json TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE orchestration_v2_verification_output (
    reference_id TEXT PRIMARY KEY,
    verification_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    content TEXT NOT NULL,
    truncated INTEGER NOT NULL CHECK(truncated IN (0, 1))
  )`;
});
