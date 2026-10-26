import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE orchestration_v2_checkout_claims (
    checkout_key TEXT PRIMARY KEY,
    environment_id TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK(generation > 0),
    state TEXT NOT NULL CHECK(state IN ('held', 'uncertain', 'released')),
    owner_thread_id TEXT NOT NULL,
    server_instance_id TEXT NOT NULL,
    payload_json TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE orchestration_v2_checkout_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    command_id TEXT NOT NULL UNIQUE,
    checkout_key TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE orchestration_v2_workspace_receipts (
    command_id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    command_type TEXT NOT NULL,
    result_json TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE orchestration_v2_checkout_waiters (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    wait_id TEXT NOT NULL UNIQUE,
    command_id TEXT NOT NULL UNIQUE,
    checkout_key TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('waiting', 'ready', 'cancelled')),
    payload_json TEXT NOT NULL
  )`;
  yield* sql`CREATE INDEX orchestration_v2_checkout_waiters_active
    ON orchestration_v2_checkout_waiters(checkout_key, state, sequence)`;
});
