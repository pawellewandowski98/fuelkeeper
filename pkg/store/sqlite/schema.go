package sqlite

// schemaV1 is schema version 1 (the current, and so far only, schema). It
// mirrors postgres/schema.sql with SQLite types: timestamps are unix
// microseconds (INTEGER; NULL means the zero time). Partial-index predicates
// must be spelled exactly as in the queries (SQLite matches them textually).
//
// fuel_rows_expiry and fuel_rows_quar carry (txid, vout) as trailing key
// columns (not just pool, expires_at/quarantined_at) so the reaper's
// keyset-paged scans (Quarantine's candidate select; ListQuarantined) can use
// the index's own order directly, with the row-value cursor comparison
// bounding the scan instead of requiring a separate sort or an unbounded
// re-scan from the start each page (see rows.go).
const schemaV1 = `
CREATE TABLE IF NOT EXISTS fuel_shards (
  pool         TEXT    NOT NULL,
  txid         BLOB    NOT NULL,
  size         INTEGER NOT NULL,
  denomination INTEGER NOT NULL,
  lock_mode    INTEGER NOT NULL,
  status       INTEGER NOT NULL,
  owner        TEXT    NOT NULL DEFAULT '',
  lease_until  INTEGER NULL,
  version      INTEGER NOT NULL DEFAULT 1,
  beef         BLOB    NOT NULL,
  created_at   INTEGER NOT NULL,
  updated_at   INTEGER NOT NULL,
  PRIMARY KEY (pool, txid)
);
CREATE UNIQUE INDEX IF NOT EXISTS fuel_shards_txid ON fuel_shards (txid);
CREATE INDEX IF NOT EXISTS fuel_shards_adopt ON fuel_shards (pool, status, created_at, txid);

CREATE TABLE IF NOT EXISTS fuel_rows (
  txid             BLOB    NOT NULL,
  vout             INTEGER NOT NULL,
  pool             TEXT    NOT NULL,
  satoshis         INTEGER NOT NULL,
  locking_script   BLOB    NOT NULL,
  unlocking_script BLOB    NOT NULL,
  state            INTEGER NOT NULL,
  reserved_by      BLOB    NULL,
  reserved_at      INTEGER NULL,
  expires_at       INTEGER NULL,
  quarantined_at   INTEGER NULL,
  spent_hint       BLOB    NULL,
  spent_by         BLOB    NULL,
  updated_at       INTEGER NOT NULL,
  PRIMARY KEY (txid, vout)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS fuel_rows_avail      ON fuel_rows (pool, txid, vout)                 WHERE state = 0;
CREATE INDEX IF NOT EXISTS fuel_rows_expiry     ON fuel_rows (pool, expires_at, txid, vout)     WHERE state = 1;
CREATE INDEX IF NOT EXISTS fuel_rows_quar       ON fuel_rows (pool, quarantined_at, txid, vout) WHERE state = 2;
CREATE INDEX IF NOT EXISTS fuel_rows_shard_live ON fuel_rows (pool, txid)                       WHERE state IN (0, 1, 2);
CREATE INDEX IF NOT EXISTS fuel_rows_pool_state ON fuel_rows (pool, state);

CREATE TABLE IF NOT EXISTS fuel_leases (
  name       TEXT PRIMARY KEY,
  holder     TEXT    NOT NULL,
  expires_at INTEGER NOT NULL
);
`

// migrations holds each schema version's DDL, in order: migrations[0] is
// version 1. Applied versions are tracked in PRAGMA user_version (see
// (*Store).migrate in sqlite.go); only versions greater than the current
// user_version run, and they run inside the same BEGIN IMMEDIATE transaction
// as the user_version bump itself, so a crash mid-migration never leaves the
// version recorded as applied when it partially isn't (or vice versa).
var migrations = []string{schemaV1}
