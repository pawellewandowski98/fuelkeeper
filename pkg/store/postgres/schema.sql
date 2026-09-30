-- FuelKeeper fuel store schema (Postgres). Idempotent; applied under an advisory lock.
-- State codes: rows 0 available 1 reserved 2 quarantined 3 spent 4 invalid;
-- shards 0 unmined 1 mined 2 retired 3 invalid.

CREATE TABLE IF NOT EXISTS fuel_shards (
  pool         TEXT        NOT NULL,
  txid         BYTEA       NOT NULL,
  size         INTEGER     NOT NULL,
  denomination BIGINT      NOT NULL,
  lock_mode    SMALLINT    NOT NULL,
  status       SMALLINT    NOT NULL,
  owner        TEXT        NOT NULL DEFAULT '',
  lease_until  TIMESTAMPTZ NULL,
  version      BIGINT      NOT NULL DEFAULT 1,
  beef         BYTEA       NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL,
  updated_at   TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (pool, txid)
);
CREATE UNIQUE INDEX IF NOT EXISTS fuel_shards_txid ON fuel_shards (txid); -- a split tx belongs to one pool
CREATE INDEX IF NOT EXISTS fuel_shards_adopt ON fuel_shards (pool, status, created_at, txid);

CREATE TABLE IF NOT EXISTS fuel_rows (
  txid             BYTEA       NOT NULL,
  vout             INTEGER     NOT NULL,
  pool             TEXT        NOT NULL,
  satoshis         BIGINT      NOT NULL,
  locking_script   BYTEA       NOT NULL,
  unlocking_script BYTEA       NOT NULL,
  state            SMALLINT    NOT NULL,
  reserved_by      BYTEA       NULL,
  reserved_at      TIMESTAMPTZ NULL,
  expires_at       TIMESTAMPTZ NULL,
  quarantined_at   TIMESTAMPTZ NULL,
  spent_hint       BYTEA       NULL,
  spent_by         BYTEA       NULL,
  updated_at       TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (txid, vout)
);
-- Partial indexes shrink with the live set (see the reservation review, idx_utxos_reserved lesson).
-- fuel_rows_expiry and fuel_rows_quar carry (txid, vout) as trailing key columns
-- (not just pool, expires_at/quarantined_at) so the reaper's keyset-paged scans
-- (Quarantine's candidate CTE; ListQuarantined) can use the index's own order
-- directly, with the row-value cursor comparison bounding the scan instead of
-- requiring a separate sort or an unbounded re-scan from the start each page.
CREATE INDEX IF NOT EXISTS fuel_rows_avail      ON fuel_rows (pool, txid, vout)                    WHERE state = 0;
CREATE INDEX IF NOT EXISTS fuel_rows_expiry     ON fuel_rows (pool, expires_at, txid, vout)        WHERE state = 1;
CREATE INDEX IF NOT EXISTS fuel_rows_quar       ON fuel_rows (pool, quarantined_at, txid, vout)    WHERE state = 2;
CREATE INDEX IF NOT EXISTS fuel_rows_shard_live ON fuel_rows (pool, txid)                          WHERE state IN (0, 1, 2);
CREATE INDEX IF NOT EXISTS fuel_rows_pool_state ON fuel_rows (pool, state);

CREATE TABLE IF NOT EXISTS fuel_leases (
  name       TEXT PRIMARY KEY,
  holder     TEXT        NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL
);
