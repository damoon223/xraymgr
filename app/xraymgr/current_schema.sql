-- This file is the CURRENT SQLite schema for the xraymgr project.
-- It is intentionally committed as a reference snapshot for future sessions.
-- Regenerate it by running: app/xraymgr/dump_schema.py (or: python -m xraymgr.dump_schema)
--
-- Generated at (UTC): 2026-01-03T05:15:17Z
-- DB: /opt/xraymgr/data/xraymgr.db
-- Output: /opt/xraymgr/app/xraymgr/current_schema.sql
-- ===================================================================

PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;

-- ---- TABLES ----
CREATE TABLE inbound (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  role TEXT NOT NULL CHECK (role IN ('primary','test')),
  is_active INTEGER NOT NULL DEFAULT 0 CHECK (is_active IN (0,1)),
  port INTEGER NOT NULL,
  tag TEXT NOT NULL,
  link_id INTEGER REFERENCES links(id) ON DELETE RESTRICT,
  outbound_tag TEXT,
  status TEXT NOT NULL DEFAULT 'new' CHECK (TRIM(status) <> '' AND instr(status,' ') = 0),
  last_test_at DATETIME
);

CREATE TABLE links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL UNIQUE,
                config_json TEXT,
                config_hash VARCHAR(64),
                is_config_primary INTEGER,
                test_stage INTEGER NOT NULL DEFAULT 0,
                is_alive INTEGER NOT NULL DEFAULT 0,
                ip TEXT,
                country TEXT,
                city TEXT,
                datacenter TEXT,
                is_in_use INTEGER NOT NULL DEFAULT 0,
                bound_port INTEGER,
                last_test_at DATETIME,
                needs_replace INTEGER NOT NULL DEFAULT 0
            , is_invalid INTEGER NOT NULL DEFAULT 0, config_group_id TEXT DEFAULT '', is_protocol_unsupported INTEGER NOT NULL DEFAULT 0, parent_id INTEGER, repaired_url TEXT, outbound_tag TEXT, inbound_tag  TEXT, test_status TEXT NOT NULL DEFAULT 'idle', test_started_at DATETIME, test_lock_until DATETIME, test_lock_owner TEXT, test_batch_id TEXT, last_test_ok INTEGER NOT NULL DEFAULT 0, last_test_error TEXT);


-- ---- INDEXES ----
CREATE INDEX idx_inbound_link_id ON inbound(link_id);

CREATE INDEX idx_inbound_out_tag ON inbound(outbound_tag);

CREATE UNIQUE INDEX idx_inbound_port_unique ON inbound(port);

CREATE INDEX idx_inbound_role ON inbound(role);

CREATE UNIQUE INDEX idx_inbound_tag_unique ON inbound(tag);

CREATE INDEX idx_links_config_hash
ON links(config_hash);

CREATE UNIQUE INDEX idx_links_inbound_tag_unique
ON links(inbound_tag)
WHERE inbound_tag IS NOT NULL AND TRIM(inbound_tag) <> '';

CREATE UNIQUE INDEX idx_links_outbound_tag_unique
ON links(outbound_tag)
WHERE outbound_tag IS NOT NULL AND TRIM(outbound_tag) <> '';

CREATE INDEX idx_links_test_batch_id ON links(test_batch_id);

CREATE INDEX idx_links_test_lock_until ON links(test_lock_until);

CREATE INDEX idx_links_test_status ON links(test_status);


-- ---- TRIGGERS ----

-- ---- VIEWS ----

COMMIT;
PRAGMA foreign_keys=ON;
