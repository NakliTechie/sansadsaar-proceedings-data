-- Netas — person-centric SQLite schema (v0.1)
--
-- Per plan/netas-person-centric-restructure-003-v0.1.md in the Netas repo.
-- This is the canonical schema for the pipeline's output (netas.sqlite).
-- The DB is a build-time artefact — Astro reads it at SSG time and emits
-- prerendered HTML; the DB never ships to the client.
--
-- Design notes:
-- * pid is the only primary key for a person. sansad DOB+name is a merge
--   signal, not identity.
-- * Schema is office-type-agnostic: LS, RS, AC supported today; LC, MC,
--   panchayat etc. fit without further migration.
-- * SQLite UNIQUE treats NULLs as distinct, so where external_id can be
--   NULL we add a source_row_hash fallback constraint.
-- * Source-id uniqueness on corpus tables guarantees idempotent
--   re-ingestion of the same upstream snapshot.

PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

-- ── Persons ──────────────────────────────────────────────────────────────
-- Pid is our own opaque taxonomy (n-<int>), minted in build_person_master.py
-- and persisted to person_pid_assignments.json so URLs stay stable across
-- re-runs.

CREATE TABLE persons (
    pid               TEXT    PRIMARY KEY,           -- 'n-<int>'
    canonical_name    TEXT    NOT NULL,
    slug              TEXT    NOT NULL,              -- not UNIQUE; pid is the URL discriminator
    dob               TEXT,                          -- ISO yyyy-mm-dd
    gender            TEXT,
    image_url         TEXT,
    identity_source   TEXT    NOT NULL CHECK (
        identity_source IN ('both', 'dob_only', 'tcpd_only', 'fallback', 'tcpd_backfill', 'rs_tcpd')
    ),
    -- External identity attributes. Attributes only — never URL keys.
    tcpd_pid          TEXT,                          -- TCPD candidate id (multi-state spline)
    rs_tcpd_id        TEXT,                          -- TCPD-RSD row id, when historical RS source contributed
    sansad_dob_hash   TEXT,                          -- internal merge key (norm_name + DOB)
    -- Anchoring evidence — which signal made this pid. Useful for audit.
    anchor_signal     TEXT,                          -- 'sansad_dob','tcpd_pid','tcpd_rsd','fallback'
    confidence        REAL                           -- 0.0..1.0; surface in debugging UI
);

CREATE INDEX persons_slug ON persons(slug);
CREATE INDEX persons_tcpd ON persons(tcpd_pid);
CREATE INDEX persons_dob_hash ON persons(sansad_dob_hash);

-- ── Tenures ──────────────────────────────────────────────────────────────
-- Each stint in an elected office. One row per (office_type, jurisdiction,
-- term).

CREATE TABLE tenures (
    id                 INTEGER PRIMARY KEY,
    pid                TEXT    NOT NULL REFERENCES persons(pid),
    office_type        TEXT    NOT NULL,             -- 'ls','rs','ac','lc','mc','panchayat'
    jurisdiction       TEXT,                          -- 'IN' for national, state for AC/LC, city for MC
    term_label         TEXT,                          -- 'LS18', 'RS-2020-2026', 'Maharashtra-AC-2024'
    external_id        TEXT,                          -- sansad mpsno for LS/RS, etc.
    term_start         TEXT,                          -- ISO date
    term_end           TEXT,                          -- ISO date; NULL if sitting
    sitting            INTEGER NOT NULL DEFAULT 0,    -- 0/1
    status             TEXT,                          -- 'Sitting','Former','Death','Retirement',...
    state              TEXT,                          -- for sansad LS/RS, the represented state
    constituency       TEXT,
    party_short        TEXT,
    speeches_count     INTEGER NOT NULL DEFAULT 0,
    questions_count    INTEGER NOT NULL DEFAULT 0,
    bills_count        INTEGER NOT NULL DEFAULT 0,
    -- Source row hash protects rows with NULL external_id from duplicate
    -- ingestion (SQLite treats NULLs as distinct under regular UNIQUE).
    source_row_hash    TEXT    NOT NULL,
    UNIQUE (office_type, jurisdiction, external_id, term_label),
    UNIQUE (office_type, jurisdiction, source_row_hash)
);

CREATE INDEX tenures_pid ON tenures(pid);
CREATE INDEX tenures_office_sitting ON tenures(office_type, sitting);
CREATE INDEX tenures_pid_office_start ON tenures(pid, office_type, term_start);
CREATE INDEX tenures_sitting_office_state ON tenures(sitting, office_type, state);
CREATE INDEX tenures_state ON tenures(state);
CREATE INDEX tenures_party ON tenures(party_short);

-- ── Contests ─────────────────────────────────────────────────────────────
-- Every election the person fought. IV-shaped (no TCPD enrichment columns);
-- tcpd_pid is kept as an external attribute for joins, not for UI.

CREATE TABLE contests (
    id                INTEGER PRIMARY KEY,
    pid               TEXT    NOT NULL REFERENCES persons(pid),
    contest_type      TEXT    NOT NULL,              -- 'ls','ac' today; 'mc',... future
    year              INTEGER NOT NULL,
    state             TEXT,
    constituency      TEXT,
    party_short       TEXT,
    votes             INTEGER,
    vote_share        REAL,                          -- fractional, 0.6618 = 66.18%
    position          INTEGER,
    won               INTEGER NOT NULL DEFAULT 0,
    iv_candidate_id   INTEGER,                       -- external join key (IV mirror)
    tcpd_pid          TEXT,                          -- external attribute
    UNIQUE (iv_candidate_id),
    UNIQUE (tcpd_pid, year, state, constituency, party_short)
);

CREATE INDEX contests_pid_year ON contests(pid, year);
CREATE INDEX contests_pid_type_year ON contests(pid, contest_type, year);
CREATE INDEX contests_state_year ON contests(state, year);

-- ── Corpora: speeches / questions / bills ────────────────────────────────
-- Each row joins to a tenure via tenure_id (canonical). source_*_id columns
-- guarantee idempotent re-ingestion — re-running the pipeline on the same
-- snapshot must not duplicate rows.

CREATE TABLE speeches (
    id                INTEGER PRIMARY KEY,
    pid               TEXT    NOT NULL REFERENCES persons(pid),
    tenure_id         INTEGER REFERENCES tenures(id),
    source_speech_id  TEXT    UNIQUE,                -- upstream id (debate_id + sequence, etc.)
    date              TEXT,
    debate_id         TEXT,
    snippet           TEXT,                          -- first ~500 chars
    full_text_ref     TEXT                           -- pointer into R2 text store
);

CREATE INDEX speeches_pid_date ON speeches(pid, date);
CREATE INDEX speeches_tenure_date ON speeches(tenure_id, date);

CREATE TABLE questions (
    id                  INTEGER PRIMARY KEY,
    pid                 TEXT    NOT NULL REFERENCES persons(pid),
    tenure_id           INTEGER REFERENCES tenures(id),
    source_question_id  TEXT    UNIQUE,
    date                TEXT,
    question_type       TEXT,                        -- 'STARRED','UNSTARRED'
    ministry            TEXT,
    subject             TEXT,
    text_ref            TEXT
);

CREATE INDEX questions_pid_date ON questions(pid, date);
CREATE INDEX questions_tenure_date ON questions(tenure_id, date);

CREATE TABLE bills (
    id              INTEGER PRIMARY KEY,
    pid             TEXT    NOT NULL REFERENCES persons(pid),
    tenure_id       INTEGER REFERENCES tenures(id),
    source_bill_id  TEXT,                            -- a bill has multiple roles, so not UNIQUE alone
    bill_title      TEXT,
    role            TEXT,                            -- 'sponsor','co-sponsor'
    UNIQUE (source_bill_id, role, pid)
);

CREATE INDEX bills_pid ON bills(pid);
CREATE INDEX bills_tenure ON bills(tenure_id);

-- ── Compatibility views ──────────────────────────────────────────────────
-- mpsno_to_pid: redirect generator queries this to emit 301s from old
-- /netas/mp/<slug>-<mpsno>/ URLs to /netas/p/<slug>-<pid>/. mpsno was the
-- LS/RS sansad external id; for other office types this view is empty.

CREATE VIEW mpsno_to_pid AS
    SELECT external_id AS mpsno, office_type AS house, pid
    FROM tenures
    WHERE office_type IN ('ls', 'rs') AND external_id IS NOT NULL;

-- ── Build metadata ───────────────────────────────────────────────────────
-- One row, overwritten on each rebuild. Lets the deploy smoke test verify
-- the DB is fresh and from the expected sources.

CREATE TABLE build_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
