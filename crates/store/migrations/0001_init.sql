-- archivar storage core, slice 1.
--
-- Two schemas with different audiences:
--   core: base tables. Only the application connects with rights on them.
--   read: the published read schema. Plain views over core, the only thing
--         the `query` command and saved view schemes are allowed to see.

CREATE SCHEMA core;
CREATE SCHEMA read;

-- Tiers and access ------------------------------------------------------------

CREATE TYPE core.tier AS ENUM ('raw', 'canonical', 'working', 'derived');

CREATE TABLE core.roles (
    name text PRIMARY KEY
);

-- The role x tier matrix. For raw, may_commit only means "may ingest new
-- documents": raw content itself never changes, whatever this table says.
CREATE TABLE core.acl (
    role        text      NOT NULL REFERENCES core.roles,
    tier        core.tier NOT NULL,
    may_read    boolean   NOT NULL,
    may_propose boolean   NOT NULL,
    may_commit  boolean   NOT NULL,
    PRIMARY KEY (role, tier)
);

INSERT INTO core.roles VALUES ('admin'), ('editor'), ('agent'), ('reader');

INSERT INTO core.acl (role, tier, may_read, may_propose, may_commit) VALUES
    ('admin',  'raw',       true, true,  true),
    ('admin',  'canonical', true, true,  true),
    ('admin',  'working',   true, true,  true),
    ('admin',  'derived',   true, true,  true),
    ('editor', 'raw',       true, true,  true),
    ('editor', 'canonical', true, true,  true),
    ('editor', 'working',   true, true,  true),
    ('editor', 'derived',   true, true,  true),
    -- The agent row also caps any human while an agent acts for them, so
    -- canonical stays closed to commits that did not come from a human hand.
    ('agent',  'raw',       true, true,  true),
    ('agent',  'canonical', true, true,  false),
    ('agent',  'working',   true, true,  true),
    ('agent',  'derived',   true, true,  true),
    ('reader', 'raw',       true, false, false),
    ('reader', 'canonical', true, false, false),
    ('reader', 'working',   true, false, false),
    ('reader', 'derived',   true, false, false);

CREATE TABLE core.principals (
    id         uuid        PRIMARY KEY,
    name       text        NOT NULL UNIQUE,
    role       text        NOT NULL REFERENCES core.roles,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Documents and blocks: the current state -------------------------------------
--
-- These tables are a projection of core.events. Every write to them happens in
-- the same transaction as the event that explains it.

CREATE TABLE core.documents (
    id         uuid        PRIMARY KEY,
    title      text        NOT NULL,
    tier       core.tier   NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE core.blocks (
    id          uuid             PRIMARY KEY,
    document_id uuid             NOT NULL REFERENCES core.documents,
    -- Fractional ordering: inserting between two blocks takes the midpoint, so
    -- no other row has to move.
    position    double precision NOT NULL,
    kind        text             NOT NULL,
    body        text             NOT NULL,
    -- Bumped on every change. Proposals pin the version they were based on.
    version     integer          NOT NULL DEFAULT 1,
    deleted     boolean          NOT NULL DEFAULT false,
    updated_at  timestamptz      NOT NULL DEFAULT now()
);

CREATE INDEX blocks_document ON core.blocks (document_id, position);

-- Proposals -------------------------------------------------------------------

CREATE TABLE core.proposals (
    id           uuid        PRIMARY KEY,
    document_id  uuid        NOT NULL REFERENCES core.documents,
    principal_id uuid        NOT NULL REFERENCES core.principals,
    agent        text,
    note         text,
    status       text        NOT NULL DEFAULT 'open'
                 CHECK (status IN ('open', 'committed', 'rejected')),
    created_at   timestamptz NOT NULL DEFAULT now(),
    decided_by   uuid        REFERENCES core.principals,
    decided_at   timestamptz
);

-- What a reviewer approves is exactly these rows, so they never change after
-- the proposal is created (see the trigger below).
CREATE TABLE core.proposal_changes (
    proposal_id  uuid    NOT NULL REFERENCES core.proposals,
    seq          integer NOT NULL,
    op           text    NOT NULL CHECK (op IN ('insert', 'update', 'delete')),
    block_id     uuid    NOT NULL,
    base_version integer,
    kind         text,
    body         text,
    position     double precision,
    PRIMARY KEY (proposal_id, seq)
);

-- The event log ---------------------------------------------------------------

CREATE TABLE core.events (
    seq          bigserial   PRIMARY KEY,
    at           timestamptz NOT NULL DEFAULT now(),
    principal_id uuid        NOT NULL REFERENCES core.principals,
    agent        text,
    kind         text        NOT NULL,
    document_id  uuid,
    block_id     uuid,
    proposal_id  uuid,
    payload      jsonb       NOT NULL DEFAULT '{}'
);

CREATE INDEX events_block ON core.events (block_id, seq);
CREATE INDEX events_document ON core.events (document_id, seq);

-- Append-only, enforced by the database itself. A REVOKE alone would not do:
-- the application connects as the owner of these tables, and owners bypass
-- privileges. A trigger fires for everyone. Purge will disable it under the
-- admin role, inside the purge transaction.
CREATE FUNCTION core.forbid_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END;
$$;

CREATE TRIGGER events_append_only
    BEFORE UPDATE OR DELETE ON core.events
    FOR EACH ROW EXECUTE FUNCTION core.forbid_mutation();

CREATE TRIGGER proposal_changes_immutable
    BEFORE UPDATE OR DELETE ON core.proposal_changes
    FOR EACH ROW EXECUTE FUNCTION core.forbid_mutation();

-- TRUNCATE skips row triggers entirely, so it needs a statement trigger.
CREATE TRIGGER events_no_truncate
    BEFORE TRUNCATE ON core.events
    FOR EACH STATEMENT EXECUTE FUNCTION core.forbid_mutation();

CREATE TRIGGER proposal_changes_no_truncate
    BEFORE TRUNCATE ON core.proposal_changes
    FOR EACH STATEMENT EXECUTE FUNCTION core.forbid_mutation();

-- The published read schema ---------------------------------------------------
--
-- Views run with the rights of their owner, not of whoever queries them, so a
-- role with SELECT on these views reads core data without any right on core.

CREATE VIEW read.documents AS
    SELECT id, title, tier::text AS tier, created_at FROM core.documents;

CREATE VIEW read.blocks AS
    SELECT id, document_id, position, kind, body, version, updated_at
    FROM core.blocks
    WHERE NOT deleted;

CREATE VIEW read.principals AS
    SELECT id, name, role FROM core.principals;

CREATE VIEW read.events AS
    SELECT e.seq, e.at, p.name AS principal, e.agent, e.kind,
           e.document_id, e.block_id, e.proposal_id, e.payload
    FROM core.events e JOIN core.principals p ON p.id = e.principal_id;

CREATE VIEW read.proposals AS
    SELECT pr.id, pr.document_id, p.name AS principal, pr.agent, pr.note,
           pr.status, pr.created_at, d.name AS decided_by, pr.decided_at
    FROM core.proposals pr
    JOIN core.principals p ON p.id = pr.principal_id
    LEFT JOIN core.principals d ON d.id = pr.decided_by;

CREATE VIEW read.proposal_changes AS
    SELECT proposal_id, seq, op, block_id, base_version, kind, body
    FROM core.proposal_changes;

-- The query role --------------------------------------------------------------
--
-- Roles belong to the whole cluster, not to one database, and every test gets
-- its own database running this migration concurrently. So create the role only
-- if it is missing, and treat losing that race as success.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'archivar_reader') THEN
        CREATE ROLE archivar_reader LOGIN;
    END IF;
EXCEPTION WHEN duplicate_object OR unique_violation THEN
    NULL;
END
$$;

-- Grants, by contrast, are per database.
GRANT USAGE ON SCHEMA read TO archivar_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA read TO archivar_reader;
