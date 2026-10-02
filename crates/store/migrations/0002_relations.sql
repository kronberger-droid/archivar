-- archivar slice 2: relations between documents and blocks.
--
-- A relation is an assertion, so like content it carries a tier and who made
-- it. Agent guesses land in `derived` directly; a human promotes the ones that
-- hold up to `canonical`. Relations never come from raw: raw is for ingested
-- originals, and a link is never an original.

CREATE TABLE core.relations (
    id            uuid             PRIMARY KEY,
    -- Each end is a document or a block. Two nullable foreign keys per end,
    -- exactly one of them set, so the database itself guarantees the end
    -- exists and is only one thing.
    from_document uuid             REFERENCES core.documents,
    from_block    uuid             REFERENCES core.blocks,
    to_document   uuid             REFERENCES core.documents,
    to_block      uuid             REFERENCES core.blocks,
    -- One id per end, whatever its kind. Block and document ids are both
    -- UUIDv7, so they never collide.
    from_node     uuid             GENERATED ALWAYS AS (coalesce(from_block, from_document)) STORED,
    to_node       uuid             GENERATED ALWAYS AS (coalesce(to_block, to_document)) STORED,
    kind          text             NOT NULL CHECK (kind <> ''),
    tier          core.tier        NOT NULL CHECK (tier <> 'raw'),
    confidence    double precision CHECK (confidence BETWEEN 0 AND 1),
    note          text,
    asserted_by   uuid             NOT NULL REFERENCES core.principals,
    agent         text,
    created_at    timestamptz      NOT NULL DEFAULT now(),
    promoted_by   uuid             REFERENCES core.principals,
    promoted_at   timestamptz,
    retracted     boolean          NOT NULL DEFAULT false,
    CHECK (num_nonnulls(from_document, from_block) = 1),
    CHECK (num_nonnulls(to_document, to_block) = 1),
    CHECK (coalesce(from_block, from_document) <> coalesce(to_block, to_document))
);

-- One live relation per (from, to, kind), whatever its tier. Asserting it again
-- is a mistake, and making a guess trusted is a promotion, not a second row.
-- Partial, so a retracted relation can be asserted afresh.
CREATE UNIQUE INDEX relations_live ON core.relations (from_node, to_node, kind)
    WHERE NOT retracted;
-- The unique index above serves walks along outgoing edges, this one incoming.
CREATE INDEX relations_to ON core.relations (to_node) WHERE NOT retracted;

-- Relation events name the relation. The column goes on the existing log, so
-- one ordered history still covers everything.
ALTER TABLE core.events ADD COLUMN relation_id uuid;
CREATE INDEX events_relation ON core.events (relation_id, seq);

-- Published read schema -------------------------------------------------------

-- OR REPLACE may only append columns, so relation_id goes last. Grants on the
-- view survive the replace.
CREATE OR REPLACE VIEW read.events AS
    SELECT e.seq, e.at, p.name AS principal, e.agent, e.kind,
           e.document_id, e.block_id, e.proposal_id, e.payload, e.relation_id
    FROM core.events e JOIN core.principals p ON p.id = e.principal_id;

-- Live relations between live nodes. A relation to a block that has since been
-- deleted stays in core (and in the log), but drops out here, just as the
-- block drops out of read.blocks.
CREATE VIEW read.relations AS
    SELECT r.id, r.from_node,
           CASE WHEN r.from_block IS NULL THEN 'document' ELSE 'block' END AS from_kind,
           r.to_node,
           CASE WHEN r.to_block IS NULL THEN 'document' ELSE 'block' END AS to_kind,
           r.kind, r.tier::text AS tier, r.confidence, r.note,
           p.name AS asserted_by, r.agent, r.created_at,
           pp.name AS promoted_by, r.promoted_at
    FROM core.relations r
    JOIN core.principals p ON p.id = r.asserted_by
    LEFT JOIN core.principals pp ON pp.id = r.promoted_by
    WHERE NOT r.retracted
      AND NOT EXISTS (
          SELECT FROM core.blocks b
          WHERE b.id IN (r.from_block, r.to_block) AND b.deleted
      );

-- `GRANT ... ON ALL TABLES IN SCHEMA read` in 0001 covered the views that
-- existed then, not later ones. Every new read view needs its own grant.
GRANT SELECT ON read.relations TO archivar_reader;
