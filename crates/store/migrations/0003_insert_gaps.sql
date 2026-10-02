-- Inserts pin the gap they land in.
--
-- An insert's position sits between two existing blocks (or before the first,
-- or after the last). Positions of existing blocks never change, so the insert
-- is still placed as reviewed exactly when both ends of that gap are still live
-- and nothing has landed between them since. These columns name the ends, NULL
-- for the open end at the start or the end of the document.
--
-- Open proposals made before this migration have no gap recorded and so read
-- as "the document was empty"; their inserts show up stale. Re-propose them.
ALTER TABLE core.proposal_changes
    ADD COLUMN gap_before uuid,
    ADD COLUMN gap_after  uuid;

-- Both ends still live and nothing landed in between.
CREATE FUNCTION core.gap_is_open(doc uuid, before uuid, after uuid)
RETURNS boolean LANGUAGE sql STABLE AS $$
    WITH ends AS (
        SELECT (SELECT position FROM core.blocks WHERE id = before AND NOT deleted) AS lo,
               (SELECT position FROM core.blocks WHERE id = after AND NOT deleted) AS hi
    )
    SELECT (before IS NULL OR lo IS NOT NULL)
       AND (after IS NULL OR hi IS NOT NULL)
       AND NOT EXISTS (
           SELECT FROM core.blocks b
           WHERE b.document_id = doc AND NOT b.deleted
             AND (before IS NULL OR b.position > lo)
             AND (after IS NULL OR b.position < hi)
       )
    FROM ends
$$;

-- Whether a change still applies as it was reviewed: an update or delete when
-- its block is still at the version it was based on, an insert when its gap is
-- open. The one definition behind both the `stale` mark in review and the
-- refusal in commit, so the two can never disagree.
CREATE FUNCTION core.change_is_fresh(c core.proposal_changes, doc uuid)
RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT CASE WHEN c.op = 'insert'
        THEN core.gap_is_open(doc, c.gap_before, c.gap_after)
        ELSE EXISTS (
            SELECT FROM core.blocks b
            WHERE b.id = c.block_id AND NOT b.deleted AND b.version = c.base_version
        )
    END
$$;

CREATE OR REPLACE VIEW read.proposal_changes AS
    SELECT proposal_id, seq, op, block_id, base_version, kind, body,
           gap_before, gap_after
    FROM core.proposal_changes;
