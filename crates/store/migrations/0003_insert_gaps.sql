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

CREATE OR REPLACE VIEW read.proposal_changes AS
    SELECT proposal_id, seq, op, block_id, base_version, kind, body,
           gap_before, gap_after
    FROM core.proposal_changes;
