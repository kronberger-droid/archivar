//! Relations: typed, tiered links between documents and blocks.
//!
//! A relation is an assertion with provenance. Writing one takes the commit
//! right in its tier, so an agent links into `derived` directly and only a
//! human hand can [`promote`](Store::promote) a link to `canonical`.

use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::{Actor, Error, EventCtx, Result, Store, Tier, forbidden, principal, rights};

/// What to link. Either end may be a document or a block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewRelation {
    pub from: Uuid,
    pub to: Uuid,
    /// Free-form type of the link, like `cites` or `supersedes`.
    pub kind: String,
    pub tier: Tier,
    /// How sure the asserter is, from 0 to 1. Mostly for agent guesses.
    #[serde(default)]
    pub confidence: Option<f64>,
    #[serde(default)]
    pub note: Option<String>,
}

/// Which edges a walk follows, seen from the node it is standing on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Out,
    In,
    Both,
}

impl Direction {
    pub fn as_str(self) -> &'static str {
        match self {
            Direction::Out => "out",
            Direction::In => "in",
            Direction::Both => "both",
        }
    }
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Direction {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "out" => Ok(Direction::Out),
            "in" => Ok(Direction::In),
            "both" => Ok(Direction::Both),
            other => Err(Error::Invalid(format!("unknown direction `{other}`"))),
        }
    }
}

/// One relation reached by [`Store::related`], with the node it leads to.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct Related {
    pub relation_id: Uuid,
    /// Steps from the start node; 1 for its direct relations.
    pub depth: i32,
    /// `out` when the relation points away from the node the walk came from.
    pub direction: String,
    /// Set when the step left a document by a relation on one of its blocks:
    /// that block.
    pub through: Option<Uuid>,
    /// The node this step arrived at.
    pub node: Uuid,
    /// `document` or `block`.
    pub node_kind: String,
    /// The document's title, or the start of the block's text.
    pub node_label: String,
    pub from_node: Uuid,
    pub to_node: Uuid,
    pub kind: String,
    pub tier: String,
    pub confidence: Option<f64>,
    pub note: Option<String>,
    pub asserted_by: String,
    pub agent: Option<String>,
    pub created_at: DateTime<Utc>,
    pub promoted_by: Option<String>,
    pub promoted_at: Option<DateTime<Utc>>,
}

/// A live end of a relation.
enum Node {
    Document(Uuid),
    Block(Uuid),
}

impl Store {
    /// Assert a relation. Needs the commit right in its tier.
    pub async fn link(&self, actor: &Actor, rel: &NewRelation) -> Result<Uuid> {
        if rel.tier == Tier::Raw {
            return Err(Error::Invalid("relations cannot be raw".into()));
        }
        if rel.from == rel.to {
            return Err(Error::Invalid("a node cannot relate to itself".into()));
        }
        if rel.kind.trim().is_empty() {
            return Err(Error::Invalid("a relation needs a kind".into()));
        }
        if let Some(c) = rel.confidence
            && !(0.0..=1.0).contains(&c)
        {
            return Err(Error::Invalid(format!("confidence {c} is not in 0..=1")));
        }

        let mut tx = self.pool.begin().await?;
        let principal = principal(&mut tx, actor).await?;
        if !rights(&mut tx, &principal, actor, rel.tier).await?.commit {
            return Err(forbidden(actor, "link", rel.tier));
        }
        let from = node(&mut *tx, rel.from).await?;
        let to = node(&mut *tx, rel.to).await?;

        // The unique index would refuse this too, but with a database error
        // that doesn't say which relation is in the way or what to do.
        let existing: Option<(Uuid, String)> = sqlx::query_as(
            "SELECT id, tier::text FROM core.relations
             WHERE from_node = $1 AND to_node = $2 AND kind = $3 AND NOT retracted",
        )
        .bind(rel.from)
        .bind(rel.to)
        .bind(&rel.kind)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some((id, tier)) = existing {
            return Err(Error::Invalid(format!(
                "already linked as relation {id} in the {tier} tier; promote or unlink it instead"
            )));
        }

        let id = Uuid::now_v7();
        let (from_document, from_block) = from.columns();
        let (to_document, to_block) = to.columns();
        sqlx::query(
            "INSERT INTO core.relations
                 (id, from_document, from_block, to_document, to_block,
                  kind, tier, confidence, note, asserted_by, agent)
             VALUES ($1, $2, $3, $4, $5, $6, $7::core.tier, $8, $9, $10, $11)",
        )
        .bind(id)
        .bind(from_document)
        .bind(from_block)
        .bind(to_document)
        .bind(to_block)
        .bind(&rel.kind)
        .bind(rel.tier.as_str())
        .bind(rel.confidence)
        .bind(&rel.note)
        .bind(principal.id)
        .bind(&actor.agent)
        .execute(&mut *tx)
        .await?;

        EventCtx::relation(&principal, actor, id)
            .log(&mut tx, "relation_asserted", None, json!(rel))
            .await?;
        tx.commit().await?;
        Ok(id)
    }

    /// Retract a relation. It stays in core and in the log, but no longer
    /// shows up anywhere live. Needs the commit right in its tier.
    pub async fn unlink(&self, actor: &Actor, relation: Uuid) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let principal = principal(&mut tx, actor).await?;
        let tier = live_relation_tier(&mut tx, relation).await?;
        if !rights(&mut tx, &principal, actor, tier).await?.commit {
            return Err(forbidden(actor, "unlink", tier));
        }
        sqlx::query("UPDATE core.relations SET retracted = true WHERE id = $1")
            .bind(relation)
            .execute(&mut *tx)
            .await?;
        EventCtx::relation(&principal, actor, relation)
            .log(&mut tx, "relation_retracted", None, json!({ "tier": tier }))
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Make a relation canonical. This is the approval of a guess, so it takes
    /// the commit right in canonical, which no agent has, and is recorded as
    /// `promoted_by` next to the original asserter.
    pub async fn promote(&self, actor: &Actor, relation: Uuid) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let principal = principal(&mut tx, actor).await?;
        let tier = live_relation_tier(&mut tx, relation).await?;
        if tier == Tier::Canonical {
            return Err(Error::Invalid(format!(
                "relation {relation} is already canonical"
            )));
        }
        if !rights(&mut tx, &principal, actor, Tier::Canonical)
            .await?
            .commit
        {
            return Err(forbidden(actor, "promote", Tier::Canonical));
        }
        sqlx::query(
            "UPDATE core.relations
             SET tier = 'canonical', promoted_by = $2, promoted_at = now()
             WHERE id = $1",
        )
        .bind(relation)
        .bind(principal.id)
        .execute(&mut *tx)
        .await?;
        EventCtx::relation(&principal, actor, relation)
            .log(
                &mut tx,
                "relation_promoted",
                None,
                json!({ "from": tier, "to": Tier::Canonical }),
            )
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Walk the relation graph from a document or block, up to `depth` steps.
    ///
    /// Standing on a document, the walk also follows relations on its blocks,
    /// and says so in [`Related::through`]. Standing on a block, it follows
    /// only that block's own relations.
    ///
    /// Every relation reached comes back once, at the shallowest depth it was
    /// found. `tiers` limits which relations the walk may follow, so a walk
    /// over `[Canonical]` only crosses human-approved links; empty means all.
    pub async fn related(
        &self,
        start: Uuid,
        depth: u32,
        direction: Direction,
        tiers: &[Tier],
    ) -> Result<Vec<Related>> {
        node(&self.pool, start).await?;
        let tiers: Option<Vec<&str>> =
            (!tiers.is_empty()).then(|| tiers.iter().map(|t| t.as_str()).collect());

        Ok(sqlx::query_as(
            "WITH RECURSIVE
             followed AS (
                 SELECT id, from_node, to_node FROM read.relations
                 WHERE $4::text[] IS NULL OR tier = ANY($4)
             ),
             -- Every followed relation as an edge leaving `node`, once per
             -- direction the walk may cross it. Normalising here means the
             -- recursive part below joins one edge list, which matters: a
             -- recursive CTE may refer to itself only once, so it can't take
             -- `out` and `in` as two separate branches.
             direct AS (
                 SELECT id, from_node AS node, to_node AS other, 'out' AS direction
                 FROM followed WHERE $3 IN ('out', 'both')
                 UNION ALL
                 SELECT id, to_node, from_node, 'in'
                 FROM followed WHERE $3 IN ('in', 'both')
             ),
             -- A document contains its blocks, so an edge leaving a block also
             -- leaves its document. `through` keeps the block it really sits on.
             edges AS (
                 SELECT id, node, other, direction, NULL::uuid AS through
                 FROM direct
                 UNION ALL
                 SELECT e.id, b.document_id, e.other, e.direction, e.node
                 FROM direct e
                 JOIN core.blocks b ON b.id = e.node
             ),
             -- The anchor takes the first step, the recursive part every next
             -- one. `path` holds the nodes already on this walk. A step back onto
             -- one of them is still reported, since that relation is as related
             -- as any, but marks the row `closed` and the walk stops there. That
             -- ends cycles; `depth` ends everything else. (Postgres 14 has a
             -- CYCLE clause for exactly this; spelled out here to show it.)
             walk AS (
                 SELECT e.id AS relation_id, e.other AS node, e.direction, e.through,
                        1 AS depth, ARRAY[$1::uuid, e.other] AS path,
                        e.other = $1 AS closed
                 FROM edges e
                 WHERE e.node = $1
                 UNION ALL
                 SELECT e.id, e.other, e.direction, e.through, w.depth + 1,
                        w.path || e.other, e.other = ANY(w.path)
                 FROM walk w
                 JOIN edges e ON e.node = w.node
                 WHERE w.depth < $2 AND NOT w.closed
             ),
             -- Several paths can reach the same relation; keep the shortest,
             -- and at equal depth the one that didn't go through a block.
             -- DISTINCT ON keeps the first row per relation in ORDER BY order.
             nearest AS (
                 SELECT DISTINCT ON (relation_id) relation_id, node, direction, through, depth
                 FROM walk
                 ORDER BY relation_id, depth, through NULLS FIRST
             )
             SELECT n.relation_id, n.depth, n.direction, n.through, n.node,
                    CASE WHEN d.id IS NULL THEN 'block' ELSE 'document' END AS node_kind,
                    coalesce(d.title, left(b.body, 80)) AS node_label,
                    r.from_node, r.to_node, r.kind, r.tier, r.confidence, r.note,
                    r.asserted_by, r.agent, r.created_at, r.promoted_by, r.promoted_at
             FROM nearest n
             JOIN read.relations r ON r.id = n.relation_id
             LEFT JOIN core.documents d ON d.id = n.node
             LEFT JOIN core.blocks b ON b.id = n.node
             ORDER BY n.depth, r.created_at",
        )
        .bind(start)
        .bind(depth as i32)
        .bind(direction.as_str())
        .bind(tiers)
        .fetch_all(&self.pool)
        .await?)
    }
}

impl Node {
    /// As `(document, block)` foreign key columns, exactly one of them set.
    fn columns(&self) -> (Option<Uuid>, Option<Uuid>) {
        match *self {
            Node::Document(id) => (Some(id), None),
            Node::Block(id) => (None, Some(id)),
        }
    }
}

/// Look up what an id names. Deleted blocks don't count: nothing new should
/// point at them.
async fn node(conn: impl sqlx::PgExecutor<'_>, id: Uuid) -> Result<Node> {
    let kind: Option<String> = sqlx::query_scalar(
        "SELECT 'document' FROM core.documents WHERE id = $1
         UNION ALL
         SELECT 'block' FROM core.blocks WHERE id = $1 AND NOT deleted",
    )
    .bind(id)
    .fetch_optional(conn)
    .await?;
    match kind.as_deref() {
        Some("document") => Ok(Node::Document(id)),
        Some(_) => Ok(Node::Block(id)),
        None => Err(Error::NotFound(format!("document or block {id}"))),
    }
}

/// The tier of a relation that is still live, locking it for the caller's
/// transaction.
async fn live_relation_tier(conn: &mut PgConnection, relation: Uuid) -> Result<Tier> {
    let tier: Option<String> = sqlx::query_scalar(
        "SELECT tier::text FROM core.relations WHERE id = $1 AND NOT retracted FOR UPDATE",
    )
    .bind(relation)
    .fetch_optional(conn)
    .await?;
    tier.ok_or_else(|| Error::NotFound(format!("relation {relation}")))?
        .parse()
}
