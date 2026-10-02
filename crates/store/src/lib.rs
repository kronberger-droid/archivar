//! Storage core of archivar.
//!
//! Every write goes through a proposal or an ingest, runs in one transaction
//! together with the event that explains it, and is checked against the
//! role x tier matrix in `core.acl`. Reads for humans and agents go through the
//! typed functions here or through [`Store::query`], which only sees the
//! published `read` schema.

pub mod markdown;
mod relations;

pub use relations::{Direction, NewRelation, Related};

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{AssertSqlSafe, PgConnection};
use uuid::Uuid;

/// The login role [`Store::query`] runs as. Created by the first migration.
pub const READER_ROLE: &str = "archivar_reader";

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    #[error(transparent)]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("{0} not found")]
    NotFound(String),
    #[error("{actor} may not {action} in the {tier} tier")]
    Forbidden {
        actor: Actor,
        action: &'static str,
        tier: Tier,
    },
    #[error("raw documents are immutable")]
    Immutable,
    #[error("proposal is stale: {} block(s) changed since it was made", blocks.len())]
    Conflict { blocks: Vec<Uuid> },
    #[error("{0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    Raw,
    Canonical,
    Working,
    Derived,
}

impl Tier {
    pub fn as_str(self) -> &'static str {
        match self {
            Tier::Raw => "raw",
            Tier::Canonical => "canonical",
            Tier::Working => "working",
            Tier::Derived => "derived",
        }
    }
}

impl fmt::Display for Tier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Tier {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "raw" => Ok(Tier::Raw),
            "canonical" => Ok(Tier::Canonical),
            "working" => Ok(Tier::Working),
            "derived" => Ok(Tier::Derived),
            other => Err(Error::Invalid(format!("unknown tier `{other}`"))),
        }
    }
}

/// Who is acting: always a principal, and optionally an agent acting on their
/// behalf. Both end up on every event, so blame can tell them apart.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Actor {
    pub principal: String,
    pub agent: Option<String>,
}

impl Actor {
    pub fn human(principal: &str) -> Self {
        Self {
            principal: principal.to_owned(),
            agent: None,
        }
    }

    pub fn via(principal: &str, agent: &str) -> Self {
        Self {
            principal: principal.to_owned(),
            agent: Some(agent.to_owned()),
        }
    }
}

impl fmt::Display for Actor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.agent {
            Some(agent) => write!(f, "{} via {agent}", self.principal),
            None => f.write_str(&self.principal),
        }
    }
}

/// One edit inside a proposal. Bodies must parse as exactly one markdown block.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum Change {
    /// Insert a new block after `after`, or at the start when it is `None`.
    Insert {
        after: Option<Uuid>,
        body: String,
    },
    Update {
        block: Uuid,
        body: String,
    },
    Delete {
        block: Uuid,
    },
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct Document {
    pub id: Uuid,
    pub title: String,
    pub tier: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct Block {
    pub id: Uuid,
    pub position: f64,
    pub kind: String,
    pub body: String,
    pub version: i32,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct Proposal {
    pub id: Uuid,
    pub document_id: Uuid,
    pub principal: String,
    pub agent: Option<String>,
    pub note: Option<String>,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Review {
    pub proposal: Proposal,
    pub changes: Vec<ReviewedChange>,
    /// All changes as one unified diff, for humans.
    pub diff: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReviewedChange {
    pub op: String,
    pub block_id: Uuid,
    pub base_version: Option<i32>,
    pub current_version: Option<i32>,
    pub old_body: Option<String>,
    pub new_body: Option<String>,
    /// The block moved on since the proposal was made.
    pub stale: bool,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct HistoryEntry {
    pub seq: i64,
    pub at: DateTime<Utc>,
    pub kind: String,
    pub principal: String,
    pub agent: Option<String>,
    pub proposal_id: Option<Uuid>,
    pub proposed_by: Option<String>,
    pub proposed_via: Option<String>,
    pub version: Option<i32>,
    pub body: Option<String>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct BlameLine {
    pub block_id: Uuid,
    pub kind: String,
    pub version: i32,
    pub body: String,
    pub at: DateTime<Utc>,
    /// Who made the change land: the ingesting or committing principal.
    pub committed_by: String,
    pub committed_via: Option<String>,
    /// Who wrote it, when it came through a proposal.
    pub proposed_by: Option<String>,
    pub proposed_via: Option<String>,
}

#[derive(Debug, Clone, Copy)]
/// Reads are not checked yet: every role reads every tier in the MVP, and the
/// published `read` schema is where a tier filter will go.
struct Rights {
    propose: bool,
    commit: bool,
}

struct Principal {
    id: Uuid,
    role: String,
}

pub struct Store {
    pool: PgPool,
    reader: PgPool,
}

impl Store {
    pub async fn connect(url: &str) -> Result<Self> {
        Ok(Self::from_pool(PgPool::connect(url).await?))
    }

    /// The reader pool reuses the same server and database but logs in as
    /// [`READER_ROLE`]. A separate login, not `SET ROLE`, since a session that
    /// switched roles can switch back with `RESET ROLE`, and the SQL running
    /// there is not ours.
    pub fn from_pool(pool: PgPool) -> Self {
        let reader_opts = (*pool.connect_options()).clone().username(READER_ROLE);
        let reader = PgPoolOptions::new()
            .max_connections(4)
            .connect_lazy_with(reader_opts);
        Self { pool, reader }
    }

    pub async fn migrate(&self) -> Result<()> {
        sqlx::migrate!().run(&self.pool).await?;
        Ok(())
    }

    /// Bootstrap operation for the CLI. There is no actor check yet: whoever
    /// holds the database URL is admin.
    pub async fn add_principal(&self, name: &str, role: &str) -> Result<Uuid> {
        let id = Uuid::now_v7();
        sqlx::query("INSERT INTO core.principals (id, name, role) VALUES ($1, $2, $3)")
            .bind(id)
            .bind(name)
            .bind(role)
            .execute(&self.pool)
            .await?;
        Ok(id)
    }

    // Documents -----------------------------------------------------------

    pub async fn ingest(
        &self,
        actor: &Actor,
        title: &str,
        tier: Tier,
        source: &str,
    ) -> Result<Uuid> {
        let mut tx = self.pool.begin().await?;
        let principal = principal(&mut tx, actor).await?;
        if !rights(&mut tx, &principal, actor, tier).await?.commit {
            return Err(forbidden(actor, "ingest", tier));
        }

        let doc = Uuid::now_v7();
        sqlx::query("INSERT INTO core.documents (id, title, tier) VALUES ($1, $2, $3::core.tier)")
            .bind(doc)
            .bind(title)
            .bind(tier.as_str())
            .execute(&mut *tx)
            .await?;
        let event = EventCtx {
            principal: &principal,
            actor,
            document: Some(doc),
            relation: None,
            proposal: None,
        };
        event
            .log(
                &mut tx,
                "document_created",
                None,
                json!({ "title": title, "tier": tier }),
            )
            .await?;

        for (i, block) in markdown::split(source).into_iter().enumerate() {
            let position = (i + 1) as f64;
            let id = Uuid::now_v7();
            sqlx::query(
                "INSERT INTO core.blocks (id, document_id, position, kind, body)
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(id)
            .bind(doc)
            .bind(position)
            .bind(&block.kind)
            .bind(&block.body)
            .execute(&mut *tx)
            .await?;
            event
                .log(
                    &mut tx,
                    "block_set",
                    Some(id),
                    json!({ "kind": block.kind, "body": block.body, "position": position, "version": 1 }),
                )
                .await?;
        }

        tx.commit().await?;
        Ok(doc)
    }

    pub async fn document(&self, doc: Uuid) -> Result<Document> {
        sqlx::query_as(
            "SELECT id, title, tier::text AS tier, created_at FROM core.documents WHERE id = $1",
        )
        .bind(doc)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| Error::NotFound(format!("document {doc}")))
    }

    pub async fn blocks(&self, doc: Uuid) -> Result<Vec<Block>> {
        Ok(sqlx::query_as(
            "SELECT id, position, kind, body, version FROM core.blocks
             WHERE document_id = $1 AND NOT deleted ORDER BY position",
        )
        .bind(doc)
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn block_document(&self, block: Uuid) -> Result<Uuid> {
        sqlx::query_scalar("SELECT document_id FROM core.blocks WHERE id = $1")
            .bind(block)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| Error::NotFound(format!("block {block}")))
    }

    pub async fn materialize(&self, doc: Uuid) -> Result<String> {
        self.document(doc).await?;
        let blocks = self.blocks(doc).await?;
        Ok(markdown::render(blocks.iter().map(|b| b.body.as_str())))
    }

    // Proposals -----------------------------------------------------------

    pub async fn propose(
        &self,
        actor: &Actor,
        doc: Uuid,
        note: Option<&str>,
        changes: &[Change],
    ) -> Result<Uuid> {
        if changes.is_empty() {
            return Err(Error::Invalid(
                "a proposal needs at least one change".into(),
            ));
        }
        let mut tx = self.pool.begin().await?;
        let principal = principal(&mut tx, actor).await?;
        let tier = document_tier(&mut tx, doc).await?;
        if tier == Tier::Raw {
            return Err(Error::Immutable);
        }
        if !rights(&mut tx, &principal, actor, tier).await?.propose {
            return Err(forbidden(actor, "propose", tier));
        }

        let proposal = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO core.proposals (id, document_id, principal_id, agent, note)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(proposal)
        .bind(doc)
        .bind(principal.id)
        .bind(&actor.agent)
        .bind(note)
        .execute(&mut *tx)
        .await?;

        // Positions of every block the proposal can refer to, including the
        // ones it inserts itself, so a later insert can anchor on an earlier one.
        let current: Vec<Block> = sqlx::query_as(
            "SELECT id, position, kind, body, version FROM core.blocks
             WHERE document_id = $1 AND NOT deleted",
        )
        .bind(doc)
        .fetch_all(&mut *tx)
        .await?;
        let versions: HashMap<Uuid, i32> = current.iter().map(|b| (b.id, b.version)).collect();
        let mut positions: HashMap<Uuid, f64> =
            current.iter().map(|b| (b.id, b.position)).collect();

        for (seq, change) in changes.iter().enumerate() {
            let (op, block, base_version, body, position) = match change {
                Change::Update { block, body } => {
                    let version = *versions.get(block).ok_or_else(|| {
                        Error::NotFound(format!("block {block} in document {doc}"))
                    })?;
                    ("update", *block, Some(version), Some(body), None)
                }
                Change::Delete { block } => {
                    let version = *versions.get(block).ok_or_else(|| {
                        Error::NotFound(format!("block {block} in document {doc}"))
                    })?;
                    ("delete", *block, Some(version), None, None)
                }
                Change::Insert { after, body } => {
                    let position = insert_position(&positions, *after)?;
                    let block = Uuid::now_v7();
                    positions.insert(block, position);
                    ("insert", block, None, Some(body), Some(position))
                }
            };
            // The gap among the blocks that exist now, not the ones this
            // proposal adds: that is what another commit can disturb.
            let (gap_before, gap_after) = match position {
                Some(position) => gap_around(&current, position),
                None => (None, None),
            };
            let kind = match body {
                Some(body) => Some(
                    markdown::single_block(body)
                        .ok_or_else(|| {
                            Error::Invalid(format!(
                                "change {seq}: body must be exactly one markdown block"
                            ))
                        })?
                        .kind,
                ),
                None => None,
            };
            sqlx::query(
                "INSERT INTO core.proposal_changes
                     (proposal_id, seq, op, block_id, base_version, kind, body, position,
                      gap_before, gap_after)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            )
            .bind(proposal)
            .bind(seq as i32)
            .bind(op)
            .bind(block)
            .bind(base_version)
            .bind(kind)
            .bind(body.map(|b| b.trim_end()))
            .bind(position)
            .bind(gap_before)
            .bind(gap_after)
            .execute(&mut *tx)
            .await?;
        }

        EventCtx {
            principal: &principal,
            actor,
            document: Some(doc),
            relation: None,
            proposal: Some(proposal),
        }
        .log(
            &mut tx,
            "proposal_created",
            None,
            json!({ "changes": changes.len(), "note": note }),
        )
        .await?;

        tx.commit().await?;
        Ok(proposal)
    }

    pub async fn proposals(&self, doc: Option<Uuid>) -> Result<Vec<Proposal>> {
        Ok(sqlx::query_as(
            "SELECT pr.id, pr.document_id, p.name AS principal, pr.agent, pr.note,
                    pr.status, pr.created_at
             FROM core.proposals pr JOIN core.principals p ON p.id = pr.principal_id
             WHERE pr.status = 'open' AND ($1::uuid IS NULL OR pr.document_id = $1)
             ORDER BY pr.created_at",
        )
        .bind(doc)
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn review(&self, proposal: Uuid) -> Result<Review> {
        let info: Proposal = sqlx::query_as(
            "SELECT pr.id, pr.document_id, p.name AS principal, pr.agent, pr.note,
                    pr.status, pr.created_at
             FROM core.proposals pr JOIN core.principals p ON p.id = pr.principal_id
             WHERE pr.id = $1",
        )
        .bind(proposal)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| Error::NotFound(format!("proposal {proposal}")))?;

        #[derive(sqlx::FromRow)]
        struct Row {
            op: String,
            block_id: Uuid,
            base_version: Option<i32>,
            new_body: Option<String>,
            current_version: Option<i32>,
            current_deleted: Option<bool>,
            old_body: Option<String>,
            gap_before: Option<Uuid>,
            gap_after: Option<Uuid>,
        }
        let rows: Vec<Row> = sqlx::query_as(
            "SELECT c.op, c.block_id, c.base_version, c.body AS new_body,
                    b.version AS current_version, b.deleted AS current_deleted,
                    old.payload->>'body' AS old_body, c.gap_before, c.gap_after
             FROM core.proposal_changes c
             LEFT JOIN core.blocks b ON b.id = c.block_id
             -- The body the proposal was based on, from the log, not the
             -- current state: the reviewer should see what the author saw.
             LEFT JOIN LATERAL (
                 SELECT e.payload FROM core.events e
                 WHERE e.block_id = c.block_id AND e.kind = 'block_set'
                   AND (e.payload->>'version')::int = c.base_version
                 ORDER BY e.seq DESC LIMIT 1
             ) old ON true
             WHERE c.proposal_id = $1
             ORDER BY c.seq",
        )
        .bind(proposal)
        .fetch_all(&self.pool)
        .await?;

        let mut conn = self.pool.acquire().await?;
        let mut diff = String::new();
        let mut changes = Vec::with_capacity(rows.len());
        for r in rows {
            let stale = if r.op == "insert" {
                !gap_is_open(&mut conn, info.document_id, r.gap_before, r.gap_after).await?
            } else {
                r.current_deleted == Some(true) || r.current_version != r.base_version
            };
            let old = r.old_body.as_deref().unwrap_or("");
            let new = r.new_body.as_deref().unwrap_or("");
            let header = format!(
                "{} {}{}",
                r.op,
                r.block_id,
                if stale { " (stale)" } else { "" }
            );
            diff.push_str(&format!(
                "{}",
                similar::TextDiff::from_lines(format!("{old}\n"), format!("{new}\n"))
                    .unified_diff()
                    .header(&header, &header)
            ));
            changes.push(ReviewedChange {
                op: r.op,
                block_id: r.block_id,
                base_version: r.base_version,
                current_version: r.current_version,
                old_body: r.old_body,
                new_body: r.new_body,
                stale,
            });
        }

        Ok(Review {
            proposal: info,
            changes,
            diff,
        })
    }

    /// Apply a proposal. All or nothing: what was reviewed is the whole diff,
    /// so a proposal with any stale block lands nowhere and the error lists
    /// every stale block.
    pub async fn commit(&self, actor: &Actor, proposal: Uuid) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let principal = principal(&mut tx, actor).await?;
        let (doc, status): (Uuid, String) = sqlx::query_as(
            "SELECT document_id, status FROM core.proposals WHERE id = $1 FOR UPDATE",
        )
        .bind(proposal)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| Error::NotFound(format!("proposal {proposal}")))?;
        if status != "open" {
            return Err(Error::Invalid(format!(
                "proposal {proposal} is already {status}"
            )));
        }
        let tier = document_tier(&mut tx, doc).await?;
        if tier == Tier::Raw {
            return Err(Error::Immutable);
        }
        if !rights(&mut tx, &principal, actor, tier).await?.commit {
            return Err(forbidden(actor, "commit", tier));
        }

        #[derive(sqlx::FromRow)]
        struct Row {
            op: String,
            block_id: Uuid,
            base_version: Option<i32>,
            kind: Option<String>,
            body: Option<String>,
            position: Option<f64>,
            gap_before: Option<Uuid>,
            gap_after: Option<Uuid>,
        }
        let changes: Vec<Row> = sqlx::query_as(
            "SELECT op, block_id, base_version, kind, body, position, gap_before, gap_after
             FROM core.proposal_changes WHERE proposal_id = $1 ORDER BY seq",
        )
        .bind(proposal)
        .fetch_all(&mut *tx)
        .await?;

        // One commit per document at a time. Locking the blocks a proposal
        // touches covers updates and deletes, but an insert's gap is defined by
        // blocks that don't exist yet, and two commits could each find the
        // same gap empty. Waiting on the document row closes that, and under
        // READ COMMITTED every check below then sees whatever the commit ahead
        // of us wrote.
        sqlx::query("SELECT FROM core.documents WHERE id = $1 FOR UPDATE")
            .bind(doc)
            .execute(&mut *tx)
            .await?;

        let mut stale = Vec::new();
        for c in &changes {
            let fresh = match c.op.as_str() {
                "insert" => gap_is_open(&mut tx, doc, c.gap_before, c.gap_after).await?,
                _ => {
                    let current: Option<(i32, bool)> =
                        sqlx::query_as("SELECT version, deleted FROM core.blocks WHERE id = $1")
                            .bind(c.block_id)
                            .fetch_optional(&mut *tx)
                            .await?;
                    matches!(current, Some((version, false)) if Some(version) == c.base_version)
                }
            };
            if !fresh {
                stale.push(c.block_id);
            }
        }
        if !stale.is_empty() {
            return Err(Error::Conflict { blocks: stale });
        }

        let event = EventCtx {
            principal: &principal,
            actor,
            document: Some(doc),
            relation: None,
            proposal: Some(proposal),
        };
        for c in &changes {
            match c.op.as_str() {
                "update" => {
                    let version: i32 = sqlx::query_scalar(
                        "UPDATE core.blocks SET kind = $2, body = $3, version = version + 1,
                                updated_at = now()
                         WHERE id = $1 RETURNING version",
                    )
                    .bind(c.block_id)
                    .bind(&c.kind)
                    .bind(&c.body)
                    .fetch_one(&mut *tx)
                    .await?;
                    event
                        .log(
                            &mut tx,
                            "block_set",
                            Some(c.block_id),
                            json!({ "kind": c.kind, "body": c.body, "version": version }),
                        )
                        .await?;
                }
                "delete" => {
                    let version: i32 = sqlx::query_scalar(
                        "UPDATE core.blocks SET deleted = true, version = version + 1,
                                updated_at = now()
                         WHERE id = $1 RETURNING version",
                    )
                    .bind(c.block_id)
                    .fetch_one(&mut *tx)
                    .await?;
                    event
                        .log(
                            &mut tx,
                            "block_deleted",
                            Some(c.block_id),
                            json!({ "version": version }),
                        )
                        .await?;
                }
                "insert" => {
                    sqlx::query(
                        "INSERT INTO core.blocks (id, document_id, position, kind, body)
                         VALUES ($1, $2, $3, $4, $5)",
                    )
                    .bind(c.block_id)
                    .bind(doc)
                    .bind(c.position)
                    .bind(&c.kind)
                    .bind(&c.body)
                    .execute(&mut *tx)
                    .await?;
                    event
                        .log(
                            &mut tx,
                            "block_set",
                            Some(c.block_id),
                            json!({ "kind": c.kind, "body": c.body, "position": c.position, "version": 1 }),
                        )
                        .await?;
                }
                other => unreachable!("op `{other}` passed the CHECK constraint"),
            }
        }

        decide(&mut tx, proposal, &principal, "committed").await?;
        event
            .log(&mut tx, "proposal_committed", None, json!({}))
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Rejecting is a decision like committing, so it takes the same right.
    pub async fn reject(&self, actor: &Actor, proposal: Uuid) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let principal = principal(&mut tx, actor).await?;
        let (doc, status): (Uuid, String) = sqlx::query_as(
            "SELECT document_id, status FROM core.proposals WHERE id = $1 FOR UPDATE",
        )
        .bind(proposal)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| Error::NotFound(format!("proposal {proposal}")))?;
        if status != "open" {
            return Err(Error::Invalid(format!(
                "proposal {proposal} is already {status}"
            )));
        }
        let tier = document_tier(&mut tx, doc).await?;
        if !rights(&mut tx, &principal, actor, tier).await?.commit {
            return Err(forbidden(actor, "reject", tier));
        }
        decide(&mut tx, proposal, &principal, "rejected").await?;
        EventCtx {
            principal: &principal,
            actor,
            document: Some(doc),
            relation: None,
            proposal: Some(proposal),
        }
        .log(&mut tx, "proposal_rejected", None, json!({}))
        .await?;
        tx.commit().await?;
        Ok(())
    }

    // History -------------------------------------------------------------

    pub async fn history(&self, block: Uuid) -> Result<Vec<HistoryEntry>> {
        Ok(sqlx::query_as(
            "SELECT e.seq, e.at, e.kind, p.name AS principal, e.agent, e.proposal_id,
                    pp.name AS proposed_by, pr.agent AS proposed_via,
                    (e.payload->>'version')::int AS version, e.payload->>'body' AS body
             FROM core.events e
             JOIN core.principals p ON p.id = e.principal_id
             LEFT JOIN core.proposals pr ON pr.id = e.proposal_id
             LEFT JOIN core.principals pp ON pp.id = pr.principal_id
             WHERE e.block_id = $1
             ORDER BY e.seq",
        )
        .bind(block)
        .fetch_all(&self.pool)
        .await?)
    }

    /// The last change to every live block of a document, in document order.
    pub async fn blame(&self, doc: Uuid) -> Result<Vec<BlameLine>> {
        self.document(doc).await?;
        Ok(sqlx::query_as(
            "SELECT b.id AS block_id, b.kind, b.version, b.body,
                    last.at, last.committed_by, last.committed_via,
                    last.proposed_by, last.proposed_via
             FROM core.blocks b
             -- LATERAL runs the subquery once per block row, with b in scope:
             -- the newest event for exactly that block.
             JOIN LATERAL (
                 SELECT e.at, p.name AS committed_by, e.agent AS committed_via,
                        pp.name AS proposed_by, pr.agent AS proposed_via
                 FROM core.events e
                 JOIN core.principals p ON p.id = e.principal_id
                 LEFT JOIN core.proposals pr ON pr.id = e.proposal_id
                 LEFT JOIN core.principals pp ON pp.id = pr.principal_id
                 WHERE e.block_id = b.id
                 ORDER BY e.seq DESC
                 LIMIT 1
             ) last ON true
             WHERE b.document_id = $1 AND NOT b.deleted
             ORDER BY b.position",
        )
        .bind(doc)
        .fetch_all(&self.pool)
        .await?)
    }

    // Query ---------------------------------------------------------------

    /// Run one read-only SELECT against the published `read` schema and return
    /// the rows as a JSON array.
    ///
    /// Four layers keep this from writing anything or seeing more than `read`:
    /// the reader login has no rights outside `read`, the transaction is read
    /// only, the statement is wrapped as a subquery so only a query fits, and
    /// sqlx sends it as one prepared statement, which Postgres refuses to
    /// split on `;`.
    pub async fn query(&self, sql: &str) -> Result<serde_json::Value> {
        let sql = sql.trim().trim_end_matches(';');
        let mut tx = self.reader.begin().await?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await?;
        sqlx::query("SET LOCAL statement_timeout = '5s'")
            .execute(&mut *tx)
            .await?;
        sqlx::query("SET LOCAL search_path = read")
            .execute(&mut *tx)
            .await?;
        // `q.*` and not a bare `q`: a bare name resolves to a column first, so
        // a query with a column called `q` would aggregate that column instead
        // of the rows.
        let wrapped = format!("SELECT coalesce(json_agg(q.*), '[]'::json) FROM ({sql}\n) AS q");
        let rows: serde_json::Value = sqlx::query_scalar(AssertSqlSafe(wrapped))
            .fetch_one(&mut *tx)
            .await?;
        // Nothing to keep. Rolling back also undoes any `set_config` the query
        // might have smuggled in, so the pooled connection comes back clean.
        tx.rollback().await?;
        Ok(rows)
    }
}

// Helpers -----------------------------------------------------------------

struct EventCtx<'a> {
    principal: &'a Principal,
    actor: &'a Actor,
    document: Option<Uuid>,
    relation: Option<Uuid>,
    proposal: Option<Uuid>,
}

impl EventCtx<'_> {
    async fn log(
        &self,
        conn: &mut PgConnection,
        kind: &str,
        block: Option<Uuid>,
        payload: serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO core.events
                 (principal_id, agent, kind, document_id, block_id, proposal_id, relation_id, payload)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(self.principal.id)
        .bind(&self.actor.agent)
        .bind(kind)
        .bind(self.document)
        .bind(block)
        .bind(self.proposal)
        .bind(self.relation)
        .bind(payload)
        .execute(conn)
        .await?;
        Ok(())
    }
}

async fn principal(conn: &mut PgConnection, actor: &Actor) -> Result<Principal> {
    let row: Option<(Uuid, String)> =
        sqlx::query_as("SELECT id, role FROM core.principals WHERE name = $1")
            .bind(&actor.principal)
            .fetch_optional(conn)
            .await?;
    let (id, role) =
        row.ok_or_else(|| Error::NotFound(format!("principal {}", actor.principal)))?;
    Ok(Principal { id, role })
}

/// The principal's rights in a tier, capped by the `agent` role whenever an
/// agent acts for them. That cap is what keeps "Claude, commit this for me"
/// from counting as a human approval.
async fn rights(
    conn: &mut PgConnection,
    principal: &Principal,
    actor: &Actor,
    tier: Tier,
) -> Result<Rights> {
    let (propose, commit): (Option<bool>, Option<bool>) = sqlx::query_as(
        "SELECT bool_and(may_propose), bool_and(may_commit)
         FROM core.acl
         WHERE tier = $1::core.tier AND (role = $2 OR ($3 AND role = 'agent'))",
    )
    .bind(tier.as_str())
    .bind(&principal.role)
    .bind(actor.agent.is_some())
    .fetch_one(conn)
    .await?;
    // No matching row means no rights, never all rights.
    Ok(Rights {
        propose: propose.unwrap_or(false),
        commit: commit.unwrap_or(false),
    })
}

async fn document_tier(conn: &mut PgConnection, doc: Uuid) -> Result<Tier> {
    let tier: Option<String> =
        sqlx::query_scalar("SELECT tier::text FROM core.documents WHERE id = $1")
            .bind(doc)
            .fetch_optional(conn)
            .await?;
    tier.ok_or_else(|| Error::NotFound(format!("document {doc}")))?
        .parse()
}

async fn decide(
    conn: &mut PgConnection,
    proposal: Uuid,
    principal: &Principal,
    status: &str,
) -> Result<()> {
    sqlx::query(
        "UPDATE core.proposals SET status = $2, decided_by = $3, decided_at = now() WHERE id = $1",
    )
    .bind(proposal)
    .bind(status)
    .bind(principal.id)
    .execute(conn)
    .await?;
    Ok(())
}

fn forbidden(actor: &Actor, action: &'static str, tier: Tier) -> Error {
    Error::Forbidden {
        actor: actor.clone(),
        action,
        tier,
    }
}

/// Midpoint between the anchor and the next block, or one step past the end.
fn insert_position(positions: &HashMap<Uuid, f64>, after: Option<Uuid>) -> Result<f64> {
    let anchor = match after {
        Some(id) => *positions
            .get(&id)
            .ok_or_else(|| Error::NotFound(format!("anchor block {id}")))?,
        None => {
            let first = positions.values().copied().fold(f64::INFINITY, f64::min);
            return Ok(if first.is_finite() { first - 1.0 } else { 1.0 });
        }
    };
    let next = positions
        .values()
        .copied()
        .filter(|p| *p > anchor)
        .fold(f64::INFINITY, f64::min);
    Ok(if next.is_finite() {
        (anchor + next) / 2.0
    } else {
        anchor + 1.0
    })
}

/// The live blocks right before and after `position`, `None` at either end.
fn gap_around(blocks: &[Block], position: f64) -> (Option<Uuid>, Option<Uuid>) {
    let before = blocks
        .iter()
        .filter(|b| b.position < position)
        .max_by(|a, b| a.position.total_cmp(&b.position));
    let after = blocks
        .iter()
        .filter(|b| b.position > position)
        .min_by(|a, b| a.position.total_cmp(&b.position));
    (before.map(|b| b.id), after.map(|b| b.id))
}

/// Whether an insert's gap is as it was when the insert was proposed: both
/// ends still live and nothing landed in between. Existing blocks never move,
/// so that is exactly when its position still means what the reviewer saw.
async fn gap_is_open(
    conn: &mut PgConnection,
    doc: Uuid,
    before: Option<Uuid>,
    after: Option<Uuid>,
) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "WITH ends AS (
             SELECT (SELECT position FROM core.blocks WHERE id = $2 AND NOT deleted) AS lo,
                    (SELECT position FROM core.blocks WHERE id = $3 AND NOT deleted) AS hi
         )
         SELECT ($2::uuid IS NULL OR lo IS NOT NULL)
            AND ($3::uuid IS NULL OR hi IS NOT NULL)
            AND NOT EXISTS (
                SELECT FROM core.blocks b
                WHERE b.document_id = $1 AND NOT b.deleted
                  AND ($2::uuid IS NULL OR b.position > lo)
                  AND ($3::uuid IS NULL OR b.position < hi)
            )
         FROM ends",
    )
    .bind(doc)
    .bind(before)
    .bind(after)
    .fetch_one(conn)
    .await?)
}
