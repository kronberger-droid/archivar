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

/// What a stored proposal change does. Kept as text in the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Op {
    Insert,
    Update,
    Delete,
}

impl Op {
    pub fn as_str(self) -> &'static str {
        match self {
            Op::Insert => "insert",
            Op::Update => "update",
            Op::Delete => "delete",
        }
    }
}

impl TryFrom<String> for Op {
    type Error = Error;

    fn try_from(s: String) -> Result<Self> {
        match s.as_str() {
            "insert" => Ok(Op::Insert),
            "update" => Ok(Op::Update),
            "delete" => Ok(Op::Delete),
            other => Err(Error::Invalid(format!("unknown op `{other}`"))),
        }
    }
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

impl Proposal {
    /// Who made the proposal, and through which agent.
    pub fn author(&self) -> Actor {
        Actor {
            principal: self.principal.clone(),
            agent: self.agent.clone(),
        }
    }
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
    pub op: Op,
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
    /// The event payload for relation events, which have no version or body:
    /// what was asserted, or which tiers a promotion moved between.
    pub detail: Option<serde_json::Value>,
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
        let inserted =
            sqlx::query("INSERT INTO core.principals (id, name, role) VALUES ($1, $2, $3)")
                .bind(id)
                .bind(name)
                .bind(role)
                .execute(&self.pool)
                .await;
        // Roles are rows in core.roles, not a Rust enum, so the constraints are
        // what knows a role is unknown. Translate them instead of duplicating.
        match inserted {
            Ok(_) => Ok(id),
            Err(sqlx::Error::Database(e)) => match e.kind() {
                sqlx::error::ErrorKind::ForeignKeyViolation => {
                    Err(Error::Invalid(format!("unknown role `{role}`")))
                }
                sqlx::error::ErrorKind::UniqueViolation => {
                    Err(Error::Invalid(format!("principal `{name}` already exists")))
                }
                _ => Err(sqlx::Error::Database(e).into()),
            },
            Err(e) => Err(e.into()),
        }
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
        let event = EventCtx::document(&principal, actor, doc);
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
            insert_block(
                &mut tx,
                &event,
                doc,
                Uuid::now_v7(),
                position,
                &block.kind,
                &block.body,
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

        let current: Vec<(Uuid, f64, i32)> = sqlx::query_as(
            "SELECT id, position, version FROM core.blocks WHERE document_id = $1 AND NOT deleted",
        )
        .bind(doc)
        .fetch_all(&mut *tx)
        .await?;
        let versions: HashMap<Uuid, i32> = current.iter().map(|&(id, _, v)| (id, v)).collect();
        let base_version = |block: &Uuid| {
            versions
                .get(block)
                .copied()
                .ok_or_else(|| Error::NotFound(format!("block {block} in document {doc}")))
        };
        let existing: HashMap<Uuid, f64> = current.iter().map(|&(id, p, _)| (id, p)).collect();
        // Also the blocks this proposal inserts, so a later insert can anchor
        // on an earlier one.
        let mut positions = existing.clone();

        for (seq, change) in changes.iter().enumerate() {
            let (op, block, base_version, body, position) = match change {
                Change::Update { block, body } => (
                    Op::Update,
                    *block,
                    Some(base_version(block)?),
                    Some(body),
                    None,
                ),
                Change::Delete { block } => {
                    (Op::Delete, *block, Some(base_version(block)?), None, None)
                }
                Change::Insert { after, body } => {
                    let position = insert_position(&positions, *after)?;
                    let block = Uuid::now_v7();
                    positions.insert(block, position);
                    (Op::Insert, block, None, Some(body), Some(position))
                }
            };
            // The gap among the blocks that exist now, not the ones this
            // proposal adds: that is what another commit can disturb.
            let (gap_before, gap_after) = position
                .map(|p| gap_around(&existing, p))
                .unwrap_or_default();
            let kind = body
                .map(|body| {
                    markdown::single_block(body).map(|b| b.kind).ok_or_else(|| {
                        Error::Invalid(format!(
                            "change {seq}: body must be exactly one markdown block"
                        ))
                    })
                })
                .transpose()?;
            sqlx::query(
                "INSERT INTO core.proposal_changes
                     (proposal_id, seq, op, block_id, base_version, kind, body, position,
                      gap_before, gap_after)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            )
            .bind(proposal)
            .bind(seq as i32)
            .bind(op.as_str())
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

        EventCtx::proposal(&principal, actor, doc, proposal)
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
            "SELECT id, document_id, principal, agent, note, status, created_at
             FROM read.proposals
             WHERE status = 'open' AND ($1::uuid IS NULL OR document_id = $1)
             ORDER BY created_at",
        )
        .bind(doc)
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn review(&self, proposal: Uuid) -> Result<Review> {
        let info: Proposal = sqlx::query_as(
            "SELECT id, document_id, principal, agent, note, status, created_at
             FROM read.proposals WHERE id = $1",
        )
        .bind(proposal)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| Error::NotFound(format!("proposal {proposal}")))?;

        #[derive(sqlx::FromRow)]
        struct Row {
            #[sqlx(try_from = "String")]
            op: Op,
            block_id: Uuid,
            base_version: Option<i32>,
            new_body: Option<String>,
            current_version: Option<i32>,
            old_body: Option<String>,
            fresh: bool,
        }
        let rows: Vec<Row> = sqlx::query_as(
            "SELECT c.op, c.block_id, c.base_version, c.body AS new_body,
                    b.version AS current_version, old.payload->>'body' AS old_body,
                    core.change_is_fresh(c, $2) AS fresh
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
        .bind(info.document_id)
        .fetch_all(&self.pool)
        .await?;

        let mut diff = String::new();
        let changes = rows
            .into_iter()
            .map(|r| {
                let stale = !r.fresh;
                let old = r.old_body.as_deref().unwrap_or("");
                let new = r.new_body.as_deref().unwrap_or("");
                let header = format!(
                    "{} {}{}",
                    r.op.as_str(),
                    r.block_id,
                    if stale { " (stale)" } else { "" }
                );
                diff += &similar::TextDiff::from_lines(format!("{old}\n"), format!("{new}\n"))
                    .unified_diff()
                    .header(&header, &header)
                    .to_string();
                ReviewedChange {
                    op: r.op,
                    block_id: r.block_id,
                    base_version: r.base_version,
                    current_version: r.current_version,
                    old_body: r.old_body,
                    new_body: r.new_body,
                    stale,
                }
            })
            .collect();

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
        let (doc, tier) = lock_open_proposal(&mut tx, proposal).await?;
        if tier == Tier::Raw {
            return Err(Error::Immutable);
        }
        if !rights(&mut tx, &principal, actor, tier).await?.commit {
            return Err(forbidden(actor, "commit", tier));
        }

        // One commit per document at a time. Locking the blocks a proposal
        // touches would cover updates and deletes, but an insert's gap is
        // defined by blocks that don't exist yet, and two commits could each
        // find the same gap empty. Waiting on the document row closes that,
        // and under READ COMMITTED the check below then sees whatever the
        // commit ahead of us wrote.
        sqlx::query("SELECT FROM core.documents WHERE id = $1 FOR UPDATE")
            .bind(doc)
            .execute(&mut *tx)
            .await?;

        #[derive(sqlx::FromRow)]
        struct Row {
            #[sqlx(try_from = "String")]
            op: Op,
            block_id: Uuid,
            kind: Option<String>,
            body: Option<String>,
            position: Option<f64>,
            fresh: bool,
        }
        let changes: Vec<Row> = sqlx::query_as(
            "SELECT c.op, c.block_id, c.kind, c.body, c.position,
                    core.change_is_fresh(c, $2) AS fresh
             FROM core.proposal_changes c WHERE c.proposal_id = $1 ORDER BY c.seq",
        )
        .bind(proposal)
        .bind(doc)
        .fetch_all(&mut *tx)
        .await?;
        let stale: Vec<Uuid> = changes
            .iter()
            .filter(|c| !c.fresh)
            .map(|c| c.block_id)
            .collect();
        if !stale.is_empty() {
            return Err(Error::Conflict { blocks: stale });
        }

        let event = EventCtx::proposal(&principal, actor, doc, proposal);
        for c in &changes {
            match c.op {
                Op::Update => {
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
                Op::Delete => {
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
                Op::Insert => {
                    let (Some(position), Some(kind), Some(body)) = (c.position, &c.kind, &c.body)
                    else {
                        unreachable!("an insert always records position, kind and body");
                    };
                    insert_block(&mut tx, &event, doc, c.block_id, position, kind, body).await?;
                }
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
        let (doc, tier) = lock_open_proposal(&mut tx, proposal).await?;
        if !rights(&mut tx, &principal, actor, tier).await?.commit {
            return Err(forbidden(actor, "reject", tier));
        }
        decide(&mut tx, proposal, &principal, "rejected").await?;
        EventCtx::proposal(&principal, actor, doc, proposal)
            .log(&mut tx, "proposal_rejected", None, json!({}))
            .await?;
        tx.commit().await?;
        Ok(())
    }

    // History -------------------------------------------------------------

    /// Every recorded change to a block or a relation.
    pub async fn history(&self, id: Uuid) -> Result<Vec<HistoryEntry>> {
        Ok(sqlx::query_as(
            "SELECT e.seq, e.at, e.kind, p.name AS principal, e.agent, e.proposal_id,
                    pp.name AS proposed_by, pr.agent AS proposed_via,
                    (e.payload->>'version')::int AS version, e.payload->>'body' AS body,
                    CASE WHEN e.relation_id IS NOT NULL THEN e.payload END AS detail
             FROM core.events e
             JOIN core.principals p ON p.id = e.principal_id
             LEFT JOIN core.proposals pr ON pr.id = e.proposal_id
             LEFT JOIN core.principals pp ON pp.id = pr.principal_id
             WHERE e.block_id = $1 OR e.relation_id = $1
             ORDER BY e.seq",
        )
        .bind(id)
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
        let mut tx = self.reader.begin_with("BEGIN READ ONLY").await?;
        // `true` makes both transaction-local, like SET LOCAL.
        sqlx::query(
            "SELECT set_config('statement_timeout', '5s', true),
                    set_config('search_path', 'read', true)",
        )
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

impl<'a> EventCtx<'a> {
    fn document(principal: &'a Principal, actor: &'a Actor, doc: Uuid) -> Self {
        Self {
            principal,
            actor,
            document: Some(doc),
            relation: None,
            proposal: None,
        }
    }

    fn proposal(principal: &'a Principal, actor: &'a Actor, doc: Uuid, proposal: Uuid) -> Self {
        Self {
            proposal: Some(proposal),
            ..Self::document(principal, actor, doc)
        }
    }

    /// Relation events carry no document: a link can span two.
    fn relation(principal: &'a Principal, actor: &'a Actor, relation: Uuid) -> Self {
        Self {
            principal,
            actor,
            document: None,
            relation: Some(relation),
            proposal: None,
        }
    }

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

/// Lock a proposal for a decision and return its document and tier. Only an
/// open proposal can be decided.
async fn lock_open_proposal(conn: &mut PgConnection, proposal: Uuid) -> Result<(Uuid, Tier)> {
    let (doc, status, tier): (Uuid, String, String) = sqlx::query_as(
        "SELECT p.document_id, p.status, d.tier::text
         FROM core.proposals p JOIN core.documents d ON d.id = p.document_id
         WHERE p.id = $1
         FOR UPDATE OF p",
    )
    .bind(proposal)
    .fetch_optional(conn)
    .await?
    .ok_or_else(|| Error::NotFound(format!("proposal {proposal}")))?;
    if status != "open" {
        return Err(Error::Invalid(format!(
            "proposal {proposal} is already {status}"
        )));
    }
    Ok((doc, tier.parse()?))
}

/// Add a block and log it. The `block_set` payload is what `history` and
/// `review` read bodies and versions back from.
async fn insert_block(
    conn: &mut PgConnection,
    event: &EventCtx<'_>,
    doc: Uuid,
    id: Uuid,
    position: f64,
    kind: &str,
    body: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO core.blocks (id, document_id, position, kind, body)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(id)
    .bind(doc)
    .bind(position)
    .bind(kind)
    .bind(body)
    .execute(&mut *conn)
    .await?;
    event
        .log(
            conn,
            "block_set",
            Some(id),
            json!({ "kind": kind, "body": body, "position": position, "version": 1 }),
        )
        .await
}

/// Midpoint between the anchor and the next block, or one step past the end.
fn insert_position(positions: &HashMap<Uuid, f64>, after: Option<Uuid>) -> Result<f64> {
    let Some(id) = after else {
        let first = positions.values().copied().min_by(f64::total_cmp);
        return Ok(first.map_or(1.0, |first| first - 1.0));
    };
    let anchor = *positions
        .get(&id)
        .ok_or_else(|| Error::NotFound(format!("anchor block {id}")))?;
    let next = positions
        .values()
        .copied()
        .filter(|p| *p > anchor)
        .min_by(f64::total_cmp);
    Ok(next.map_or(anchor + 1.0, |next| (anchor + next) / 2.0))
}

/// The existing blocks right before and after `position`, `None` at either
/// end. `core.gap_is_open` later checks the gap is still as found here.
fn gap_around(existing: &HashMap<Uuid, f64>, position: f64) -> (Option<Uuid>, Option<Uuid>) {
    let before = existing
        .iter()
        .filter(|&(_, p)| *p < position)
        .max_by(|a, b| a.1.total_cmp(b.1));
    let after = existing
        .iter()
        .filter(|&(_, p)| *p > position)
        .min_by(|a, b| a.1.total_cmp(b.1));
    (before.map(|(id, _)| *id), after.map(|(id, _)| *id))
}
