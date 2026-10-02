//! The archivar command line. Data comes out as JSON, so in nushell
//! `archivar blame $doc | from json` is a table. `get` prints markdown and
//! `review` prints a diff unless `--json` is given.

use std::io::Read;
use std::path::PathBuf;

use anyhow::{Context, bail};
use archivar_store::{Actor, Change, Direction, NewRelation, Store, Tier};
use clap::{Parser, Subcommand};
use serde::Serialize;
use uuid::Uuid;

#[derive(Parser)]
#[command(name = "archivar", version, about = "Agent-mediated knowledge store")]
struct Cli {
    #[arg(long, env = "DATABASE_URL", hide_env_values = true, global = true)]
    database_url: Option<String>,
    /// The principal acting.
    #[arg(long = "as", env = "ARCHIVAR_PRINCIPAL", global = true)]
    principal: Option<String>,
    /// The agent acting on the principal's behalf, if any.
    #[arg(long, env = "ARCHIVAR_AGENT", global = true)]
    agent: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create or upgrade the schema.
    Init,
    /// Manage principals.
    #[command(subcommand)]
    Principal(PrincipalCmd),
    /// Ingest a markdown file as a new document.
    Ingest {
        file: PathBuf,
        #[arg(long)]
        tier: Tier,
        /// Defaults to the file name.
        #[arg(long)]
        title: Option<String>,
    },
    /// Print a document as markdown.
    Get { doc: Uuid },
    /// List a document's blocks with their IDs and versions.
    Blocks { doc: Uuid },
    /// Propose a change.
    #[command(subcommand)]
    Propose(ProposeCmd),
    /// List open proposals.
    Proposals {
        #[arg(long)]
        doc: Option<Uuid>,
    },
    /// Show what a proposal would change.
    Review {
        proposal: Uuid,
        #[arg(long)]
        json: bool,
    },
    /// Apply a proposal.
    Commit { proposal: Uuid },
    /// Close a proposal without applying it.
    Reject { proposal: Uuid },
    /// Every recorded change to a block.
    History { block: Uuid },
    /// Who last changed each block of a document.
    Blame { doc: Uuid },
    /// Relate two documents or blocks. Needs the commit right in the tier,
    /// so agents link into `derived`.
    Link {
        from: Uuid,
        to: Uuid,
        /// What the link means, like `cites` or `supersedes`.
        #[arg(long)]
        kind: String,
        #[arg(long, default_value = "derived")]
        tier: Tier,
        /// How sure the asserter is, from 0 to 1.
        #[arg(long)]
        confidence: Option<f64>,
        #[arg(long)]
        note: Option<String>,
    },
    /// Retract a relation.
    Unlink { relation: Uuid },
    /// Make a relation canonical.
    Promote { relation: Uuid },
    /// Relations around a document or block, walking up to DEPTH steps.
    Related {
        node: Uuid,
        #[arg(long, default_value_t = 1)]
        depth: u32,
        /// out, in or both.
        #[arg(long, default_value = "both")]
        direction: Direction,
        /// Follow only relations in these tiers. Repeatable.
        #[arg(long)]
        tier: Vec<Tier>,
    },
    /// Run a read-only SELECT against the published read schema.
    Query { sql: String },
}

#[derive(Subcommand)]
enum PrincipalCmd {
    Add {
        name: String,
        /// admin, editor, agent or reader.
        #[arg(long)]
        role: String,
    },
}

#[derive(clap::Args)]
struct ProposeOpts {
    #[arg(long)]
    note: Option<String>,
    /// Commit right away, when the actor may.
    #[arg(long)]
    commit: bool,
}

#[derive(Subcommand)]
enum ProposeCmd {
    /// Replace a block's text. TEXT `-` reads stdin.
    Update {
        block: Uuid,
        text: String,
        #[command(flatten)]
        opts: ProposeOpts,
    },
    /// Insert a block after another, or at the start.
    Insert {
        doc: Uuid,
        text: String,
        #[arg(long, conflicts_with = "start", required_unless_present = "start")]
        after: Option<Uuid>,
        #[arg(long)]
        start: bool,
        #[command(flatten)]
        opts: ProposeOpts,
    },
    /// Delete a block.
    Delete {
        block: Uuid,
        #[command(flatten)]
        opts: ProposeOpts,
    },
    /// Several changes at once, as a JSON array on stdin:
    /// `[{"op": "update", "block": "<id>", "body": "..."}]`.
    Batch {
        doc: Uuid,
        #[command(flatten)]
        opts: ProposeOpts,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let url = cli
        .database_url
        .as_deref()
        .context("no database: set DATABASE_URL or pass --database-url")?;
    let store = Store::connect(url).await?;
    let actor = || -> anyhow::Result<Actor> {
        let principal = cli
            .principal
            .clone()
            .context("who is acting? set ARCHIVAR_PRINCIPAL or pass --as")?;
        Ok(Actor {
            principal,
            agent: cli.agent.clone(),
        })
    };

    match cli.command {
        Command::Init => store.migrate().await?,
        Command::Principal(PrincipalCmd::Add { name, role }) => {
            print_json(&store.add_principal(&name, &role).await?)?
        }
        Command::Ingest { file, tier, title } => {
            let source = std::fs::read_to_string(&file)
                .with_context(|| format!("reading {}", file.display()))?;
            let title = match title {
                Some(t) => t,
                None => file
                    .file_stem()
                    .context("file has no name to use as title")?
                    .to_string_lossy()
                    .into_owned(),
            };
            print_json(&store.ingest(&actor()?, &title, tier, &source).await?)?
        }
        Command::Get { doc } => print!("{}", store.materialize(doc).await?),
        Command::Blocks { doc } => print_json(&store.blocks(doc).await?)?,
        Command::Propose(cmd) => {
            let actor = actor()?;
            let (doc, changes, opts) = match cmd {
                ProposeCmd::Update { block, text, opts } => (
                    store.block_document(block).await?,
                    vec![Change::Update {
                        block,
                        body: text_arg(text)?,
                    }],
                    opts,
                ),
                ProposeCmd::Insert {
                    doc,
                    text,
                    after,
                    start: _,
                    opts,
                } => (
                    doc,
                    vec![Change::Insert {
                        after,
                        body: text_arg(text)?,
                    }],
                    opts,
                ),
                ProposeCmd::Delete { block, opts } => (
                    store.block_document(block).await?,
                    vec![Change::Delete { block }],
                    opts,
                ),
                ProposeCmd::Batch { doc, opts } => {
                    let changes: Vec<Change> = serde_json::from_str(&text_arg("-".into())?)
                        .context("stdin is not a JSON array of changes")?;
                    (doc, changes, opts)
                }
            };
            let proposal = store
                .propose(&actor, doc, opts.note.as_deref(), &changes)
                .await?;
            if opts.commit {
                store.commit(&actor, proposal).await?;
            }
            print_json(&proposal)?
        }
        Command::Proposals { doc } => print_json(&store.proposals(doc).await?)?,
        Command::Review { proposal, json } => {
            let review = store.review(proposal).await?;
            if json {
                print_json(&review)?
            } else {
                let p = &review.proposal;
                let by = match &p.agent {
                    Some(agent) => format!("{} via {agent}", p.principal),
                    None => p.principal.clone(),
                };
                println!("proposal {} by {by}, {}", p.id, p.status);
                if let Some(note) = &p.note {
                    println!("  {note}");
                }
                println!();
                print!("{}", review.diff);
            }
        }
        Command::Commit { proposal } => store.commit(&actor()?, proposal).await?,
        Command::Reject { proposal } => store.reject(&actor()?, proposal).await?,
        Command::History { block } => print_json(&store.history(block).await?)?,
        Command::Blame { doc } => print_json(&store.blame(doc).await?)?,
        Command::Link {
            from,
            to,
            kind,
            tier,
            confidence,
            note,
        } => {
            let rel = NewRelation {
                from,
                to,
                kind,
                tier,
                confidence,
                note,
            };
            print_json(&store.link(&actor()?, &rel).await?)?
        }
        Command::Unlink { relation } => store.unlink(&actor()?, relation).await?,
        Command::Promote { relation } => store.promote(&actor()?, relation).await?,
        Command::Related {
            node,
            depth,
            direction,
            tier,
        } => print_json(&store.related(node, depth, direction, &tier).await?)?,
        Command::Query { sql } => print_json(&store.query(&sql).await?)?,
    }
    Ok(())
}

/// `-` means stdin, anything else is the text itself.
fn text_arg(text: String) -> anyhow::Result<String> {
    if text != "-" {
        return Ok(text);
    }
    let mut buf = String::new();
    std::io::stdin().read_to_string(&mut buf)?;
    if buf.trim().is_empty() {
        bail!("nothing on stdin");
    }
    Ok(buf)
}

fn print_json(value: &impl Serialize) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
