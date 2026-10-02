//! The trust guarantees of slice 1, each against a fresh database.
//!
//! Needs `DATABASE_URL` pointing at a Postgres where the user may create
//! databases; `pg-up` in the devshell provides one.

use archivar_store::{Actor, Change, Error, Store, Tier};
use sqlx::PgPool;
use uuid::Uuid;

const DOC: &str = "# Cell\n\nRated for 200 bar.\n\nTorque 12 Nm.\n";

async fn setup(pool: PgPool) -> Store {
    let store = Store::from_pool(pool);
    store.add_principal("alice", "editor").await.unwrap();
    store.add_principal("bob", "editor").await.unwrap();
    store.add_principal("bot", "agent").await.unwrap();
    store
}

async fn block_ids(store: &Store, doc: Uuid) -> Vec<Uuid> {
    store
        .blocks(doc)
        .await
        .unwrap()
        .into_iter()
        .map(|b| b.id)
        .collect()
}

fn update(block: Uuid, body: &str) -> Vec<Change> {
    vec![Change::Update {
        block,
        body: body.into(),
    }]
}

#[sqlx::test]
async fn ingest_then_materialize_round_trips(pool: PgPool) {
    let store = setup(pool).await;
    let doc = store
        .ingest(&Actor::human("alice"), "Cell", Tier::Canonical, DOC)
        .await
        .unwrap();
    assert_eq!(store.materialize(doc).await.unwrap(), DOC);
}

#[sqlx::test]
async fn agents_cannot_commit_to_canonical(pool: PgPool) {
    let store = setup(pool).await;
    let alice = Actor::human("alice");
    let doc = store
        .ingest(&alice, "Cell", Tier::Canonical, DOC)
        .await
        .unwrap();
    let para = block_ids(&store, doc).await[1];
    let changes = update(para, "Rated for 250 bar.");

    // An agent principal may propose, but not commit.
    let bot = Actor::human("bot");
    let p = store.propose(&bot, doc, None, &changes).await.unwrap();
    assert!(matches!(
        store.commit(&bot, p).await,
        Err(Error::Forbidden { .. })
    ));

    // Neither may an editor while an agent acts for them.
    let alice_via_claude = Actor::via("alice", "claude");
    assert!(matches!(
        store.commit(&alice_via_claude, p).await,
        Err(Error::Forbidden { .. })
    ));

    // The human, acting directly, may.
    store.commit(&alice, p).await.unwrap();
    assert_eq!(
        store.blocks(doc).await.unwrap()[1].body,
        "Rated for 250 bar."
    );
}

#[sqlx::test]
async fn agents_commit_directly_to_working(pool: PgPool) {
    let store = setup(pool).await;
    let bot = Actor::human("bot");
    let doc = store
        .ingest(&bot, "Scratch", Tier::Working, DOC)
        .await
        .unwrap();
    let para = block_ids(&store, doc).await[1];
    let p = store
        .propose(&bot, doc, None, &update(para, "Maybe 250 bar?"))
        .await
        .unwrap();
    store.commit(&bot, p).await.unwrap();
}

#[sqlx::test]
async fn raw_documents_never_change(pool: PgPool) {
    let store = setup(pool).await;
    let alice = Actor::human("alice");
    let doc = store.ingest(&alice, "Mail", Tier::Raw, DOC).await.unwrap();
    let para = block_ids(&store, doc).await[1];
    assert!(matches!(
        store
            .propose(&alice, doc, None, &update(para, "edited"))
            .await,
        Err(Error::Immutable)
    ));
}

#[sqlx::test]
async fn blame_names_author_agent_and_approver(pool: PgPool) {
    let store = setup(pool).await;
    let doc = store
        .ingest(&Actor::human("alice"), "Cell", Tier::Canonical, DOC)
        .await
        .unwrap();
    let para = block_ids(&store, doc).await[1];

    let p = store
        .propose(
            &Actor::via("alice", "claude"),
            doc,
            Some("bump rating"),
            &update(para, "Rated for 250 bar."),
        )
        .await
        .unwrap();
    store.commit(&Actor::human("bob"), p).await.unwrap();

    let blame = store.blame(doc).await.unwrap();
    let line = blame.iter().find(|l| l.block_id == para).unwrap();
    assert_eq!(line.version, 2);
    assert_eq!(line.committed_by, "bob");
    assert_eq!(line.committed_via, None);
    assert_eq!(line.proposed_by.as_deref(), Some("alice"));
    assert_eq!(line.proposed_via.as_deref(), Some("claude"));

    // Untouched blocks still blame the ingest.
    let heading = blame.iter().find(|l| l.block_id != para).unwrap();
    assert_eq!(heading.committed_by, "alice");
    assert_eq!(heading.proposed_by, None);

    let history = store.history(para).await.unwrap();
    let bodies: Vec<_> = history.iter().filter_map(|h| h.body.as_deref()).collect();
    assert_eq!(bodies, ["Rated for 200 bar.", "Rated for 250 bar."]);
}

#[sqlx::test]
async fn stale_proposals_conflict_per_block_and_apply_nothing(pool: PgPool) {
    let store = setup(pool).await;
    let alice = Actor::human("alice");
    let doc = store
        .ingest(&alice, "Cell", Tier::Canonical, DOC)
        .await
        .unwrap();
    let ids = block_ids(&store, doc).await;
    let (rating, torque) = (ids[1], ids[2]);

    let both = store
        .propose(
            &Actor::via("alice", "claude"),
            doc,
            None,
            &[
                Change::Update {
                    block: rating,
                    body: "Rated for 250 bar.".into(),
                },
                Change::Update {
                    block: torque,
                    body: "Torque 14 Nm.".into(),
                },
            ],
        )
        .await
        .unwrap();
    let other = store
        .propose(
            &Actor::human("bob"),
            doc,
            None,
            &update(torque, "Torque 13 Nm."),
        )
        .await
        .unwrap();
    store.commit(&Actor::human("bob"), other).await.unwrap();

    let review = store.review(both).await.unwrap();
    let stale: Vec<_> = review
        .changes
        .iter()
        .filter(|c| c.stale)
        .map(|c| c.block_id)
        .collect();
    assert_eq!(stale, [torque]);

    match store.commit(&alice, both).await {
        Err(Error::Conflict { blocks }) => assert_eq!(blocks, [torque]),
        other => panic!("expected a conflict, got {other:?}"),
    }
    // The clean half did not land either.
    let blocks = store.blocks(doc).await.unwrap();
    assert_eq!(blocks[1].body, "Rated for 200 bar.");
    assert_eq!(blocks[1].version, 1);
    assert_eq!(blocks[2].body, "Torque 13 Nm.");
}

#[sqlx::test]
async fn inserts_and_deletes_keep_order(pool: PgPool) {
    let store = setup(pool).await;
    let alice = Actor::human("alice");
    let doc = store
        .ingest(&alice, "Cell", Tier::Canonical, DOC)
        .await
        .unwrap();
    let ids = block_ids(&store, doc).await;
    let p = store
        .propose(
            &alice,
            doc,
            None,
            &[
                Change::Insert {
                    after: Some(ids[0]),
                    body: "Bought 2024.".into(),
                },
                Change::Delete { block: ids[2] },
                Change::Insert {
                    after: None,
                    body: "---".into(),
                },
            ],
        )
        .await
        .unwrap();
    store.commit(&alice, p).await.unwrap();
    assert_eq!(
        store.materialize(doc).await.unwrap(),
        "---\n\n# Cell\n\nBought 2024.\n\nRated for 200 bar.\n"
    );
}

#[sqlx::test]
async fn query_reads_the_published_schema(pool: PgPool) {
    let store = setup(pool).await;
    store
        .ingest(&Actor::human("alice"), "Cell", Tier::Canonical, DOC)
        .await
        .unwrap();
    let rows = store
        .query("SELECT kind, count(*) AS n FROM blocks GROUP BY kind ORDER BY kind;")
        .await
        .unwrap();
    assert_eq!(
        rows,
        serde_json::json!([{ "kind": "heading", "n": 1 }, { "kind": "paragraph", "n": 2 }])
    );
}

#[sqlx::test]
async fn query_cannot_write_or_escape(pool: PgPool) {
    let store = setup(pool).await;
    let attempts = [
        // Not a query at all.
        "INSERT INTO read.principals VALUES (gen_random_uuid(), 'eve', 'admin')",
        "RESET ROLE",
        // Statement stacking.
        "SELECT 1; DELETE FROM core.events",
        // Base tables are out of reach.
        "SELECT * FROM core.events",
        "SELECT * FROM core.acl",
        // The reader is not a member of the superuser role, so it cannot
        // switch to it from inside a query either.
        "SELECT set_config('role', (SELECT rolname FROM pg_roles WHERE rolsuper LIMIT 1), false)",
    ];
    for sql in attempts {
        assert!(store.query(sql).await.is_err(), "should fail: {sql}");
    }
}

#[sqlx::test]
async fn query_leaves_no_session_state_behind(pool: PgPool) {
    let store = setup(pool).await;
    // Leave a mark on whichever pooled connection runs this. Nothing in
    // `query` resets application_name, so only the rollback can undo it.
    for _ in 0..8 {
        store
            .query("SELECT set_config('application_name', 'tampered', false) AS q")
            .await
            .unwrap();
    }
    for _ in 0..8 {
        // Columns named like the wrapper's alias must not confuse it.
        let rows = store
            .query(
                "SELECT current_setting('application_name') = 'tampered' AS q, current_user AS t",
            )
            .await
            .unwrap();
        assert_eq!(
            rows,
            serde_json::json!([{ "q": false, "t": "archivar_reader" }])
        );
    }
}

#[sqlx::test]
async fn the_event_log_is_append_only(pool: PgPool) {
    let store = setup(pool.clone()).await;
    store
        .ingest(&Actor::human("alice"), "Cell", Tier::Canonical, DOC)
        .await
        .unwrap();
    // Even the owner of the table, connected with full rights, cannot rewrite it.
    assert!(
        sqlx::query("UPDATE core.events SET agent = 'nobody'")
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM core.events")
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("TRUNCATE core.events")
            .execute(&pool)
            .await
            .is_err()
    );
    let events: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(events, 4);
}
