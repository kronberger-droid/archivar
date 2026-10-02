//! The guarantees of relations, each against a fresh database.
//!
//! Needs `DATABASE_URL` like `trust.rs`; `pg-up` in the devshell provides it.

use archivar_store::{Actor, Change, Direction, Error, NewRelation, Related, Store, Tier};
use sqlx::PgPool;
use uuid::Uuid;

async fn setup(pool: PgPool) -> Store {
    let store = Store::from_pool(pool);
    store.add_principal("alice", "editor").await.unwrap();
    store.add_principal("bot", "agent").await.unwrap();
    store
}

async fn doc(store: &Store, title: &str) -> Uuid {
    store
        .ingest(
            &Actor::human("alice"),
            title,
            Tier::Canonical,
            &format!("# {title}\n\nSome text.\n"),
        )
        .await
        .unwrap()
}

fn rel(from: Uuid, to: Uuid, kind: &str, tier: Tier) -> NewRelation {
    NewRelation {
        from,
        to,
        kind: kind.into(),
        tier,
        confidence: None,
        note: None,
    }
}

fn ids(related: &[Related]) -> Vec<Uuid> {
    related.iter().map(|r| r.relation_id).collect()
}

#[sqlx::test]
async fn agent_links_land_in_derived_with_provenance(pool: PgPool) {
    let store = setup(pool).await;
    let (cell, gasket) = (doc(&store, "Cell").await, doc(&store, "Gasket").await);
    let block = store.blocks(gasket).await.unwrap()[1].id;

    let id = store
        .link(
            &Actor::via("alice", "claude"),
            &NewRelation {
                confidence: Some(0.7),
                note: Some("same part number".into()),
                ..rel(cell, block, "uses", Tier::Derived)
            },
        )
        .await
        .unwrap();

    let related = store.related(cell, 1, Direction::Both, &[]).await.unwrap();
    assert_eq!(ids(&related), [id]);
    let r = &related[0];
    assert_eq!((r.node, r.node_kind.as_str()), (block, "block"));
    assert_eq!(r.node_label, "Some text.");
    assert_eq!(r.tier, "derived");
    assert_eq!(r.asserted_by, "alice");
    assert_eq!(r.agent.as_deref(), Some("claude"));
    assert_eq!(r.confidence, Some(0.7));
    assert_eq!(r.promoted_by, None);
}

#[sqlx::test]
async fn only_a_human_hand_makes_a_link_canonical(pool: PgPool) {
    let store = setup(pool).await;
    let (a, b) = (doc(&store, "A").await, doc(&store, "B").await);
    let alice = Actor::human("alice");
    let alice_via_claude = Actor::via("alice", "claude");

    // Neither an agent nor a human acting through one can link canonical
    // directly, or lift a guess there.
    for actor in [&Actor::human("bot"), &alice_via_claude] {
        assert!(matches!(
            store
                .link(actor, &rel(a, b, "cites", Tier::Canonical))
                .await,
            Err(Error::Forbidden { .. })
        ));
    }
    let guess = store
        .link(&alice_via_claude, &rel(a, b, "cites", Tier::Derived))
        .await
        .unwrap();
    for actor in [&Actor::human("bot"), &alice_via_claude] {
        assert!(matches!(
            store.promote(actor, guess).await,
            Err(Error::Forbidden { .. })
        ));
    }

    store.promote(&alice, guess).await.unwrap();
    let r = &store.related(a, 1, Direction::Out, &[]).await.unwrap()[0];
    assert_eq!(r.tier, "canonical");
    // The guess keeps its author, and the approval is named next to it.
    assert_eq!(r.agent.as_deref(), Some("claude"));
    assert_eq!(r.promoted_by.as_deref(), Some("alice"));

    let history = store.history(guess).await.unwrap();
    let steps: Vec<_> = history
        .iter()
        .map(|h| (h.kind.as_str(), h.principal.as_str(), h.agent.as_deref()))
        .collect();
    assert_eq!(
        steps,
        [
            ("relation_asserted", "alice", Some("claude")),
            ("relation_promoted", "alice", None)
        ]
    );
    assert_eq!(
        history[1].detail,
        Some(serde_json::json!({ "from": "derived", "to": "canonical" }))
    );

    // Once canonical, the agent can't take it back either.
    assert!(matches!(
        store.unlink(&alice_via_claude, guess).await,
        Err(Error::Forbidden { .. })
    ));
}

#[sqlx::test]
async fn a_live_link_is_asserted_once(pool: PgPool) {
    let store = setup(pool).await;
    let (a, b) = (doc(&store, "A").await, doc(&store, "B").await);
    let bot = Actor::human("bot");

    let first = store
        .link(&bot, &rel(a, b, "cites", Tier::Derived))
        .await
        .unwrap();
    // Not as a second guess, and not by skipping the promotion either.
    for tier in [Tier::Derived, Tier::Canonical] {
        let again = store
            .link(&Actor::human("alice"), &rel(a, b, "cites", tier))
            .await;
        assert!(
            matches!(&again, Err(Error::Invalid(msg)) if msg.contains(&first.to_string())),
            "{again:?}"
        );
    }
    // Another kind, or the other direction, is another relation.
    store
        .link(&bot, &rel(a, b, "supersedes", Tier::Derived))
        .await
        .unwrap();
    store
        .link(&bot, &rel(b, a, "cites", Tier::Derived))
        .await
        .unwrap();

    // Retracted, it can be asserted afresh, and the log keeps both.
    store.unlink(&bot, first).await.unwrap();
    let second = store
        .link(&bot, &rel(a, b, "cites", Tier::Derived))
        .await
        .unwrap();
    assert_ne!(first, second);
    let log = store
        .query("SELECT kind FROM events WHERE relation_id IS NOT NULL ORDER BY seq")
        .await
        .unwrap();
    let kinds: Vec<_> = log
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["kind"].as_str().unwrap())
        .collect();
    assert_eq!(
        kinds,
        [
            "relation_asserted",
            "relation_asserted",
            "relation_asserted",
            "relation_retracted",
            "relation_asserted"
        ]
    );
}

#[sqlx::test]
async fn links_need_live_ends_and_a_real_tier(pool: PgPool) {
    let store = setup(pool).await;
    let a = doc(&store, "A").await;
    let alice = Actor::human("alice");

    assert!(matches!(
        store
            .link(&alice, &rel(a, Uuid::now_v7(), "cites", Tier::Derived))
            .await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        store.link(&alice, &rel(a, a, "cites", Tier::Derived)).await,
        Err(Error::Invalid(_))
    ));
    let b = doc(&store, "B").await;
    assert!(matches!(
        store.link(&alice, &rel(a, b, "cites", Tier::Raw)).await,
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        store
            .link(
                &alice,
                &NewRelation {
                    confidence: Some(1.5),
                    ..rel(a, b, "cites", Tier::Derived)
                }
            )
            .await,
        Err(Error::Invalid(_))
    ));
}

/// a -> b -> c -> a, plus a canonical shortcut a -> c.
#[sqlx::test]
async fn related_walks_depth_and_direction_without_looping(pool: PgPool) {
    let store = setup(pool).await;
    let (a, b, c) = (
        doc(&store, "A").await,
        doc(&store, "B").await,
        doc(&store, "C").await,
    );
    let alice = Actor::human("alice");
    let ab = store
        .link(&alice, &rel(a, b, "next", Tier::Derived))
        .await
        .unwrap();
    let bc = store
        .link(&alice, &rel(b, c, "next", Tier::Derived))
        .await
        .unwrap();
    let ca = store
        .link(&alice, &rel(c, a, "next", Tier::Derived))
        .await
        .unwrap();
    let ac = store
        .link(&alice, &rel(a, c, "see", Tier::Canonical))
        .await
        .unwrap();

    let walk = |depth, direction, tiers: &'static [Tier]| {
        let store = &store;
        async move {
            let mut ids = ids(&store.related(a, depth, direction, tiers).await.unwrap());
            ids.sort();
            ids
        }
    };
    let sorted = |mut v: Vec<Uuid>| {
        v.sort();
        v
    };

    assert_eq!(walk(1, Direction::Out, &[]).await, sorted(vec![ab, ac]));
    assert_eq!(walk(1, Direction::In, &[]).await, [ca]);
    assert_eq!(
        walk(1, Direction::Both, &[]).await,
        sorted(vec![ab, ca, ac])
    );
    // Deep enough to go round the cycle several times; every relation still
    // comes back once.
    assert_eq!(
        walk(10, Direction::Out, &[]).await,
        sorted(vec![ab, bc, ca, ac])
    );
    // Following only canonical links never reaches b.
    assert_eq!(walk(10, Direction::Both, &[Tier::Canonical]).await, [ac]);

    // Each relation sits at its shallowest depth: b -> c is two steps out via
    // b, even though c itself is one step away.
    let related = store.related(a, 10, Direction::Out, &[]).await.unwrap();
    let depth = |id| related.iter().find(|r| r.relation_id == id).unwrap().depth;
    assert_eq!((depth(ab), depth(ac), depth(bc), depth(ca)), (1, 1, 2, 2));
}

#[sqlx::test]
async fn documents_carry_their_blocks_links(pool: PgPool) {
    let store = setup(pool).await;
    let (a, b) = (doc(&store, "A").await, doc(&store, "B").await);
    let alice = Actor::human("alice");
    let para = store.blocks(a).await.unwrap()[1].id;
    let cites = store
        .link(&alice, &rel(para, b, "cites", Tier::Derived))
        .await
        .unwrap();
    // A block pointing at its own document must not send the walk in circles.
    let part = store
        .link(&alice, &rel(para, a, "part-of", Tier::Derived))
        .await
        .unwrap();

    let from_a = store.related(a, 3, Direction::Both, &[]).await.unwrap();
    let mut got = ids(&from_a);
    got.sort();
    let mut want = vec![cites, part];
    want.sort();
    assert_eq!(got, want);
    let r = from_a.iter().find(|r| r.relation_id == cites).unwrap();
    assert_eq!((r.depth, r.node, r.through), (1, b, Some(para)));

    // From the other end the block is reached as itself, not lifted.
    let from_b = store.related(b, 1, Direction::In, &[]).await.unwrap();
    assert_eq!(ids(&from_b), [cites]);
    assert_eq!((from_b[0].node, from_b[0].through), (para, None));
}

#[sqlx::test]
async fn links_to_deleted_blocks_drop_out(pool: PgPool) {
    let store = setup(pool).await;
    let (a, b) = (doc(&store, "A").await, doc(&store, "B").await);
    let alice = Actor::human("alice");
    let block = store.blocks(b).await.unwrap()[1].id;
    store
        .link(&alice, &rel(a, block, "quotes", Tier::Derived))
        .await
        .unwrap();
    assert_eq!(
        store
            .related(a, 1, Direction::Both, &[])
            .await
            .unwrap()
            .len(),
        1
    );

    let p = store
        .propose(&alice, b, None, &[Change::Delete { block }])
        .await
        .unwrap();
    store.commit(&alice, p).await.unwrap();

    assert!(
        store
            .related(a, 1, Direction::Both, &[])
            .await
            .unwrap()
            .is_empty()
    );
    // Nothing new may point at it either.
    assert!(matches!(
        store
            .link(&alice, &rel(a, block, "cites", Tier::Derived))
            .await,
        Err(Error::NotFound(_))
    ));
}

#[sqlx::test]
async fn query_reads_relations(pool: PgPool) {
    let store = setup(pool).await;
    let (a, b) = (doc(&store, "A").await, doc(&store, "B").await);
    store
        .link(&Actor::human("bot"), &rel(a, b, "cites", Tier::Derived))
        .await
        .unwrap();
    let rows = store
        .query("SELECT from_kind, to_kind, kind, tier, asserted_by FROM relations")
        .await
        .unwrap();
    assert_eq!(
        rows,
        serde_json::json!([{
            "from_kind": "document", "to_kind": "document",
            "kind": "cites", "tier": "derived", "asserted_by": "bot"
        }])
    );
}
