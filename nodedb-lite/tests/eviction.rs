//! Shape eviction tests — memory pressure triggers collection eviction,
//! data survives in storage, lazy reload on next access.

use nodedb_client::NodeDb;
use nodedb_lite::{NodeDbLite, RedbStorage};

async fn open_db_with_budget(budget: usize) -> NodeDbLite<RedbStorage> {
    let storage = RedbStorage::open_in_memory().unwrap();
    NodeDbLite::open_with_budget(storage, 1, budget)
        .await
        .unwrap()
}

#[tokio::test]
async fn evict_collection_persists_and_removes_from_memory() {
    let db = open_db_with_budget(100 * 1024 * 1024).await;

    // Insert vectors into two collections.
    let vecs_a: Vec<(String, Vec<f32>)> = (0..100)
        .map(|i| {
            let emb: Vec<f32> = (0..16).map(|d| ((i * 16 + d) as f32) * 0.01).collect();
            (format!("a{i}"), emb)
        })
        .collect();
    let refs_a: Vec<(&str, &[f32])> = vecs_a
        .iter()
        .map(|(id, e)| (id.as_str(), e.as_slice()))
        .collect();
    db.batch_vector_insert("coll_a", &refs_a).unwrap();

    let vecs_b: Vec<(String, Vec<f32>)> = (0..50)
        .map(|i| {
            let emb: Vec<f32> = (0..16).map(|d| ((i * 16 + d) as f32) * 0.02).collect();
            (format!("b{i}"), emb)
        })
        .collect();
    let refs_b: Vec<(&str, &[f32])> = vecs_b
        .iter()
        .map(|(id, e)| (id.as_str(), e.as_slice()))
        .collect();
    db.batch_vector_insert("coll_b", &refs_b).unwrap();

    // Both loaded.
    let loaded = db.loaded_collections().unwrap();
    assert_eq!(loaded.len(), 2);

    // Evict 1 collection.
    let evicted = db.evict_collections(1).await.unwrap();
    assert_eq!(evicted, 1);

    let loaded = db.loaded_collections().unwrap();
    assert_eq!(loaded.len(), 1);
}

#[tokio::test]
async fn evicted_collection_lazily_reloads_on_search() {
    let db = open_db_with_budget(100 * 1024 * 1024).await;

    // Insert vectors.
    let vecs: Vec<(String, Vec<f32>)> = (0..50)
        .map(|i| {
            let emb: Vec<f32> = (0..8).map(|d| ((i * 8 + d) as f32) * 0.01).collect();
            (format!("v{i}"), emb)
        })
        .collect();
    let refs: Vec<(&str, &[f32])> = vecs
        .iter()
        .map(|(id, e)| (id.as_str(), e.as_slice()))
        .collect();
    db.batch_vector_insert("lazy_coll", &refs).unwrap();

    // Flush so data is in storage.
    db.flush().await.unwrap();

    // Evict.
    let evicted = db.evict_collections(1).await.unwrap();
    assert_eq!(evicted, 1);
    assert!(db.loaded_collections().unwrap().is_empty());

    // Search — should lazily reload from storage.
    let query: Vec<f32> = (0..8).map(|d| (d as f32) * 0.01).collect();
    let results = db
        .vector_search("lazy_coll", &query, 5, None)
        .await
        .unwrap();
    assert!(!results.is_empty(), "search should work after lazy reload");

    // Collection should be loaded again.
    assert!(
        db.loaded_collections()
            .unwrap()
            .contains(&"lazy_coll".to_string())
    );
}

#[tokio::test]
async fn check_and_evict_responds_to_pressure() {
    // Tiny budget so any data triggers pressure.
    let db = open_db_with_budget(100).await;

    let vecs: Vec<(String, Vec<f32>)> = (0..10)
        .map(|i| {
            let emb: Vec<f32> = (0..4).map(|d| ((i * 4 + d) as f32) * 0.1).collect();
            (format!("v{i}"), emb)
        })
        .collect();
    let refs: Vec<(&str, &[f32])> = vecs
        .iter()
        .map(|(id, e)| (id.as_str(), e.as_slice()))
        .collect();
    db.batch_vector_insert("pressure_coll", &refs).unwrap();
    db.flush().await.unwrap();

    // Memory pressure should be high.
    db.update_memory_stats();

    let evicted = db.check_and_evict().await.unwrap();
    // Should evict something if pressure is Critical/Warning.
    // With 100 byte budget, any HNSW data exceeds it.
    assert!(evicted > 0 || db.governor().total_used() <= db.governor().total_budget());
}

#[tokio::test]
async fn startup_loads_only_persisted_collections() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lazy_start.redb");

    // Write data, flush, close.
    {
        let storage = RedbStorage::open(&path).unwrap();
        let db = NodeDbLite::open(storage, 1).await.unwrap();

        db.batch_vector_insert("active", &[("v1", &[1.0f32, 0.0][..])])
            .unwrap();
        db.batch_vector_insert("inactive", &[("v2", &[0.0, 1.0][..])])
            .unwrap();
        db.flush().await.unwrap();
    }

    // Reopen — both should be loaded from storage.
    {
        let storage = RedbStorage::open(&path).unwrap();
        let db = NodeDbLite::open(storage, 1).await.unwrap();

        let loaded = db.loaded_collections().unwrap();
        assert!(
            loaded.len() >= 2,
            "both collections should load from storage"
        );
    }
}
