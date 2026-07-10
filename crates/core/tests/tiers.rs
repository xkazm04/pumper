//! Integration test for the self-learning tier router memory against a real
//! temp-dir SQLite with the full migration chain.

use pumper_core::config::StorageConfig;
use pumper_core::{Storage, TierMemory};

#[tokio::test]
async fn learns_browser_after_three_strikes_and_resets_on_http_win() {
    let dir = std::env::temp_dir().join(format!("pumper-tier-test-{}", uuid::Uuid::new_v4()));
    let cfg = StorageConfig {
        database_path: dir.join("pumper.db"),
        artifacts_dir: dir.join("artifacts"),
    };
    let storage = Storage::connect(&cfg).await.expect("connect + migrate");
    let tiers = TierMemory::new(storage.pool());

    assert_eq!(tiers.preferred("spa.example").await.unwrap(), None);

    // Two HTTP losses: strikes accumulate but no preference yet.
    tiers.record("SPA.example", "browser", true).await.unwrap();
    tiers.record("spa.example", "browser", true).await.unwrap();
    assert_eq!(tiers.preferred("spa.example").await.unwrap(), None);

    // Third consecutive loss flips the host to the browser tier.
    tiers.record("spa.example", "claude", true).await.unwrap();
    assert_eq!(
        tiers.preferred("Spa.Example").await.unwrap().as_deref(),
        Some("browser"),
        "case-insensitive host"
    );

    // A browser win with no HTTP attempt teaches nothing (skip path).
    tiers.record("spa.example", "browser", false).await.unwrap();
    assert_eq!(tiers.preferred("spa.example").await.unwrap().as_deref(), Some("browser"));

    // One HTTP win fully resets the record.
    tiers.record("spa.example", "http", false).await.unwrap();
    assert_eq!(tiers.preferred("spa.example").await.unwrap(), None);

    // Unrelated hosts are untouched.
    assert_eq!(tiers.preferred("plain.example").await.unwrap(), None);

    drop(storage);
    std::fs::remove_dir_all(&dir).ok();
}
