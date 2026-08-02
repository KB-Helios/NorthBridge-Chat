use std::{env, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use northbridge_domain::ManagedModelStatus;
use northbridge_model_manager::{DownloadModelRequest, ModelManager, ModelManagerConfig};
use northbridge_storage::Database;
use tokio::time::{Instant, sleep};

#[tokio::test]
#[ignore = "requires Hugging Face network access"]
async fn downloads_validates_resolves_and_removes_a_real_snapshot() -> Result<()> {
    ensure!(
        env::var("NORTHBRIDGE_RUN_REAL_MODEL_DOWNLOAD").as_deref() == Ok("1"),
        "run this test through `npm run desktop:test:model-manager`"
    );
    let directory = tempfile::tempdir()?;
    let database = Database::connect(&directory.path().join("models.sqlite")).await?;
    let manager = ModelManager::open(
        database,
        ModelManagerConfig::local(directory.path().join("models")),
    )
    .await?;
    let queued = manager
        .start_download(DownloadModelRequest {
            model_id: "hf-internal-testing/tiny-random-gpt2".to_string(),
            revision: Some("main".to_string()),
            display_name: Some("Tiny GPT-2 acceptance model".to_string()),
        })
        .await?;
    assert_eq!(queued.status, ManagedModelStatus::Queued);

    let deadline = Instant::now() + Duration::from_secs(5 * 60);
    let installed = loop {
        let model = manager.get(queued.id).await?;
        match model.status {
            ManagedModelStatus::Installed => break model,
            ManagedModelStatus::Failed | ManagedModelStatus::Cancelled => {
                bail!(
                    "real model download ended in state {}: {:?}",
                    model.status.as_str(),
                    model.last_error
                );
            }
            ManagedModelStatus::Queued | ManagedModelStatus::Downloading => {}
            ManagedModelStatus::Imported => bail!("download unexpectedly became an import"),
        }
        ensure!(
            Instant::now() < deadline,
            "real model download did not finish within five minutes"
        );
        sleep(Duration::from_millis(100)).await;
    };
    ensure!(installed.bytes_downloaded > 0);
    ensure!(installed.total_bytes.is_some_and(|total| total > 0));
    ensure!(installed.file_count.is_some_and(|count| count > 0));
    ensure!(
        installed
            .manifest_sha256
            .as_deref()
            .is_some_and(|sha256| sha256.len() == 64)
    );
    let runtime_path = manager
        .runtime_reference(&installed.model_id)
        .await?
        .context("installed model did not resolve to a local runtime path")?;
    ensure!(runtime_path.join("config.json").is_file());
    ensure!(runtime_path.join(".northbridge-model.json").is_file());

    manager.remove(installed.id).await?;
    ensure!(!runtime_path.exists());
    ensure!(manager.list().await?.is_empty());
    Ok(())
}
