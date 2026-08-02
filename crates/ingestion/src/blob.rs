use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use crate::{IngestionError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobRecord {
    pub sha256: String,
    pub path: PathBuf,
    pub byte_size: u64,
}

#[derive(Debug, Clone)]
pub struct ContentAddressedBlobStore {
    root: PathBuf,
}

impl ContentAddressedBlobStore {
    pub async fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        if !root.is_absolute() {
            return Err(IngestionError::InvalidInput(
                "blob store root must be absolute".to_string(),
            ));
        }
        tokio::fs::create_dir_all(&root).await?;
        Ok(Self { root })
    }

    pub async fn put(&self, bytes: &[u8]) -> Result<BlobRecord> {
        let sha256 = format!("{:x}", Sha256::digest(bytes));
        let directory = self.root.join(&sha256[..2]);
        let path = directory.join(&sha256);
        tokio::fs::create_dir_all(&directory).await?;
        if !path.is_file() {
            let temporary = directory.join(format!(".{sha256}-{}.tmp", Uuid::new_v4().simple()));
            let mut file = tokio::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)
                .await?;
            if let Err(error) = file.write_all(bytes).await {
                let _ = tokio::fs::remove_file(&temporary).await;
                return Err(error.into());
            }
            if let Err(error) = file.sync_all().await {
                let _ = tokio::fs::remove_file(&temporary).await;
                return Err(error.into());
            }
            drop(file);
            match tokio::fs::rename(&temporary, &path).await {
                Ok(()) => {}
                Err(error) if path.is_file() => {
                    let _ = tokio::fs::remove_file(&temporary).await;
                    tracing::debug!(%error, "content-addressed blob was published concurrently");
                }
                Err(error) => {
                    let _ = tokio::fs::remove_file(&temporary).await;
                    return Err(error.into());
                }
            }
        }
        Ok(BlobRecord {
            sha256,
            path,
            byte_size: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        })
    }

    #[must_use]
    pub fn path_for_hash(&self, sha256: &str) -> Option<PathBuf> {
        valid_sha256(sha256).then(|| self.root.join(&sha256[..2]).join(sha256))
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
