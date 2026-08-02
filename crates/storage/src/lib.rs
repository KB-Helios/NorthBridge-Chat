mod agents;
mod collaboration;
mod conversations;
mod error;
mod generations;
mod knowledge;
mod models;
mod package_import;
mod platform;
mod rows;
mod search;
mod settings;
mod split_chats;
mod workflows;

use std::{path::Path, time::Duration};

pub use agents::{
    FinishCrewMember, NewAgentProfile, NewAgentRun, NewCrew, NewCrewRun, NewMcpServer, NewPersona,
    NewRemoteAgent, NewShadowSubscription, NewSkill, NewToolIntent, SaveAgentCheckpoint,
};
pub use collaboration::{NewSyncChange, NewTeamPolicy, NewTeamUser};
pub use error::{Result, StorageError};
pub use knowledge::{
    DenseJournalEntry, KnowledgeChunkRecord, KnowledgeLexicalHit, NewKnowledgeChunk,
    NewKnowledgeDocument, NewKnowledgeStack, NewKnowledgeWatchRoot, StoredEmbedding,
};
pub use package_import::PackageImportTransaction;
pub use platform::{NewArtifact, NewCredentialReference, NewEnvironment, NewMediaProvider};
use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
pub use workflows::{NewWorkflow, NewWorkflowRun};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

#[derive(Debug, Clone)]
pub struct Database {
    pool: SqlitePool,
}

impl Database {
    pub async fn connect(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            tokio::fs::create_dir_all(parent).await?;
        }

        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(8)
            .acquire_timeout(Duration::from_secs(10))
            .connect_with(options)
            .await?;

        MIGRATOR.run(&pool).await?;
        Ok(Self { pool })
    }

    #[must_use]
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub(crate) async fn begin_write(&self) -> Result<sqlx::Transaction<'static, sqlx::Sqlite>> {
        Ok(self.pool.begin_with("BEGIN IMMEDIATE").await?)
    }

    pub async fn ping(&self) -> Result<()> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    pub async fn schema_version(&self) -> Result<i64> {
        let version = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MAX(version) FROM _sqlx_migrations WHERE success = 1",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(version.unwrap_or_default())
    }
}

#[must_use]
pub fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests;
