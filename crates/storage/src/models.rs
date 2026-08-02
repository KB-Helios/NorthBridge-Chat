use std::str::FromStr;

use northbridge_domain::{ManagedModel, ManagedModelSource, ManagedModelStatus};
use sqlx::FromRow;
use uuid::Uuid;

use crate::{Database, Result, StorageError, now_ms};

#[derive(Debug, FromRow)]
struct ManagedModelRow {
    id: String,
    model_id: String,
    display_name: String,
    source: String,
    source_reference: String,
    revision: Option<String>,
    local_path: String,
    status: String,
    bytes_downloaded: i64,
    total_bytes: Option<i64>,
    file_count: Option<i64>,
    manifest_sha256: Option<String>,
    last_error: Option<String>,
    created_at_ms: i64,
    updated_at_ms: i64,
}

impl Database {
    pub async fn upsert_managed_model(&self, model: &ManagedModel) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO managed_models (
                id, model_id, display_name, source, source_reference, revision, local_path,
                status, bytes_downloaded, total_bytes, file_count, manifest_sha256,
                last_error, created_at_ms, updated_at_ms
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                model_id = excluded.model_id,
                display_name = excluded.display_name,
                source = excluded.source,
                source_reference = excluded.source_reference,
                revision = excluded.revision,
                local_path = excluded.local_path,
                status = excluded.status,
                bytes_downloaded = excluded.bytes_downloaded,
                total_bytes = excluded.total_bytes,
                file_count = excluded.file_count,
                manifest_sha256 = excluded.manifest_sha256,
                last_error = excluded.last_error,
                updated_at_ms = excluded.updated_at_ms
            "#,
        )
        .bind(model.id.to_string())
        .bind(&model.model_id)
        .bind(&model.display_name)
        .bind(model.source.as_str())
        .bind(&model.source_reference)
        .bind(&model.revision)
        .bind(&model.local_path)
        .bind(model.status.as_str())
        .bind(to_i64(model.bytes_downloaded, "model bytes downloaded")?)
        .bind(optional_i64(model.total_bytes, "model total bytes")?)
        .bind(optional_i64(model.file_count, "model file count")?)
        .bind(&model.manifest_sha256)
        .bind(&model.last_error)
        .bind(model.created_at_ms)
        .bind(model.updated_at_ms)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    pub async fn managed_models(&self) -> Result<Vec<ManagedModel>> {
        sqlx::query_as::<_, ManagedModelRow>(
            "SELECT * FROM managed_models ORDER BY updated_at_ms DESC, id DESC",
        )
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(ManagedModel::try_from)
        .collect()
    }

    pub async fn managed_model(&self, id: Uuid) -> Result<ManagedModel> {
        let row = sqlx::query_as::<_, ManagedModelRow>("SELECT * FROM managed_models WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(self.pool())
            .await?
            .ok_or(StorageError::NotFound {
                entity: "managed model",
            })?;
        ManagedModel::try_from(row)
    }

    pub async fn managed_model_by_model_id(&self, model_id: &str) -> Result<Option<ManagedModel>> {
        sqlx::query_as::<_, ManagedModelRow>("SELECT * FROM managed_models WHERE model_id = ?")
            .bind(model_id)
            .fetch_optional(self.pool())
            .await?
            .map(ManagedModel::try_from)
            .transpose()
    }

    pub async fn update_managed_model_progress(
        &self,
        id: Uuid,
        bytes_downloaded: u64,
        total_bytes: Option<u64>,
        file_count: Option<u64>,
    ) -> Result<()> {
        let result = sqlx::query(
            r#"
            UPDATE managed_models
            SET bytes_downloaded = ?, total_bytes = ?, file_count = ?, updated_at_ms = ?
            WHERE id = ? AND status IN ('queued', 'downloading')
            "#,
        )
        .bind(to_i64(bytes_downloaded, "model bytes downloaded")?)
        .bind(optional_i64(total_bytes, "model total bytes")?)
        .bind(optional_i64(file_count, "model file count")?)
        .bind(now_ms())
        .bind(id.to_string())
        .execute(self.pool())
        .await?;
        if result.rows_affected() == 0 {
            return Err(StorageError::Conflict(
                "managed model is no longer downloading".to_string(),
            ));
        }
        Ok(())
    }

    pub async fn delete_managed_model(&self, id: Uuid) -> Result<()> {
        let result = sqlx::query("DELETE FROM managed_models WHERE id = ?")
            .bind(id.to_string())
            .execute(self.pool())
            .await?;
        if result.rows_affected() == 0 {
            return Err(StorageError::NotFound {
                entity: "managed model",
            });
        }
        Ok(())
    }

    pub async fn recover_interrupted_model_downloads(&self) -> Result<u64> {
        let result = sqlx::query(
            r#"
            UPDATE managed_models
            SET status = 'failed',
                last_error = 'download was interrupted when desktop-core stopped',
                updated_at_ms = ?
            WHERE status IN ('queued', 'downloading')
            "#,
        )
        .bind(now_ms())
        .execute(self.pool())
        .await?;
        Ok(result.rows_affected())
    }
}

impl TryFrom<ManagedModelRow> for ManagedModel {
    type Error = StorageError;

    fn try_from(row: ManagedModelRow) -> Result<Self> {
        Ok(Self {
            id: Uuid::parse_str(&row.id).map_err(|error| StorageError::CorruptData {
                field: "managed_models.id",
                message: error.to_string(),
            })?,
            model_id: row.model_id,
            display_name: row.display_name,
            source: ManagedModelSource::from_str(&row.source).map_err(|message| {
                StorageError::CorruptData {
                    field: "managed_models.source",
                    message,
                }
            })?,
            source_reference: row.source_reference,
            revision: row.revision,
            local_path: row.local_path,
            status: ManagedModelStatus::from_str(&row.status).map_err(|message| {
                StorageError::CorruptData {
                    field: "managed_models.status",
                    message,
                }
            })?,
            bytes_downloaded: from_i64(row.bytes_downloaded, "managed_models.bytes_downloaded")?,
            total_bytes: optional_u64(row.total_bytes, "managed_models.total_bytes")?,
            file_count: optional_u64(row.file_count, "managed_models.file_count")?,
            manifest_sha256: row.manifest_sha256,
            last_error: row.last_error,
            created_at_ms: row.created_at_ms,
            updated_at_ms: row.updated_at_ms,
        })
    }
}

fn to_i64(value: u64, field: &'static str) -> Result<i64> {
    i64::try_from(value).map_err(|_| StorageError::InvalidInput(format!("{field} exceeds i64")))
}

fn optional_i64(value: Option<u64>, field: &'static str) -> Result<Option<i64>> {
    value.map(|value| to_i64(value, field)).transpose()
}

fn from_i64(value: i64, field: &'static str) -> Result<u64> {
    u64::try_from(value).map_err(|error| StorageError::CorruptData {
        field,
        message: error.to_string(),
    })
}

fn optional_u64(value: Option<i64>, field: &'static str) -> Result<Option<u64>> {
    value.map(|value| from_i64(value, field)).transpose()
}
