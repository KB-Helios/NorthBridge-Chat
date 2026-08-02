use serde_json::Value;

use crate::{Database, Result, StorageError, now_ms};

impl Database {
    pub async fn get_json_setting(&self, scope: &str, key: &str) -> Result<Option<Value>> {
        let (scope, key) = validate_setting_key(scope, key)?;
        let value = sqlx::query_scalar::<_, String>(
            "SELECT value_json FROM settings WHERE scope = ? AND key = ?",
        )
        .bind(scope)
        .bind(key)
        .fetch_optional(self.pool())
        .await?;
        value
            .map(|value| {
                serde_json::from_str(&value).map_err(|error| StorageError::CorruptData {
                    field: "setting.value_json",
                    message: error.to_string(),
                })
            })
            .transpose()
    }

    pub async fn set_json_setting(&self, scope: &str, key: &str, value: &Value) -> Result<()> {
        let (scope, key) = validate_setting_key(scope, key)?;
        let value = serde_json::to_string(value)
            .map_err(|error| StorageError::InvalidInput(error.to_string()))?;
        if value.len() > 1024 * 1024 {
            return Err(StorageError::InvalidInput(
                "setting value cannot exceed 1 MiB".to_string(),
            ));
        }
        sqlx::query(
            "INSERT INTO settings (scope, key, value_json, updated_at_ms)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(scope, key) DO UPDATE SET
                 value_json = excluded.value_json,
                 updated_at_ms = excluded.updated_at_ms",
        )
        .bind(scope)
        .bind(key)
        .bind(value)
        .bind(now_ms())
        .execute(self.pool())
        .await?;
        Ok(())
    }

    pub async fn delete_setting(&self, scope: &str, key: &str) -> Result<bool> {
        let (scope, key) = validate_setting_key(scope, key)?;
        let result = sqlx::query("DELETE FROM settings WHERE scope = ? AND key = ?")
            .bind(scope)
            .bind(key)
            .execute(self.pool())
            .await?;
        Ok(result.rows_affected() > 0)
    }
}

fn validate_setting_key<'a>(scope: &'a str, key: &'a str) -> Result<(&'a str, &'a str)> {
    let scope = scope.trim();
    let key = key.trim();
    if scope.is_empty() || scope.len() > 100 || key.is_empty() || key.len() > 200 {
        return Err(StorageError::InvalidInput(
            "setting scope/key must be non-empty and within their size limits".to_string(),
        ));
    }
    Ok((scope, key))
}
