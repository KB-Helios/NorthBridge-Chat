use northbridge_domain::SearchHit;
use sqlx::FromRow;
use uuid::Uuid;

use crate::{Database, Result, StorageError};

#[derive(Debug, FromRow)]
struct SearchHitRow {
    message_id: String,
    conversation_id: String,
    part_id: String,
    content: String,
    score: f64,
}

impl Database {
    pub async fn search_messages(
        &self,
        workspace_id: Uuid,
        query: &str,
        limit: u32,
    ) -> Result<Vec<SearchHit>> {
        if !(1..=100).contains(&limit) {
            return Err(StorageError::InvalidInput(
                "search result limit must be between 1 and 100".to_string(),
            ));
        }
        let fts_query = simple_fts_query(query)?;
        let rows = sqlx::query_as::<_, SearchHitRow>(
            "SELECT fts.message_id,
                    fts.conversation_id,
                    fts.part_id,
                    fts.content,
                    bm25(message_parts_fts) AS score
             FROM message_parts_fts AS fts
             JOIN conversations ON conversations.id = fts.conversation_id
             WHERE message_parts_fts MATCH ?
               AND conversations.workspace_id = ?
               AND conversations.deleted_at_ms IS NULL
             ORDER BY score, fts.conversation_id, fts.message_id, fts.part_id
             LIMIT ?",
        )
        .bind(fts_query)
        .bind(workspace_id.to_string())
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(SearchHit {
                    message_id: parse_uuid("search.message_id", &row.message_id)?,
                    conversation_id: parse_uuid("search.conversation_id", &row.conversation_id)?,
                    part_id: parse_uuid("search.part_id", &row.part_id)?,
                    content: row.content,
                    score: row.score,
                })
            })
            .collect()
    }
}

fn simple_fts_query(query: &str) -> Result<String> {
    let tokens = query
        .split_whitespace()
        .filter(|token| !token.is_empty())
        .take(32)
        .map(|token| format!("\"{}\"", token.replace('"', "\"\"")))
        .collect::<Vec<_>>();
    if tokens.is_empty() {
        return Err(StorageError::InvalidInput(
            "search query cannot be empty".to_string(),
        ));
    }
    Ok(tokens.join(" AND "))
}

fn parse_uuid(field: &'static str, value: &str) -> Result<Uuid> {
    Uuid::parse_str(value).map_err(|error| StorageError::CorruptData {
        field,
        message: error.to_string(),
    })
}
