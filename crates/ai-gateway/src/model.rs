use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{GatewayError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ModelApi {
    Completions,
    Responses,
    Embeddings,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelAlias {
    pub name: String,
    pub upstream_model: String,
    pub apis: BTreeSet<ModelApi>,
}

impl ModelAlias {
    pub fn local_chat(name: impl Into<String>) -> Result<Self> {
        let alias = Self {
            name: name.into(),
            upstream_model: "default".to_string(),
            apis: BTreeSet::from([ModelApi::Completions, ModelApi::Responses]),
        };
        alias.validate()?;
        Ok(alias)
    }

    pub fn local_embedding(name: impl Into<String>) -> Result<Self> {
        let alias = Self {
            name: name.into(),
            upstream_model: "default".to_string(),
            apis: BTreeSet::from([ModelApi::Embeddings]),
        };
        alias.validate()?;
        Ok(alias)
    }

    pub fn validate(&self) -> Result<()> {
        if !valid_alias(&self.name) {
            return Err(GatewayError::InvalidConfig(format!(
                "invalid model alias {:?}; use lowercase letters, digits, and internal hyphens",
                self.name
            )));
        }
        if self.upstream_model.trim().is_empty() || self.upstream_model.len() > 256 {
            return Err(GatewayError::InvalidConfig(format!(
                "model alias {} has an invalid upstream model",
                self.name
            )));
        }
        if self.apis.is_empty() {
            return Err(GatewayError::InvalidConfig(format!(
                "model alias {} must expose at least one API format",
                self.name
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct GatewayEmbeddingResponse {
    pub data: Vec<GatewayEmbeddingData>,
    pub model: String,
    #[serde(default)]
    pub usage: Option<GatewayEmbeddingUsage>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct GatewayEmbeddingData {
    pub embedding: Vec<f32>,
    pub index: usize,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct GatewayEmbeddingUsage {
    #[serde(default)]
    pub prompt_tokens: u64,
    #[serde(default)]
    pub total_tokens: u64,
}

fn valid_alias(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes.len() > 64 {
        return false;
    }
    if !bytes[0].is_ascii_lowercase() || !bytes[bytes.len() - 1].is_ascii_alphanumeric() {
        return false;
    }
    bytes
        .iter()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
}

#[cfg(test)]
mod tests {
    use super::{ModelAlias, ModelApi};

    #[test]
    fn validates_stable_product_aliases() {
        assert!(ModelAlias::local_chat("fast-local").is_ok());
        assert!(ModelAlias::local_chat("Fast Local").is_err());
        assert!(ModelAlias::local_chat("-local").is_err());
        assert!(ModelAlias::local_chat("local-").is_err());
        let embedding = ModelAlias::local_embedding("embedding-default").unwrap();
        assert_eq!(embedding.apis, [ModelApi::Embeddings].into());
    }
}
