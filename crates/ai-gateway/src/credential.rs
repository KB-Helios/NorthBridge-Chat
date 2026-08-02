use std::fmt;

use uuid::Uuid;

#[derive(Clone, PartialEq, Eq)]
pub struct GatewayCredential(String);

impl GatewayCredential {
    #[must_use]
    pub fn generate() -> Self {
        Self(format!(
            "nb-{}{}",
            Uuid::new_v4().simple(),
            Uuid::new_v4().simple()
        ))
    }

    pub fn parse(value: String) -> crate::Result<Self> {
        if value.len() < 40
            || value.len() > 256
            || value.bytes().any(|byte| byte.is_ascii_whitespace())
        {
            return Err(crate::GatewayError::InvalidConfig(
                "gateway credential must contain 40-256 non-whitespace characters".to_string(),
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for GatewayCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("GatewayCredential([REDACTED])")
    }
}
