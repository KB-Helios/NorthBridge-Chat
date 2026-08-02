use std::sync::Arc;

use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use northbridge_domain::{DEFAULT_WORKSPACE_ID, TeamRole};
use northbridge_storage::Database;
use serde_json::json;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use uuid::Uuid;

use crate::error::{ApiError, CoreApiConfigError};

#[derive(Clone)]
pub(crate) struct AuthToken(Arc<[u8]>);

impl AuthToken {
    pub(crate) fn new(token: String, token_name: &'static str) -> Result<Self, CoreApiConfigError> {
        if token.len() < 32 {
            return Err(CoreApiConfigError::WeakToken(token_name));
        }
        if token.chars().any(char::is_whitespace) {
            return Err(CoreApiConfigError::InvalidToken(token_name));
        }
        Ok(Self(Arc::from(token.into_bytes())))
    }

    pub(crate) fn matches(&self, candidate: &str) -> bool {
        bool::from(candidate.as_bytes().ct_eq(self.0.as_ref()))
    }
}

#[derive(Clone)]
pub(crate) struct AuthState {
    desktop_token: AuthToken,
    database: Database,
}

impl AuthState {
    pub(crate) fn new(desktop_token: AuthToken, database: Database) -> Self {
        Self {
            desktop_token,
            database,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AuthenticatedPrincipal {
    pub(crate) id: String,
    pub(crate) user_id: Option<Uuid>,
    pub(crate) workspace_id: Uuid,
    pub(crate) role: TeamRole,
    pub(crate) desktop_session: bool,
}

pub(crate) async fn require_bearer(
    State(state): State<AuthState>,
    mut request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    if request.uri().path() == "/v1/auth/login" {
        return Ok(next.run(request).await);
    }
    let candidate = request
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    let workspace_id =
        Uuid::parse_str(DEFAULT_WORKSPACE_ID).map_err(|_| ApiError::unauthorized())?;
    let principal = if let Some(candidate) = candidate
        && state.desktop_token.matches(candidate)
    {
        AuthenticatedPrincipal {
            id: "desktop-session".to_string(),
            user_id: None,
            workspace_id,
            role: TeamRole::Owner,
            desktop_session: true,
        }
    } else if let Some(candidate) = candidate {
        let token_hash = format!("{:x}", Sha256::digest(candidate.as_bytes()));
        let user = state
            .database
            .authenticate_session_hash(&token_hash)
            .await?
            .ok_or_else(ApiError::unauthorized)?;
        AuthenticatedPrincipal {
            id: format!("user:{}", user.id),
            user_id: Some(user.id),
            workspace_id: user.workspace_id,
            role: user.role,
            desktop_session: false,
        }
    } else {
        return Err(ApiError::unauthorized());
    };
    if !state
        .database
        .team_policy_allows(
            principal.workspace_id,
            principal.role,
            request.method().as_str(),
            request.uri().path(),
        )
        .await?
    {
        let _ = state
            .database
            .append_audit_event(
                principal.workspace_id,
                &principal.id,
                request.method().as_str(),
                request.uri().path(),
                "403",
                request
                    .headers()
                    .get("x-request-id")
                    .and_then(|value| value.to_str().ok()),
                &json!({
                    "desktopSession": principal.desktop_session,
                    "policyDenied": true
                }),
            )
            .await;
        return Err(ApiError::forbidden());
    }
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let request_id = request
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    request.extensions_mut().insert(principal.clone());
    let response = next.run(request).await;
    if let Err(error) = state
        .database
        .append_audit_event(
            principal.workspace_id,
            &principal.id,
            &method,
            &path,
            response.status().as_str(),
            request_id.as_deref(),
            &json!({ "desktopSession": principal.desktop_session }),
        )
        .await
    {
        tracing::error!(%error, "failed to append API audit event");
    }
    Ok(response)
}
