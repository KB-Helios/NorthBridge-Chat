use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng},
};
use axum::{
    Extension, Json, Router,
    extract::{Query, State},
    routing::{get, post},
};
use northbridge_domain::{AuthSession, SyncChange, TeamPolicy, TeamRole, TeamUser};
use northbridge_storage::{NewSyncChange, NewTeamPolicy, NewTeamUser, StorageError, now_ms};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{auth::AuthenticatedPrincipal, error::ApiError, routes::ApiState};

const SESSION_TTL_MS: i64 = 30 * 24 * 60 * 60 * 1000;
const MAX_LOGIN_EMAIL_BYTES: usize = 320;
const MAX_PASSWORD_BYTES: usize = 1024;

pub(crate) fn collaboration_routes() -> Router<ApiState> {
    Router::new()
        .route("/v1/auth/bootstrap", post(bootstrap_owner))
        .route("/v1/auth/login", post(login))
        .route("/v1/auth/logout", post(logout))
        .route("/v1/auth/me", get(who_am_i))
        .route("/v1/team/users", get(list_users).post(create_user))
        .route("/v1/team/policies", get(list_policies).post(create_policy))
        .route("/v1/audit/events", get(list_audit_events))
        .route("/v1/audit/verify", get(verify_audit_chain))
        .route("/v1/sync/push", post(push_sync_changes))
        .route("/v1/sync/pull", get(pull_sync_changes))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateUserRequest {
    email: String,
    display_name: String,
    password: String,
    #[serde(default = "default_member_role")]
    role: TeamRole,
}

async fn bootstrap_owner(
    State(state): State<ApiState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<CreateUserRequest>,
) -> Result<Json<TeamUser>, ApiError> {
    if !principal.desktop_session {
        return Err(ApiError::forbidden());
    }
    if state
        .database
        .count_team_users(principal.workspace_id)
        .await?
        != 0
    {
        return Err(ApiError::conflict(
            "team_already_bootstrapped",
            "the team already has an owner",
        ));
    }
    let password_hash = hash_password(request.password).await?;
    Ok(Json(
        state
            .database
            .create_team_user(NewTeamUser {
                workspace_id: principal.workspace_id,
                email: request.email,
                display_name: request.display_name,
                password_hash,
                role: TeamRole::Owner,
            })
            .await?,
    ))
}

#[derive(Debug, Deserialize)]
struct LoginRequest {
    email: String,
    password: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LoginResponse {
    token: String,
    session: AuthSession,
    user: TeamUser,
}

async fn login(
    State(state): State<ApiState>,
    Json(request): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, ApiError> {
    validate_login_input(&request)?;
    let lookup = state.database.team_user_with_password(&request.email).await;
    let (user, hash) = match lookup {
        Ok(value) => value,
        Err(StorageError::NotFound { .. }) => {
            consume_dummy_password_check(request.password).await?;
            audit_login(&state, "denied").await;
            return Err(ApiError::unauthorized());
        }
        Err(error) => return Err(error.into()),
    };
    let password_valid = verify_password(request.password, hash).await?;
    if !user.active || !password_valid {
        audit_login(&state, "denied").await;
        return Err(ApiError::unauthorized());
    }
    let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let token_sha256 = format!("{:x}", Sha256::digest(token.as_bytes()));
    let session = state
        .database
        .create_auth_session(user.id, &token_sha256, now_ms() + SESSION_TTL_MS)
        .await?;
    state
        .database
        .append_audit_event(
            user.workspace_id,
            &format!("user:{}", user.id),
            "LOGIN",
            "/v1/auth/login",
            "success",
            None,
            &json!({ "sessionId": session.id }),
        )
        .await?;
    Ok(Json(LoginResponse {
        token,
        session,
        user,
    }))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LogoutRequest {
    session_id: Uuid,
}

async fn logout(
    State(state): State<ApiState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<LogoutRequest>,
) -> Result<Json<Value>, ApiError> {
    let user_id = principal.user_id.ok_or_else(ApiError::forbidden)?;
    Ok(Json(json!({
        "revoked": state.database.revoke_auth_session(request.session_id, user_id).await?
    })))
}

async fn who_am_i(
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Json<AuthenticatedPrincipalResponse> {
    Json(principal.into())
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AuthenticatedPrincipalResponse {
    id: String,
    user_id: Option<Uuid>,
    workspace_id: Uuid,
    role: TeamRole,
    desktop_session: bool,
}

impl From<AuthenticatedPrincipal> for AuthenticatedPrincipalResponse {
    fn from(value: AuthenticatedPrincipal) -> Self {
        Self {
            id: value.id,
            user_id: value.user_id,
            workspace_id: value.workspace_id,
            role: value.role,
            desktop_session: value.desktop_session,
        }
    }
}

async fn list_users(
    State(state): State<ApiState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<Json<Vec<TeamUser>>, ApiError> {
    require_admin(&principal)?;
    Ok(Json(
        state
            .database
            .list_team_users(principal.workspace_id)
            .await?,
    ))
}

async fn create_user(
    State(state): State<ApiState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<CreateUserRequest>,
) -> Result<Json<TeamUser>, ApiError> {
    require_admin(&principal)?;
    if request.role.rank() >= principal.role.rank() && principal.role != TeamRole::Owner {
        return Err(ApiError::forbidden());
    }
    let password_hash = hash_password(request.password).await?;
    Ok(Json(
        state
            .database
            .create_team_user(NewTeamUser {
                workspace_id: principal.workspace_id,
                email: request.email,
                display_name: request.display_name,
                password_hash,
                role: request.role,
            })
            .await?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreatePolicyRequest {
    name: String,
    method: String,
    path_prefix: String,
    minimum_role: TeamRole,
    effect: String,
}

async fn list_policies(
    State(state): State<ApiState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<Json<Vec<TeamPolicy>>, ApiError> {
    require_admin(&principal)?;
    Ok(Json(
        state
            .database
            .list_team_policies(principal.workspace_id)
            .await?,
    ))
}

async fn create_policy(
    State(state): State<ApiState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<CreatePolicyRequest>,
) -> Result<Json<TeamPolicy>, ApiError> {
    require_admin(&principal)?;
    Ok(Json(
        state
            .database
            .create_team_policy(NewTeamPolicy {
                workspace_id: principal.workspace_id,
                name: request.name,
                method: request.method,
                path_prefix: request.path_prefix,
                minimum_role: request.minimum_role,
                effect: request.effect,
            })
            .await?,
    ))
}

#[derive(Debug, Deserialize)]
struct CursorQuery {
    after: Option<i64>,
    limit: Option<u32>,
}

async fn list_audit_events(
    State(state): State<ApiState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(query): Query<CursorQuery>,
) -> Result<Json<Vec<northbridge_domain::AuditEvent>>, ApiError> {
    require_admin(&principal)?;
    Ok(Json(
        state
            .database
            .list_audit_events(
                principal.workspace_id,
                query.after.unwrap_or(0),
                query.limit.unwrap_or(200),
            )
            .await?,
    ))
}

async fn verify_audit_chain(
    State(state): State<ApiState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&principal)?;
    Ok(Json(json!({
        "valid": state.database.verify_audit_chain().await?
    })))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SyncPushRequest {
    changes: Vec<SyncPushItem>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SyncPushItem {
    entity_type: String,
    entity_id: String,
    base_version: u64,
    #[serde(default)]
    deleted: bool,
    #[serde(default)]
    payload: Value,
    device_id: Uuid,
    client_change_id: String,
}

async fn push_sync_changes(
    State(state): State<ApiState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<SyncPushRequest>,
) -> Result<Json<Vec<SyncChange>>, ApiError> {
    if request.changes.is_empty() || request.changes.len() > 256 {
        return Err(ApiError::invalid(
            "sync push must contain between 1 and 256 changes",
        ));
    }
    let mut applied = Vec::with_capacity(request.changes.len());
    for change in request.changes {
        applied.push(
            state
                .database
                .apply_sync_change(NewSyncChange {
                    workspace_id: principal.workspace_id,
                    entity_type: change.entity_type,
                    entity_id: change.entity_id,
                    base_version: change.base_version,
                    deleted: change.deleted,
                    payload: change.payload,
                    device_id: change.device_id,
                    client_change_id: change.client_change_id,
                })
                .await?,
        );
    }
    Ok(Json(applied))
}

async fn pull_sync_changes(
    State(state): State<ApiState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(query): Query<CursorQuery>,
) -> Result<Json<Vec<SyncChange>>, ApiError> {
    Ok(Json(
        state
            .database
            .pull_sync_changes(
                principal.workspace_id,
                query.after.unwrap_or(0),
                query.limit.unwrap_or(500),
            )
            .await?,
    ))
}

async fn hash_password(password: String) -> Result<String, ApiError> {
    validate_password(&password)?;
    tokio::task::spawn_blocking(move || {
        let salt = SaltString::generate(&mut OsRng);
        Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .map(|hash| hash.to_string())
            .map_err(|error| ApiError::invalid(format!("password hashing failed: {error}")))
    })
    .await
    .map_err(|error| ApiError::invalid(format!("password hashing task failed: {error}")))?
}

async fn verify_password(password: String, encoded: String) -> Result<bool, ApiError> {
    tokio::task::spawn_blocking(move || {
        let parsed = PasswordHash::new(&encoded)
            .map_err(|_| ApiError::invalid("stored password hash is malformed"))?;
        Ok(Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok())
    })
    .await
    .map_err(|error| ApiError::invalid(format!("password verification task failed: {error}")))?
}

async fn consume_dummy_password_check(password: String) -> Result<(), ApiError> {
    tokio::task::spawn_blocking(move || {
        let salt = SaltString::encode_b64(b"northbridge-login-dummy")
            .map_err(|error| ApiError::invalid(format!("dummy password salt failed: {error}")))?;
        Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .map(|_| ())
            .map_err(|error| ApiError::invalid(format!("dummy password check failed: {error}")))
    })
    .await
    .map_err(|error| ApiError::invalid(format!("dummy password task failed: {error}")))?
}

fn validate_login_input(request: &LoginRequest) -> Result<(), ApiError> {
    if request.email.trim().is_empty() || request.email.len() > MAX_LOGIN_EMAIL_BYTES {
        return Err(ApiError::invalid(
            "login email must contain between 1 and 320 bytes",
        ));
    }
    if request.password.len() > MAX_PASSWORD_BYTES {
        return Err(ApiError::invalid("login password cannot exceed 1 KiB"));
    }
    Ok(())
}

fn validate_password(password: &str) -> Result<(), ApiError> {
    if password.chars().count() < 12 || password.len() > MAX_PASSWORD_BYTES {
        return Err(ApiError::invalid(
            "password must contain between 12 characters and 1 KiB",
        ));
    }
    Ok(())
}

fn require_admin(principal: &AuthenticatedPrincipal) -> Result<(), ApiError> {
    if principal.role.rank() < TeamRole::Admin.rank() {
        return Err(ApiError::forbidden());
    }
    Ok(())
}

async fn audit_login(state: &ApiState, outcome: &str) {
    if let Ok(workspace_id) = Uuid::parse_str(northbridge_domain::DEFAULT_WORKSPACE_ID) {
        let _ = state
            .database
            .append_audit_event(
                workspace_id,
                "anonymous",
                "LOGIN",
                "/v1/auth/login",
                outcome,
                None,
                &json!({}),
            )
            .await;
    }
}

const fn default_member_role() -> TeamRole {
    TeamRole::Member
}
