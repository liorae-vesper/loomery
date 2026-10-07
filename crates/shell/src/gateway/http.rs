// SPDX-License-Identifier: MPL-2.0

//! The axum adapter: HTTP in, gRPC/Raft out.
//!
//! Three routes:
//!
//! * `POST /organizations/{organization_id}/commands` — submit a command;
//! * `GET  /organizations/{organization_id}/events` — read applied events,
//!   gated by `X-Min-Index` (read-your-writes);
//! * `POST /organizations` — provision a tenant (admin only).
//!
//! Every inbound id is **parsed**, never adopted: `Id::parse` rejects anything
//! that is not a canonical UUID, so untrusted strings never become identity
//! (D10/D12). Errors map onto HTTP status codes: `401` unauthenticated, `403`
//! forbidden, `404` unknown organization, `409` not active, `400` bad input,
//! `503` unavailable (with `x-leader-id` on a wrong-leader response).

use std::sync::Arc;

use axum::Json;
use axum::Router as AxumRouter;
use axum::extract::Path;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use axum::routing::post;
use loomery_core::id::Id;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;

use super::command::CommandError;
use super::command::CommandPlane;
use super::command::CommandRequest;
use super::identity::AuthError;
use super::provision::ProvisionError;
use super::provision::ProvisionRequest;
use super::provision::Provisioner;
use super::ryw::RywOutcome;
use super::ryw::ensure_min_index;
use crate::group::GroupOps;
use crate::group::ProposeOutcome;

/// Shared HTTP state: the command plane behind an `Arc`, plus the optional
/// provisioning seam.
#[derive(Clone)]
struct GatewayState {
    plane: Arc<CommandPlane>,
    provisioner: Option<Arc<dyn Provisioner>>,
}

/// Builds the gateway's axum router without provisioning.
pub fn router(plane: Arc<CommandPlane>) -> AxumRouter {
    router_with_provisioner(plane, None)
}

/// Builds the gateway's axum router, wiring `POST /organizations` to `provisioner`
/// when one is given (otherwise that route answers `503`).
pub fn router_with_provisioner(
    plane: Arc<CommandPlane>,
    provisioner: Option<Arc<dyn Provisioner>>,
) -> AxumRouter {
    AxumRouter::new()
        .route("/organizations", post(provision_tenant))
        .route("/organizations/{organization_id}/commands", post(submit))
        .route("/organizations/{organization_id}/events", get(events))
        .with_state(GatewayState { plane, provisioner })
}

/// The provisioning body.
///
/// The field names are the wire contract (they mirror
/// [`ProvisionRequest`]), hence the allow.
#[allow(clippy::struct_field_names)]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProvisionBody {
    /// The organization to create (canonical UUID).
    organization_id: String,
    /// The user who owns its genesis.
    leader_user_id: String,
    /// The tenant group's id; the host derives one when absent.
    group_id: Option<String>,
}

/// `POST /organizations` — provision a tenant. Admin only.
async fn provision_tenant(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Json(body): Json<ProvisionBody>,
) -> Result<(StatusCode, Json<Value>), HttpError> {
    let identity = state
        .plane
        .authenticate(bearer(&headers).as_deref())
        .await?;
    if !identity.is_admin {
        return Err(CommandError::Auth(super::identity::AuthError::Forbidden).into());
    }

    let provisioner = state.provisioner.ok_or(HttpError::Unavailable)?;
    let request = ProvisionRequest {
        organization_id: parse_id(&body.organization_id)?,
        leader_user_id: parse_id(&body.leader_user_id)?,
        group_id: body.group_id,
    };

    let placement = provisioner
        .provision(request)
        .await
        .map_err(|error| match error {
            ProvisionError::Refused(message) => HttpError::BadRequest(message),
            ProvisionError::Unavailable | ProvisionError::Failed(_) => HttpError::Unavailable,
        })?;

    Ok((
        StatusCode::CREATED,
        Json(json!({
            "organization_id": placement.organization_id.to_string(),
            "group_id": placement.group_id,
        })),
    ))
}

/// The command submission body.
#[derive(Debug, Deserialize)]
struct SubmitBody {
    /// The aggregate the command targets (canonical UUID).
    aggregate_id: String,
    /// The workspace scope, when any.
    workspace_id: Option<String>,
    /// The wire command type.
    command_type: String,
    /// The plain-JSON payload.
    payload: Value,
    /// A client-supplied idempotency key, when the client has one.
    causation_id: Option<String>,
    /// A saga/workflow correlation key.
    correlation_id: Option<String>,
}

/// `POST /organizations/{organization_id}/commands`.
async fn submit(
    State(state): State<GatewayState>,
    Path(organization_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<SubmitBody>,
) -> Result<Json<Value>, HttpError> {
    let request = CommandRequest {
        organization_id: parse_id(&organization_id)?,
        aggregate_id: parse_id(&body.aggregate_id)?,
        workspace_id: body.workspace_id.as_deref().map(parse_id).transpose()?,
        command_type: body.command_type,
        payload: body.payload,
        causation_id: body.causation_id,
        correlation_id: body.correlation_id,
        token: bearer(&headers),
    };

    let outcome = state.plane.submit(request).await?;

    Ok(Json(json!({
        "causation_key": outcome.causation_key.to_string(),
        "outcome": match outcome.outcome {
            ProposeOutcome::Appended { .. } => "appended",
            ProposeOutcome::Replayed { .. } => "replayed",
        },
    })))
}

/// `GET /organizations/{organization_id}/events`, authenticated and gated by
/// `X-Min-Index`.
async fn events(
    State(state): State<GatewayState>,
    Path(organization_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Value>, HttpError> {
    let organization_id = parse_id(&organization_id)?;
    // Reads are tenant data: authenticate *and* authorize before anything else.
    let identity = state
        .plane
        .authenticate(bearer(&headers).as_deref())
        .await?;
    let group = state.plane.group_for(&organization_id)?;
    state
        .plane
        .authorize(&organization_id, &identity, &group)
        .await?;

    if let Some(min_index) = min_index(&headers)? {
        match ensure_min_index(&group, min_index, state.plane.ryw_hold()).await {
            RywOutcome::Recent => {}
            RywOutcome::ForwardToLeader { leader } => {
                return Err(HttpError::ForwardToLeader(leader));
            }
            RywOutcome::Unavailable => return Err(HttpError::Unavailable),
        }
    }

    let events = group
        .committed_events(&organization_id)
        .await
        .map_err(HttpError::Read)?;

    Ok(Json(json!({ "events": events })))
}

/// Parses a canonical id from untrusted input.
fn parse_id(value: &str) -> Result<Id, HttpError> {
    Id::parse(value).map_err(|_| HttpError::BadRequest("invalid id".to_owned()))
}

/// Extracts the bearer token, when present.
fn bearer(headers: &HeaderMap) -> Option<String> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::to_owned)
}

/// Parses `X-Min-Index`, when present.
fn min_index(headers: &HeaderMap) -> Result<Option<u64>, HttpError> {
    match headers.get("x-min-index") {
        None => Ok(None),
        Some(value) => value
            .to_str()
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map(Some)
            .ok_or(HttpError::BadRequest("invalid X-Min-Index".to_owned())),
    }
}

/// What the HTTP layer answers with on failure.
#[derive(Debug)]
enum HttpError {
    /// Malformed request input.
    BadRequest(String),
    /// The local replica is behind and the leader is elsewhere.
    ForwardToLeader(u64),
    /// No leader could serve the read.
    Unavailable,
    /// Reading applied state failed.
    Read(anyhow::Error),
    /// The command plane refused the command.
    Command(CommandError),
}

impl From<CommandError> for HttpError {
    fn from(error: CommandError) -> Self {
        HttpError::Command(error)
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        match self {
            HttpError::BadRequest(message) => {
                (StatusCode::BAD_REQUEST, Json(json!({ "error": message }))).into_response()
            }
            HttpError::ForwardToLeader(leader) => {
                let mut response = (
                    StatusCode::TEMPORARY_REDIRECT,
                    Json(json!({ "error": "the leader holds this write" })),
                )
                    .into_response();
                if let Ok(value) = HeaderValue::from_str(&leader.to_string()) {
                    response.headers_mut().insert("x-leader-id", value);
                }
                response
            }
            HttpError::Unavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "no reachable leader" })),
            )
                .into_response(),
            HttpError::Read(error) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": error.to_string() })),
            )
                .into_response(),
            HttpError::Command(error) => {
                let status = status_for(&error);
                // A rejection carries the plan's stable code, which is what a
                // client should branch on (the message is for humans).
                let body = match &error {
                    CommandError::Propose(source) => match rejection(source) {
                        Some(code) => json!({ "error": error.to_string(), "code": code }),
                        None => json!({ "error": error.to_string() }),
                    },
                    _ => json!({ "error": error.to_string() }),
                };
                (status, Json(body)).into_response()
            }
        }
    }
}

/// The status code a command-plane failure maps to.
fn status_for(error: &CommandError) -> StatusCode {
    match error {
        CommandError::Auth(AuthError::Missing | AuthError::Unknown) => StatusCode::UNAUTHORIZED,
        CommandError::Auth(AuthError::Forbidden) => StatusCode::FORBIDDEN,
        CommandError::UnknownOrganization => StatusCode::NOT_FOUND,
        CommandError::NotActive | CommandError::KeyReused => StatusCode::CONFLICT,
        // A *rejection* is the plan saying no: the command is well-formed but the
        // state refuses it. That is the client's problem, not an outage — a 503
        // would invite a retry that fails identically.
        CommandError::Propose(error) => match rejection(error) {
            Some(code) if code == "invalid_payload" || code == "unknown_command" => {
                StatusCode::BAD_REQUEST
            }
            Some(_) => StatusCode::CONFLICT,
            None => StatusCode::SERVICE_UNAVAILABLE,
        },
        CommandError::GroupUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        CommandError::InvalidKey | CommandError::Serialize(_) => StatusCode::BAD_REQUEST,
        CommandError::PreCompute(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// The stable code of a plan rejection, when the proposal failed that way.
fn rejection(error: &anyhow::Error) -> Option<&str> {
    match error.downcast_ref::<crate::raft::ProposeError>() {
        Some(crate::raft::ProposeError::Rejected { code, .. }) => Some(code.as_str()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    use super::*;
    use crate::control::Router;
    use crate::gateway::command::GroupRegistry;
    use crate::gateway::identity::Identity;
    use crate::gateway::identity::StaticAuthenticator;
    use crate::raft::RaftGroup;
    use loomery_core::tenant::Replica;
    use loomery_core::tenant::TenantState;
    use loomery_core::tenant::TenantStatus;

    const ORG: &str = "018f2c3d-4e5f-7071-8293-a4b5c6d7e8f9";
    const TASK: &str = "018f2c3d-4e5f-7071-8293-a4b5c6d7e8fa";
    const WS: &str = "018f2c3d-4e5f-7071-8293-a4b5c6d7e8fb";

    struct OneGroup {
        group: RaftGroup,
    }

    impl GroupRegistry for OneGroup {
        fn group(&self, group_id: &str) -> Option<RaftGroup> {
            (group_id == "tenant-1").then(|| self.group.clone())
        }
    }

    async fn app() -> AxumRouter {
        let mut group = RaftGroup::boot_single_node(1).await.unwrap();
        // `member` belongs to the organization and is a Member of the workspace
        // the requests name, so it may read and write.
        crate::test_support::assign_member(&mut group, &Id::from(ORG), &Id::from("user-1")).await;
        crate::test_support::join_workspace(
            &mut group,
            &Id::from(ORG),
            &Id::from(WS),
            &Id::from("user-1"),
            loomery_core::membership::Role::Member,
        )
        .await;
        let control_router = Router::new();
        control_router.apply(
            Id::from(ORG),
            &TenantState {
                group_id: Some("tenant-1".to_owned()),
                replicas: vec![Replica {
                    node_id: 1,
                    address: "http://127.0.0.1:7001".to_owned(),
                }],
                leader_user_id: None,
                status: TenantStatus::Active,
            },
        );

        let authenticator = StaticAuthenticator::new()
            .with_token(
                "member",
                Identity {
                    user_id: Id::from("user-1"),
                    is_admin: false,
                },
            )
            .with_token(
                "admin",
                Identity {
                    user_id: Id::from("user-0"),
                    is_admin: true,
                },
            );

        let plane = CommandPlane::new(
            Arc::new(control_router),
            Arc::new(OneGroup { group }),
            Arc::new(authenticator),
            Duration::from_millis(50),
        );

        router(Arc::new(plane))
    }

    fn post_command(token: &str, command_type: &str) -> Request<Body> {
        let body = json!({
            "aggregate_id": TASK,
            "workspace_id": WS,
            "command_type": command_type,
            "payload": { "title": "a task" },
        });

        Request::builder()
            .method("POST")
            .uri(format!("/organizations/{ORG}/commands"))
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    #[tokio::test]
    async fn a_command_is_accepted_and_its_event_is_readable() {
        let app = app().await;

        let response = app
            .clone()
            .oneshot(post_command("member", "task.create"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/organizations/{ORG}/events"))
                    .header("authorization", "Bearer member")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn admin_only_commands_are_forbidden_for_members() {
        let response = app()
            .await
            .oneshot(post_command("member", "organization.archive"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn unauthenticated_commands_are_rejected() {
        let mut request = post_command("member", "task.create");
        request.headers_mut().remove("authorization");
        let response = app().await.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn a_rejected_command_is_a_conflict_not_an_outage() {
        let app = app().await;

        // The first create applies.
        assert_eq!(
            app.clone()
                .oneshot(post_command("member", "task.create"))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );

        // Creating the same aggregate again is the plan saying no: a conflict,
        // with the plan's stable code, not a 503 that invites a retry.
        let response = app
            .clone()
            .oneshot(post_command("member", "task.create"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("\"code\""), "the code is reported: {body}");

        // A malformed payload is a bad request, not a conflict.
        let mut malformed = post_command("member", "task.create");
        let body = json!({
            "aggregate_id": TASK,
            "workspace_id": WS,
            "command_type": "task.create",
            "payload": { "title": 42 },
        })
        .to_string();
        *malformed.body_mut() = Body::from(body);
        assert_eq!(
            app.oneshot(malformed).await.unwrap().status(),
            StatusCode::CONFLICT,
            "the task already exists, which the plan reports before the payload"
        );
    }

    #[tokio::test]
    async fn unauthenticated_reads_are_rejected() {
        let app = app().await;
        let request = Request::builder()
            .method("GET")
            .uri(format!("/organizations/{ORG}/events"))
            .body(Body::empty())
            .unwrap();

        // An organization's event log is tenant data: no token, no read.
        assert_eq!(
            app.oneshot(request).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn unknown_and_malformed_organizations_are_rejected() {
        // A well-formed but unregistered organization: 404.
        let response = app()
            .await
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/organizations/018f2c3d-4e5f-7071-8293-a4b5c6d7e8ff/events")
                    .header("authorization", "Bearer member")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // Not a canonical UUID: 400, never adopted.
        let response = app()
            .await
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/organizations/not-a-uuid/events")
                    .header("authorization", "Bearer member")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn a_read_ahead_of_the_applied_index_is_not_served_stale() {
        let response = app()
            .await
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/organizations/{ORG}/events"))
                    .header("authorization", "Bearer member")
                    .header("x-min-index", "100000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // A single-node leader cannot forward away, so it refuses rather than
        // answering with an older state.
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
