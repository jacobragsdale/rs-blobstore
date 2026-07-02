use std::path::PathBuf;
use std::sync::Arc;

use axum::{
    body::Body,
    extract::{FromRequest, Path, State},
    http::{header, HeaderMap, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_util::io::ReaderStream;

use crate::path::resolve_safe;
use crate::writer::{ByteBudget, ByteBudgetError, WriteJob};

const GET_STREAM_CAPACITY: usize = 256 * 1024;

enum ContentLengthError {
    Missing,
    Invalid,
}

#[derive(Clone)]
pub struct AppState {
    pub storage_root: Arc<PathBuf>,
    pub tx: mpsc::Sender<WriteJob>,
    pub queue_byte_budget: Arc<ByteBudget>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/blobs/*path", get(get_blob).post(post_blob))
        .with_state(state)
}

async fn healthz() -> &'static str {
    "ok"
}

async fn post_blob(
    State(state): State<AppState>,
    Path(path): Path<String>,
    req: Request<Body>,
) -> Response {
    let safe_path = match resolve_safe(&state.storage_root, &path) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(path = %path, error = %e, "rejected POST path");
            return (StatusCode::BAD_REQUEST, format!("invalid path: {e}")).into_response();
        }
    };

    let body_len = match content_length(req.headers()) {
        Ok(len) => len,
        Err(e) => return content_length_error(e),
    };

    let permit = match state.tx.clone().try_reserve_owned() {
        Ok(permit) => permit,
        Err(mpsc::error::TrySendError::Full(_)) => {
            return (StatusCode::SERVICE_UNAVAILABLE, "write queue full, retry").into_response();
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "writer shut down").into_response();
        }
    };

    let mut byte_reservation = match state.queue_byte_budget.try_reserve(body_len) {
        Ok(reservation) => reservation,
        Err(e) => return queue_byte_budget_error(e, state.queue_byte_budget.max_bytes()),
    };

    let body = match Bytes::from_request(req, &state).await {
        Ok(body) => body,
        Err(rejection) => return rejection.into_response(),
    };

    if let Err(e) = byte_reservation.resize(body.len()) {
        return queue_byte_budget_error(e, state.queue_byte_budget.max_bytes());
    }

    let job = WriteJob {
        path: safe_path,
        bytes: body,
        _byte_reservation: byte_reservation,
    };

    permit.send(job);
    StatusCode::ACCEPTED.into_response()
}

async fn get_blob(State(state): State<AppState>, Path(path): Path<String>) -> Response {
    let safe_path = match resolve_safe(&state.storage_root, &path) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(path = %path, error = %e, "rejected GET path");
            return (StatusCode::BAD_REQUEST, format!("invalid path: {e}")).into_response();
        }
    };

    let file = match tokio::fs::File::open(&safe_path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return StatusCode::NOT_FOUND.into_response();
        }
        Err(e) => {
            tracing::error!(path = %safe_path.display(), error = %e, "open failed");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let len = file.metadata().await.ok().map(|m| m.len());
    let stream = ReaderStream::with_capacity(file, GET_STREAM_CAPACITY);
    let body = Body::from_stream(stream);

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream");
    if let Some(l) = len {
        builder = builder.header(header::CONTENT_LENGTH, l);
    }
    match builder.body(body) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "build response failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

fn content_length(headers: &HeaderMap) -> Result<usize, ContentLengthError> {
    let Some(value) = headers.get(header::CONTENT_LENGTH) else {
        return Err(ContentLengthError::Missing);
    };

    value
        .to_str()
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .ok_or(ContentLengthError::Invalid)
}

fn content_length_error(error: ContentLengthError) -> Response {
    match error {
        ContentLengthError::Missing => {
            (StatusCode::LENGTH_REQUIRED, "content-length required").into_response()
        }
        ContentLengthError::Invalid => {
            (StatusCode::BAD_REQUEST, "invalid content-length").into_response()
        }
    }
}

fn queue_byte_budget_error(error: ByteBudgetError, max_bytes: usize) -> Response {
    match error {
        ByteBudgetError::Full => (
            StatusCode::SERVICE_UNAVAILABLE,
            "write queue byte limit reached, retry",
        )
            .into_response(),
        ByteBudgetError::TooLarge => (
            StatusCode::PAYLOAD_TOO_LARGE,
            format!("body exceeds write queue byte limit of {max_bytes} bytes"),
        )
            .into_response(),
    }
}
