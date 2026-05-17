use std::path::PathBuf;
use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_util::io::ReaderStream;

use crate::path::resolve_safe;
use crate::writer::WriteJob;

#[derive(Clone)]
pub struct AppState {
    pub storage_root: Arc<PathBuf>,
    pub tx: mpsc::Sender<WriteJob>,
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
    body: Bytes,
) -> Response {
    let safe_path = match resolve_safe(&state.storage_root, &path) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(path = %path, error = %e, "rejected POST path");
            return (StatusCode::BAD_REQUEST, format!("invalid path: {e}")).into_response();
        }
    };

    let job = WriteJob {
        path: safe_path,
        bytes: body,
    };

    match state.tx.try_send(job) {
        Ok(_) => StatusCode::ACCEPTED.into_response(),
        Err(mpsc::error::TrySendError::Full(_)) => {
            (StatusCode::SERVICE_UNAVAILABLE, "write queue full, retry").into_response()
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            (StatusCode::INTERNAL_SERVER_ERROR, "writer shut down").into_response()
        }
    }
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
    let stream = ReaderStream::new(file);
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
