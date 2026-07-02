mod path;
mod routes;
mod writer;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use tokio::sync::mpsc;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

use crate::routes::{router, AppState};
use crate::writer::{spawn_writers, ByteBudget, WriteJob};

const DEFAULT_WRITE_QUEUE_MAX_BYTES: &str = "8589934592";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let storage_root = PathBuf::from(env_or("STORAGE_ROOT", "/data"));
    let bind_addr: SocketAddr = env_or("BIND_ADDR", "0.0.0.0:8080").parse()?;
    let queue_cap: usize = env_or("WRITE_QUEUE_CAPACITY", "1024").parse()?;
    let queue_max_bytes: usize =
        env_or("WRITE_QUEUE_MAX_BYTES", DEFAULT_WRITE_QUEUE_MAX_BYTES).parse()?;
    let workers: usize = env_or("WRITE_WORKERS", "4").parse()?;
    let max_body: usize = env_or("MAX_BODY_BYTES", "1073741824").parse()?;

    tokio::fs::create_dir_all(&storage_root).await?;

    let (tx, rx) = mpsc::channel::<WriteJob>(queue_cap);
    spawn_writers(workers, rx);

    let state = AppState {
        storage_root: Arc::new(storage_root.clone()),
        tx,
        queue_byte_budget: Arc::new(ByteBudget::new(queue_max_bytes)),
    };

    let app = router(state)
        .layer(DefaultBodyLimit::disable())
        .layer(RequestBodyLimitLayer::new(max_body))
        .layer(TraceLayer::new_for_http());

    tracing::info!(
        %bind_addr,
        storage_root = %storage_root.display(),
        queue_cap,
        queue_max_bytes,
        workers,
        max_body,
        "rs-blobstore starting"
    );

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}
