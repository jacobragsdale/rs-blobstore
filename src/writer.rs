use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use rand::RngCore;
use tokio::sync::{mpsc, Mutex};

pub struct WriteJob {
    pub path: PathBuf,
    pub bytes: Bytes,
}

pub fn spawn_writers(workers: usize, rx: mpsc::Receiver<WriteJob>) {
    let rx = Arc::new(Mutex::new(rx));
    for id in 0..workers {
        let rx = rx.clone();
        tokio::spawn(async move {
            loop {
                let job = {
                    let mut guard = rx.lock().await;
                    guard.recv().await
                };
                match job {
                    Some(job) => {
                        if let Err(e) = write_atomic(&job).await {
                            tracing::error!(worker = id, path = %job.path.display(), error = %e, "write failed");
                        }
                    }
                    None => break,
                }
            }
        });
    }
}

async fn write_atomic(job: &WriteJob) -> Result<()> {
    if let Some(parent) = job.path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create_dir_all {}", parent.display()))?;
    }

    let mut rand_bytes = [0u8; 8];
    rand::thread_rng().fill_bytes(&mut rand_bytes);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);

    let mut tmp = job.path.clone().into_os_string();
    tmp.push(format!(".tmp.{}.{}", nanos, hex(&rand_bytes)));
    let tmp = PathBuf::from(tmp);

    tokio::fs::write(&tmp, &job.bytes)
        .await
        .with_context(|| format!("write {}", tmp.display()))?;
    tokio::fs::rename(&tmp, &job.path)
        .await
        .with_context(|| format!("rename {} -> {}", tmp.display(), job.path.display()))?;
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0xf) as usize] as char);
    }
    s
}
