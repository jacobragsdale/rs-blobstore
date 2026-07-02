use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use anyhow::{Context, Result};
use bytes::Bytes;
use rand::RngCore;
use tokio::sync::{mpsc, Mutex};

pub struct WriteJob {
    pub path: PathBuf,
    pub bytes: Bytes,
    pub _byte_reservation: ByteReservation,
}

pub struct ByteBudget {
    max_bytes: usize,
    used_bytes: AtomicUsize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteBudgetError {
    Full,
    TooLarge,
}

pub struct ByteReservation {
    budget: Arc<ByteBudget>,
    bytes: usize,
}

impl ByteBudget {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            used_bytes: AtomicUsize::new(0),
        }
    }

    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    #[cfg(test)]
    pub fn used_bytes(&self) -> usize {
        self.used_bytes.load(Ordering::Relaxed)
    }

    pub fn try_reserve(self: &Arc<Self>, bytes: usize) -> Result<ByteReservation, ByteBudgetError> {
        if bytes > self.max_bytes {
            return Err(ByteBudgetError::TooLarge);
        }

        self.used_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |used| {
                used.checked_add(bytes)
                    .filter(|new_used| *new_used <= self.max_bytes)
            })
            .map_err(|_| ByteBudgetError::Full)?;

        Ok(ByteReservation {
            budget: Arc::clone(self),
            bytes,
        })
    }
}

pub fn spawn_writers(workers: usize, rx: mpsc::Receiver<WriteJob>) {
    let rx = Arc::new(Mutex::new(rx));
    for id in 0..workers {
        let rx = rx.clone();
        tokio::spawn(async move {
            let mut created_dirs = HashSet::new();
            loop {
                let job = {
                    let mut guard = rx.lock().await;
                    guard.recv().await
                };
                match job {
                    Some(job) => {
                        if let Err(e) = write_atomic(&job, &mut created_dirs).await {
                            tracing::error!(worker = id, path = %job.path.display(), error = %e, "write failed");
                        }
                    }
                    None => break,
                }
            }
        });
    }
}

impl ByteReservation {
    pub fn resize(&mut self, bytes: usize) -> Result<(), ByteBudgetError> {
        if bytes > self.bytes {
            let extra = bytes - self.bytes;
            self.budget.add_bytes(extra)?;
        } else {
            let unused = self.bytes - bytes;
            self.budget.release(unused);
        }

        self.bytes = bytes;
        Ok(())
    }
}

impl ByteBudget {
    fn add_bytes(&self, bytes: usize) -> Result<(), ByteBudgetError> {
        if bytes > self.max_bytes {
            return Err(ByteBudgetError::TooLarge);
        }

        self.used_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |used| {
                used.checked_add(bytes)
                    .filter(|new_used| *new_used <= self.max_bytes)
            })
            .map_err(|_| ByteBudgetError::Full)?;

        Ok(())
    }

    fn release(&self, bytes: usize) {
        if bytes > 0 {
            self.used_bytes.fetch_sub(bytes, Ordering::AcqRel);
        }
    }
}

impl Drop for ByteReservation {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

async fn write_atomic(job: &WriteJob, created_dirs: &mut HashSet<PathBuf>) -> Result<()> {
    if let Some(parent) = job.path.parent() {
        if !created_dirs.contains(parent) {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create_dir_all {}", parent.display()))?;
            created_dirs.insert(parent.to_path_buf());
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_budget_tracks_reservations() {
        let budget = Arc::new(ByteBudget::new(10));
        let reservation = budget.try_reserve(4).unwrap();

        assert_eq!(budget.used_bytes(), 4);
        drop(reservation);
        assert_eq!(budget.used_bytes(), 0);
    }

    #[test]
    fn byte_budget_rejects_when_full() {
        let budget = Arc::new(ByteBudget::new(10));
        let _reservation = budget.try_reserve(8).unwrap();

        assert!(matches!(budget.try_reserve(3), Err(ByteBudgetError::Full)));
    }

    #[test]
    fn byte_budget_rejects_single_too_large_reservation() {
        let budget = Arc::new(ByteBudget::new(10));

        assert!(matches!(
            budget.try_reserve(11),
            Err(ByteBudgetError::TooLarge)
        ));
    }

    #[test]
    fn byte_reservation_can_shrink() {
        let budget = Arc::new(ByteBudget::new(10));
        let mut reservation = budget.try_reserve(8).unwrap();

        reservation.resize(3).unwrap();

        assert_eq!(budget.used_bytes(), 3);
    }
}
