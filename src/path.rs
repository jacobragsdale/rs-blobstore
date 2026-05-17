use std::path::{Component, Path, PathBuf};

use anyhow::{anyhow, Result};

pub fn resolve_safe(root: &Path, user_path: &str) -> Result<PathBuf> {
    if user_path.is_empty() {
        return Err(anyhow!("empty path"));
    }
    if user_path.as_bytes().contains(&0) {
        return Err(anyhow!("NUL byte in path"));
    }

    let p = Path::new(user_path);
    if p.is_absolute() {
        return Err(anyhow!("absolute paths not allowed"));
    }

    for c in p.components() {
        match c {
            Component::Normal(_) => {}
            Component::CurDir => {}
            _ => return Err(anyhow!("path component not allowed: {c:?}")),
        }
    }

    let joined = root.join(p);

    let mut depth: i32 = 0;
    for c in Path::new(user_path).components() {
        match c {
            Component::Normal(_) => depth += 1,
            Component::ParentDir => depth -= 1,
            _ => {}
        }
        if depth < 0 {
            return Err(anyhow!("path escapes root"));
        }
    }

    Ok(joined)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root() -> PathBuf {
        PathBuf::from("/data")
    }

    #[test]
    fn ok_simple() {
        let p = resolve_safe(&root(), "foo/bar.bin").unwrap();
        assert_eq!(p, PathBuf::from("/data/foo/bar.bin"));
    }

    #[test]
    fn ok_dotted_filename() {
        let p = resolve_safe(&root(), "foo/bar.parquet.gz").unwrap();
        assert_eq!(p, PathBuf::from("/data/foo/bar.parquet.gz"));
    }

    #[test]
    fn rejects_parent_dir() {
        assert!(resolve_safe(&root(), "../etc/passwd").is_err());
        assert!(resolve_safe(&root(), "foo/../../etc/passwd").is_err());
    }

    #[test]
    fn rejects_absolute() {
        assert!(resolve_safe(&root(), "/etc/passwd").is_err());
    }

    #[test]
    fn rejects_nul() {
        assert!(resolve_safe(&root(), "foo\0bar").is_err());
    }

    #[test]
    fn rejects_empty() {
        assert!(resolve_safe(&root(), "").is_err());
    }
}
