use std::path::{Path, PathBuf};

/// Paths for split output files (all, asserted, mined)
#[derive(Debug, Clone)]
pub struct SplitOutputPaths {
    pub all: PathBuf,
    pub asserted: PathBuf,
    pub mined: PathBuf,
}

impl SplitOutputPaths {
    /// Generate split paths from a base path
    /// "results.jsonl" -> "results.jsonl", "results_asserted.jsonl", "results_mined.jsonl"
    pub fn from_base<P: AsRef<Path>>(base: P) -> Self {
        let base = base.as_ref();
        let stem = base.file_stem().and_then(|s| s.to_str()).unwrap_or("");
        let extension = base.extension().and_then(|s| s.to_str());
        let parent = base.parent();

        let make_path = |suffix: &str| -> PathBuf {
            let filename = match extension {
                Some(ext) => format!("{}_{}.{}", stem, suffix, ext),
                None => format!("{}_{}", stem, suffix),
            };
            match parent {
                Some(p) if !p.as_os_str().is_empty() => p.join(filename),
                _ => PathBuf::from(filename),
            }
        };

        Self {
            all: base.to_path_buf(),
            asserted: make_path("asserted"),
            mined: make_path("mined"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_path_generation() {
        let paths = SplitOutputPaths::from_base("results.jsonl");
        assert_eq!(paths.all, PathBuf::from("results.jsonl"));
        assert_eq!(paths.asserted, PathBuf::from("results_asserted.jsonl"));
        assert_eq!(paths.mined, PathBuf::from("results_mined.jsonl"));
    }

    #[test]
    fn test_split_path_with_directory() {
        let paths = SplitOutputPaths::from_base("/path/to/output.jsonl");
        assert_eq!(paths.all, PathBuf::from("/path/to/output.jsonl"));
        assert_eq!(paths.asserted, PathBuf::from("/path/to/output_asserted.jsonl"));
        assert_eq!(paths.mined, PathBuf::from("/path/to/output_mined.jsonl"));
    }

    #[test]
    fn test_split_path_no_extension() {
        let paths = SplitOutputPaths::from_base("results");
        assert_eq!(paths.all, PathBuf::from("results"));
        assert_eq!(paths.asserted, PathBuf::from("results_asserted"));
        assert_eq!(paths.mined, PathBuf::from("results_mined"));
    }
}
