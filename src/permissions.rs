use crate::{ProxyError, Result};
use std::fs;
use std::path::Path;
use tracing::{error, info, warn};

/// Validates directory permissions on startup
pub struct PermissionValidator;

impl PermissionValidator {
    /// Validate all required directories have proper permissions
    pub fn validate_all(
        cache_dir: &Path,
        access_log_dir: &Path,
        app_log_dir: &Path,
        write_cache_enabled: bool,
    ) -> Result<()> {
        info!("Validating directory permissions...");

        // Validate cache directory
        Self::validate_directory(cache_dir, "Cache", write_cache_enabled)?;

        // Validate access log directory
        Self::validate_directory(access_log_dir, "Access log", write_cache_enabled)?;

        // Validate app log directory
        Self::validate_directory(app_log_dir, "Application log", write_cache_enabled)?;

        info!("All directory permissions validated successfully");
        Ok(())
    }

    /// Validate a single directory has read/write permissions
    fn validate_directory(
        dir_path: &Path,
        dir_type: &str,
        write_cache_enabled: bool,
    ) -> Result<()> {
        let path = dir_path;

        let dir_path_str = path.display();

        // Try to create directory if it doesn't exist
        if !path.exists() {
            info!(
                "{} directory does not exist, attempting to create: {}",
                dir_type, dir_path_str
            );

            if let Err(e) = fs::create_dir_all(path) {
                error!("{} directory creation failed: {}", dir_type, dir_path_str);
                return Err(ProxyError::ConfigError(format!(
                    "Failed to create {} directory '{}': {}\n\n\
                    Please ensure:\n\
                    1. The parent directory exists and is writable\n\
                    2. You have permission to create directories in this location\n\
                    3. The path is valid and accessible\n\n\
                    Suggested fix:\n\
                    sudo mkdir -p {}\n\
                    sudo chown $USER:$USER {}",
                    dir_type, dir_path_str, e, dir_path_str, dir_path_str
                )));
            }

            info!(
                "{} directory created successfully: {}",
                dir_type, dir_path_str
            );
        }

        // For cache directory, also create subdirectories
        if dir_type == "Cache" {
            // Build subdirectory list based on write_cache_enabled flag
            let mut subdirs = vec!["metadata", "ranges", "locks"];
            if write_cache_enabled {
                subdirs.push("mpus_in_progress");
            }

            for subdir in &subdirs {
                let subdir_path = path.join(subdir);
                if !subdir_path.exists() {
                    if let Err(e) = fs::create_dir_all(&subdir_path) {
                        error!("Failed to create cache subdirectory {}: {}", subdir, e);
                        return Err(ProxyError::ConfigError(format!(
                            "Failed to create cache subdirectory '{}': {}\n\n\
                            Suggested fix:\n\
                            sudo mkdir -p {}\n\
                            sudo chown $USER:$USER {}",
                            subdir,
                            e,
                            subdir_path.display(),
                            subdir_path.display()
                        )));
                    }
                    info!("Created cache subdirectory: {}", subdir);
                }
            }
        }

        // Validate read permission
        if let Err(e) = fs::read_dir(path) {
            error!("{} directory is not readable: {}", dir_type, dir_path_str);
            return Err(ProxyError::ConfigError(format!(
                "Cannot read {} directory '{}': {}\n\n\
                Please ensure you have read permission on this directory.\n\n\
                Suggested fix:\n\
                sudo chmod u+r {}\n\
                sudo chown $USER:$USER {}",
                dir_type, dir_path_str, e, dir_path_str, dir_path_str
            )));
        }

        // Validate write permission by attempting to create a test file.
        // Defense-in-depth: refuse if the probe path is a symlink (prevents an
        // attacker who controls a symlink from redirecting our write outside the
        // intended directory). Uses create_new semantics so pre-existing files
        // (including symlinks) cause a failure rather than being followed.
        let test_file = path.join(".permission_test");

        // Pre-check: if the path already exists as a symlink, remove it before
        // probing. A symlink here is unexpected (we control the name) and would
        // cause create_new to fail — but we want the write probe to succeed on
        // healthy directories, so we clean up defensively.
        if let Ok(meta) = fs::symlink_metadata(&test_file) {
            if meta.file_type().is_symlink() {
                warn!(
                    "Symlink detected at permission test path {:?} — removing (defense-in-depth)",
                    test_file
                );
                let _ = fs::remove_file(&test_file);
            }
        }

        // Use create_new (O_CREAT|O_EXCL) which refuses to follow symlinks
        // and fails if the file already exists — no TOCTOU window.
        let probe_result = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&test_file);

        if let Err(e) = probe_result {
            error!("{} directory is not writable: {}", dir_type, dir_path_str);
            return Err(ProxyError::ConfigError(format!(
                "Cannot write to {} directory '{}': {}\n\n\
                Please ensure you have write permission on this directory.\n\n\
                Suggested fix:\n\
                sudo chmod u+w {}\n\
                sudo chown $USER:$USER {}",
                dir_type, dir_path_str, e, dir_path_str, dir_path_str
            )));
        }

        // Clean up test file — check for symlink before removing (belt and suspenders)
        match fs::symlink_metadata(&test_file) {
            Ok(meta) if meta.file_type().is_symlink() => {
                warn!(
                    "Symlink appeared at permission test path after creation {:?} — skipping removal",
                    test_file
                );
            }
            _ => {
                if let Err(e) = fs::remove_file(&test_file) {
                    warn!("Failed to remove permission test file: {}", e);
                }
            }
        }

        info!(
            "{} directory permissions validated: {}",
            dir_type, dir_path_str
        );
        Ok(())
    }
}

/// An NFS mount holding the cache directory whose options leave negative lookup caching on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegativeLookupCacheMount {
    /// The mount point, unescaped.
    pub mount_point: String,
    /// `nfs` or `nfs4`.
    pub fstype: String,
    /// The per-mount and per-superblock options as `/proc/self/mountinfo` lists them.
    ///
    /// Not for logging: it carries `addr=`, `clientaddr=` and `sec=`. Log
    /// [`Self::relevant_options`].
    pub options: String,
}
impl NegativeLookupCacheMount {
    /// The options the warning is about: `vers`/`nfsvers` and any `lookupcache`, joined with
    /// `, `, followed by `no lookupcache option` when none is set. Every other option is left
    /// out, so the log never carries `addr=`, `clientaddr=`, `mountaddr=` or `sec=`.
    pub(crate) fn relevant_options(&self) -> String {
        let mut kept: Vec<&str> = self
            .options
            .split(',')
            .filter(|o| {
                o.starts_with("lookupcache=") || o.starts_with("vers=") || o.starts_with("nfsvers=")
            })
            .collect();
        if !kept.iter().any(|o| o.starts_with("lookupcache=")) {
            kept.push("no lookupcache option");
        }
        kept.join(", ")
    }
    /// The startup WARN's text for cache directory `dir` on this mount.
    pub(crate) fn warning(&self, dir: &Path) -> String {
        format!(
            "Cache directory {:?} is on {} mount {} whose options ({}) include neither \
             lookupcache=pos nor lookupcache=none. lookupcache=pos is required on a shared \
             cache volume: without it this proxy can cache another proxy's new files and \
             directories as absent, and writes that need them fail. Remount with \
             lookupcache=pos; see Mount requirements in docs/SHARED_STORAGE.md.",
            dir,
            self.fstype,
            self.mount_point,
            self.relevant_options()
        )
    }
}
/// The mount `cache_dir` sits on, from `mountinfo` (the format of `/proc/self/mountinfo`),
/// when it is NFS and its options carry neither `lookupcache=pos` nor `lookupcache=none`.
///
/// The longest mount point that contains `cache_dir` wins; among equal mount points the later
/// line wins, because a later mount stacks on top. `cache_dir` must be absolute and should be
/// canonical, or no mount point will contain it. Pure, so it can be tested over sample lines.
pub fn nfs_negative_lookup_cache_mount(
    mountinfo: &str,
    cache_dir: &Path,
) -> Option<NegativeLookupCacheMount> {
    let mut best: Option<(usize, NegativeLookupCacheMount)> = None;
    for line in mountinfo.lines() {
        // `id parent maj:min root mount_point mount_opts [optional...] - fstype source super_opts`
        let Some((pre, post)) = line.split_once(" - ") else {
            continue;
        };
        let pre: Vec<&str> = pre.split(' ').collect();
        if pre.len() < 6 {
            continue;
        }
        let mount_point = unescape_mountinfo(pre[4]);
        if !cache_dir.starts_with(Path::new(&mount_point)) {
            continue;
        }
        let depth = Path::new(&mount_point).components().count();
        if best.as_ref().is_some_and(|(d, _)| depth < *d) {
            continue;
        }
        let mut post = post.split(' ');
        let fstype = post.next().unwrap_or("").to_string();
        let _source = post.next();
        let super_opts = post.next().unwrap_or("");
        let options = format!("{},{}", pre[5], super_opts);
        best = Some((
            depth,
            NegativeLookupCacheMount {
                mount_point,
                fstype,
                options,
            },
        ));
    }
    let (_, mount) = best?;
    if mount.fstype != "nfs" && mount.fstype != "nfs4" {
        return None;
    }
    let caches_no_negatives = mount
        .options
        .split(',')
        .any(|o| o == "lookupcache=pos" || o == "lookupcache=none");
    if caches_no_negatives {
        None
    } else {
        Some(mount)
    }
}
/// `/proc/self/mountinfo` escapes space, tab, newline and backslash as octal.
fn unescape_mountinfo(field: &str) -> String {
    field
        .replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
}
/// Log one WARN at startup when `cache_dir` is on an NFS mount that caches negative lookups.
///
/// Without `lookupcache=pos` an NFS client can cache "absent" for a directory or file a peer
/// proxy has since created, and a write that needs it then fails. Linux 6.15 and later make one
/// such case reachable: `create_dir_all` returns EEXIST because `mkdir` reaches the server while
/// the `stat` straight after trusts the cached absence. Advisory only: nothing is refused and
/// no behaviour changes. Silent when `/proc/self/mountinfo` is unreadable (not Linux).
pub fn warn_if_nfs_caches_negative_lookups(cache_dir: &Path) {
    let Ok(mountinfo) = fs::read_to_string("/proc/self/mountinfo") else {
        return;
    };
    let dir = fs::canonicalize(cache_dir).unwrap_or_else(|_| cache_dir.to_path_buf());
    if let Some(mount) = nfs_negative_lookup_cache_mount(&mountinfo, &dir) {
        warn!("{}", mount.warning(&dir));
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::TempDir;

    #[test]
    fn test_validate_existing_directory() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path();

        // Should succeed for existing writable directory
        assert!(PermissionValidator::validate_directory(dir_path, "Test", false).is_ok());
    }

    #[test]
    fn test_validate_nonexistent_directory() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().join("subdir");

        // Should create directory and succeed
        assert!(PermissionValidator::validate_directory(&dir_path, "Test", false).is_ok());
        assert!(dir_path.exists());
    }

    #[test]
    fn test_validate_all_directories() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();

        let cache_dir = base_path.join("cache");
        let access_log_dir = base_path.join("logs/access");
        let app_log_dir = base_path.join("logs/app");

        // Should create all directories and succeed
        assert!(PermissionValidator::validate_all(
            &cache_dir,
            &access_log_dir,
            &app_log_dir,
            false
        )
        .is_ok());

        assert!(cache_dir.exists());
        assert!(access_log_dir.exists());
        assert!(app_log_dir.exists());
    }

    #[test]
    fn test_cache_subdirectories_without_write_cache() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().join("cache");

        // Validate with write_cache_enabled = false
        assert!(PermissionValidator::validate_directory(&cache_dir, "Cache", false).is_ok());

        // Check that base subdirectories exist (sharded structure)
        assert!(cache_dir.join("metadata").exists());
        assert!(cache_dir.join("ranges").exists());
        assert!(cache_dir.join("locks").exists());

        // Check that write_cache subdirectory does NOT exist
        assert!(!cache_dir.join("write_cache").exists());
    }

    #[test]
    fn test_cache_subdirectories_with_write_cache() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().join("cache");

        // Validate with write_cache_enabled = true
        assert!(PermissionValidator::validate_directory(&cache_dir, "Cache", true).is_ok());

        // Check that all subdirectories exist including mpus_in_progress
        assert!(cache_dir.join("metadata").exists());
        assert!(cache_dir.join("ranges").exists());
        assert!(cache_dir.join("locks").exists());
        assert!(cache_dir.join("mpus_in_progress").exists());
    }

    /// **Feature: legacy-write-cache-removal, Property 2: Directory structure correctness**
    /// *For any* cache initialization with write_cache_enabled=true, the cache directory
    /// SHALL contain `mpus_in_progress/` but SHALL NOT contain `write_cache/` or `parts/` directories.
    /// **Validates: Requirements 1.3, 1.4, 4.1, 4.2, 4.3**
    #[test]
    fn prop_directory_structure_correctness() {
        use quickcheck::{quickcheck, TestResult};

        fn property(write_cache_enabled: bool) -> TestResult {
            let temp_dir = match TempDir::new() {
                Ok(dir) => dir,
                Err(_) => return TestResult::discard(),
            };
            let cache_dir = temp_dir.path().join("cache");

            // Initialize cache directory
            let result =
                PermissionValidator::validate_directory(&cache_dir, "Cache", write_cache_enabled);
            if result.is_err() {
                return TestResult::discard();
            }

            // Property: Base directories should always exist
            let base_dirs_exist = cache_dir.join("metadata").exists()
                && cache_dir.join("ranges").exists()
                && cache_dir.join("locks").exists();

            if !base_dirs_exist {
                return TestResult::error("Base directories not created");
            }

            // Property: Legacy directories should NEVER exist
            let legacy_write_cache_absent = !cache_dir.join("write_cache").exists();
            let legacy_parts_absent = !cache_dir.join("parts").exists();

            if !legacy_write_cache_absent {
                return TestResult::error("Legacy write_cache directory should not exist");
            }

            if !legacy_parts_absent {
                return TestResult::error("Legacy parts directory should not exist");
            }

            // Property: mpus_in_progress should exist only when write_cache_enabled
            let mpus_exists = cache_dir.join("mpus_in_progress").exists();
            let mpus_correct = if write_cache_enabled {
                mpus_exists
            } else {
                !mpus_exists
            };

            if !mpus_correct {
                return TestResult::error(format!(
                    "mpus_in_progress existence ({}) doesn't match write_cache_enabled ({})",
                    mpus_exists, write_cache_enabled
                ));
            }

            TestResult::passed()
        }

        // Run property test with 100 iterations
        quickcheck(property as fn(bool) -> TestResult);
    }

    /// Symlink planted at the permission-test path must not be followed.
    /// The write probe uses create_new semantics, so a pre-existing symlink is
    /// detected and removed rather than written through. The sentinel target file
    /// must remain untouched.
    #[cfg(unix)]
    #[test]
    fn test_symlink_not_followed_in_write_probe() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().join("test_dir");
        fs::create_dir_all(&dir_path).unwrap();

        // Create a sentinel file outside the test directory
        let sentinel = temp_dir.path().join("sentinel.txt");
        fs::write(&sentinel, b"original content").unwrap();

        // Plant a symlink at the exact path the write probe will use
        let symlink_path = dir_path.join(".permission_test");
        std::os::unix::fs::symlink(&sentinel, &symlink_path).unwrap();

        // Run validation — it should succeed (directory is writable) and the
        // symlink should be safely handled
        let result = PermissionValidator::validate_directory(&dir_path, "Test", false);
        assert!(result.is_ok(), "validate_directory should succeed");

        // Sentinel must still have its original content — never written through
        let content = fs::read_to_string(&sentinel).unwrap();
        assert_eq!(
            content, "original content",
            "Sentinel file must not be modified through symlink"
        );
    }
}

#[cfg(test)]
mod nfs_lookupcache_tests {
    use super::*;
    const ROOT_XFS: &str = "22 1 259:1 / / rw,noatime shared:1 - xfs /dev/nvme0n1p1 rw,attr2,inode64,logbufs=8,logbsize=32k,noquota";
    const EFS_DEFAULT: &str = "412 22 0:52 / /mnt/efs rw,relatime shared:220 - nfs4 fs-0123.efs.us-west-2.amazonaws.com:/ rw,vers=4.1,rsize=1048576,wsize=1048576,namlen=255,hard,noresvport,proto=tcp,timeo=600,retrans=2,sec=sys,clientaddr=10.0.34.221,local_lock=none,addr=10.0.40.1";
    fn mountinfo(lines: &[&str]) -> String {
        lines.join("\n") + "\n"
    }
    #[test]
    fn nfs4_without_lookupcache_warns() {
        let found = nfs_negative_lookup_cache_mount(
            &mountinfo(&[ROOT_XFS, EFS_DEFAULT]),
            Path::new("/mnt/efs/cache"),
        )
        .expect("an nfs4 mount without lookupcache must be reported");
        assert_eq!(found.mount_point, "/mnt/efs");
        assert_eq!(found.fstype, "nfs4");
        assert!(found.options.contains("vers=4.1"), "{}", found.options);
    }
    /// The startup WARN names the mount and only the options it is about: never the NFS
    /// server's or this host's address, nor the security flavour.
    #[test]
    fn the_warning_logs_no_address_or_security_option() {
        let found = nfs_negative_lookup_cache_mount(
            &mountinfo(&[ROOT_XFS, EFS_DEFAULT]),
            Path::new("/mnt/efs/cache"),
        )
        .expect("fixture: reported");
        let text = found.warning(Path::new("/mnt/efs/cache"));
        for leaked in ["10.0.40.1", "10.0.34.221", "addr=", "sec="] {
            assert!(
                !text.contains(leaked),
                "the warning must not log {leaked}: {text}"
            );
        }
        assert!(text.contains("nfs4 mount /mnt/efs "), "{text}");
        assert!(text.contains("(vers=4.1, no lookupcache option)"), "{text}");
    }
    #[test]
    fn nfs4_with_lookupcache_pos_is_silent() {
        let line = EFS_DEFAULT.replace("local_lock=none", "lookupcache=pos,local_lock=none");
        assert_eq!(
            nfs_negative_lookup_cache_mount(
                &mountinfo(&[ROOT_XFS, &line]),
                Path::new("/mnt/efs/cache")
            ),
            None
        );
    }
    #[test]
    fn nfs_with_lookupcache_none_is_silent() {
        let line = EFS_DEFAULT
            .replace(" nfs4 ", " nfs ")
            .replace("vers=4.1", "vers=3,lookupcache=none");
        assert_eq!(
            nfs_negative_lookup_cache_mount(
                &mountinfo(&[ROOT_XFS, &line]),
                Path::new("/mnt/efs/cache")
            ),
            None
        );
    }
    #[test]
    fn local_ext4_or_xfs_is_silent() {
        let ext4 = "40 22 259:2 / /var/cache rw,relatime shared:30 - ext4 /dev/nvme1n1 rw";
        let info = mountinfo(&[ROOT_XFS, ext4]);
        assert_eq!(
            nfs_negative_lookup_cache_mount(&info, Path::new("/var/cache/s3")),
            None
        );
        assert_eq!(
            nfs_negative_lookup_cache_mount(&info, Path::new("/srv/cache")),
            None,
            "the root xfs mount holds this path"
        );
    }
    #[test]
    fn the_deeper_mount_wins() {
        let nested_ext4 = "41 412 259:2 / /mnt/efs/local rw,relatime - ext4 /dev/nvme1n1 rw";
        assert_eq!(
            nfs_negative_lookup_cache_mount(
                &mountinfo(&[ROOT_XFS, EFS_DEFAULT, nested_ext4]),
                Path::new("/mnt/efs/local/cache")
            ),
            None,
            "a local mount nested under the bad NFS mount holds the cache"
        );
        let good_outer = EFS_DEFAULT
            .replace("/mnt/efs ", "/mnt ")
            .replace("local_lock=none", "lookupcache=pos");
        let bad_inner = EFS_DEFAULT.replace("412 22", "413 412");
        let found = nfs_negative_lookup_cache_mount(
            &mountinfo(&[ROOT_XFS, &good_outer, &bad_inner]),
            Path::new("/mnt/efs/cache"),
        )
        .expect("the deeper, bad mount holds the cache");
        assert_eq!(found.mount_point, "/mnt/efs");
    }
    #[test]
    fn a_sibling_with_a_common_prefix_does_not_match() {
        assert_eq!(
            nfs_negative_lookup_cache_mount(
                &mountinfo(&[ROOT_XFS, EFS_DEFAULT]),
                Path::new("/mnt/efs2/cache")
            ),
            None
        );
    }
    #[test]
    fn an_escaped_mount_point_is_unescaped() {
        let line = EFS_DEFAULT.replace("/mnt/efs ", "/mnt/my\\040cache ");
        let found = nfs_negative_lookup_cache_mount(
            &mountinfo(&[ROOT_XFS, &line]),
            Path::new("/mnt/my cache/c"),
        )
        .expect("the escaped mount point must match");
        assert_eq!(found.mount_point, "/mnt/my cache");
    }
}
