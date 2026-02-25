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

        // Validate write permission by attempting to create a test file
        let test_file = path.join(".permission_test");
        if let Err(e) = fs::write(&test_file, b"test") {
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

        // Clean up test file
        if let Err(e) = fs::remove_file(&test_file) {
            warn!("Failed to remove permission test file: {}", e);
        }

        info!(
            "{} directory permissions validated: {}",
            dir_type, dir_path_str
        );
        Ok(())
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
}
