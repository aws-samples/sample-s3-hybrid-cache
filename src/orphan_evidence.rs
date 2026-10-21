//! Strict journal evidence for orphan reclamation (R15, unit ORPHAN-U3).
//!
//! Answers one question: is an `Add` pending for `(cache_key, start, end)` in any
//! instance's journal under `metadata/_journals/`? A `.bin` that no `.meta` lists may
//! still be a fill whose `Add` has not been consolidated yet, so reclaiming it on
//! anything weaker than proof of absence deletes live data.
//!
//! The reader fails closed. Only [`JournalEvidence::ConfirmedAbsent`] permits
//! reclamation, and it is returned only when the directory listing succeeded, no
//! `.journal.tmp` was present, and every listed `.journal` was read in full and every
//! line in it parsed. Anything the reader could not see maps to a non-absent outcome.
//! This is the opposite policy to `unfolded_journal_add_ranges`, which skips files it
//! cannot read and is therefore unfit for this question.
//!
//! [`JournalEvidenceReader`] keeps a per-leaf snapshot (`name`, `len`, `mtime`) so a
//! reclaimer can call [`JournalEvidenceReader::refresh`] under each candidate's slot
//! lock and re-read only the journals that changed since the last refresh.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::journal_manager::{JournalEntry, JournalOperation};

/// The outcome of a journal-evidence query. Only `ConfirmedAbsent` permits reclamation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalEvidence {
    /// An `Add` for the extent is pending in some journal.
    JournalPresent,
    /// A listed journal could not be opened, stat'ed or read (any error but NotFound).
    PeerUnavailable,
    /// The directory listing failed, a line was torn or unparseable, or a
    /// `.journal.tmp` was present.
    Indeterminate,
    /// Every journal was read in full and none holds an `Add` for the extent.
    ConfirmedAbsent,
}

impl JournalEvidence {
    /// Whether this outcome permits reclaiming the extent. True for `ConfirmedAbsent` only.
    #[cfg(test)]
    pub fn permits_reclamation(self) -> bool {
        matches!(self, JournalEvidence::ConfirmedAbsent)
    }
}

/// One journal file as last observed: its name, length and modification time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafSnapshot {
    pub name: String,
    pub len: u64,
    pub mtime: Option<SystemTime>,
}

#[derive(Debug, Clone)]
enum LeafState {
    /// Read in full; the `(cache_key, start, end)` of every `Add` line.
    Parsed(HashSet<(String, u64, u64)>),
    /// Open, stat or read failed with an error other than NotFound.
    Unavailable,
    /// A line was torn or unparseable, or the content was not UTF-8.
    Indeterminate,
}

#[derive(Debug, Clone)]
struct Leaf {
    snapshot: LeafSnapshot,
    state: LeafState,
}

/// Reads every instance's journal and answers [`JournalEvidence`] queries, re-reading
/// only journals whose snapshot changed on each [`refresh`](Self::refresh).
#[derive(Debug)]
pub struct JournalEvidenceReader {
    journals_dir: PathBuf,
    leaves: BTreeMap<String, Leaf>,
    /// The last refresh's directory listing failed (or none has run yet).
    listing_failed: bool,
    /// The last refresh saw a `.journal.tmp`.
    tmp_present: bool,
}

impl JournalEvidenceReader {
    /// A reader over `{cache_dir}/metadata/_journals`. Nothing is read until
    /// [`refresh`](Self::refresh); before that every query is `Indeterminate`.
    pub fn new(cache_dir: &Path) -> Self {
        Self {
            journals_dir: cache_dir.join("metadata").join("_journals"),
            leaves: BTreeMap::new(),
            listing_failed: true,
            tmp_present: false,
        }
    }

    /// Re-list the journals directory and re-read each `.journal` whose `(len, mtime)`
    /// differs from the stored snapshot (or that is new). Returns how many were re-read.
    pub fn refresh(&mut self) -> usize {
        let entries = match std::fs::read_dir(&self.journals_dir) {
            Ok(entries) => entries,
            Err(_) => {
                self.listing_failed = true;
                self.leaves.clear();
                return 0;
            }
        };
        self.listing_failed = false;
        self.tmp_present = false;
        let mut seen: HashSet<String> = HashSet::new();
        let mut reread = 0usize;
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => {
                    self.listing_failed = true;
                    continue;
                }
            };
            let name = match entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => {
                    // A name this reader cannot represent could be a journal.
                    self.listing_failed = true;
                    continue;
                }
            };
            if name.ends_with(".journal.tmp") {
                self.tmp_present = true;
                continue;
            }
            if !name.ends_with(".journal") {
                continue;
            }
            let path = entry.path();
            let meta = match std::fs::metadata(&path) {
                Ok(meta) => meta,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(_) => {
                    seen.insert(name.clone());
                    self.leaves.insert(
                        name.clone(),
                        Leaf {
                            snapshot: LeafSnapshot {
                                name,
                                len: 0,
                                mtime: None,
                            },
                            state: LeafState::Unavailable,
                        },
                    );
                    continue;
                }
            };
            let snapshot = LeafSnapshot {
                name: name.clone(),
                len: meta.len(),
                mtime: meta.modified().ok(),
            };
            let unchanged = self.leaves.get(&name).is_some_and(|leaf| {
                leaf.snapshot == snapshot
                    && snapshot.mtime.is_some()
                    && !matches!(leaf.state, LeafState::Unavailable)
            });
            if unchanged {
                seen.insert(name);
                continue;
            }
            reread += 1;
            match read_leaf(&path) {
                Some(state) => {
                    seen.insert(name.clone());
                    self.leaves.insert(name, Leaf { snapshot, state });
                }
                // Removed between the listing and the read: nothing pending in it.
                None => {
                    self.leaves.remove(&name);
                }
            }
        }
        self.leaves.retain(|name, _| seen.contains(name));
        reread
    }

    /// Whether an `Add` for `(cache_key, start, end)` is pending, as of the last refresh.
    ///
    /// Precedence: `JournalPresent` (any journal holds the `Add`), then
    /// `PeerUnavailable`, then `Indeterminate`, then `ConfirmedAbsent`.
    pub fn evidence(&self, cache_key: &str, start: u64, end: u64) -> JournalEvidence {
        let wanted = (cache_key.to_string(), start, end);
        let mut unavailable = false;
        let mut indeterminate = self.listing_failed || self.tmp_present;
        for leaf in self.leaves.values() {
            match &leaf.state {
                LeafState::Parsed(adds) => {
                    if adds.contains(&wanted) {
                        return JournalEvidence::JournalPresent;
                    }
                }
                LeafState::Unavailable => unavailable = true,
                LeafState::Indeterminate => indeterminate = true,
            }
        }
        if unavailable {
            JournalEvidence::PeerUnavailable
        } else if indeterminate {
            JournalEvidence::Indeterminate
        } else {
            JournalEvidence::ConfirmedAbsent
        }
    }

    /// The per-leaf snapshot as of the last refresh, in name order.
    pub fn snapshot(&self) -> Vec<LeafSnapshot> {
        self.leaves
            .values()
            .map(|leaf| leaf.snapshot.clone())
            .collect()
    }
}

/// One-shot query: list and read every journal, then answer for the extent.
pub fn journal_evidence(
    cache_dir: &Path,
    cache_key: &str,
    start: u64,
    end: u64,
) -> JournalEvidence {
    let mut reader = JournalEvidenceReader::new(cache_dir);
    reader.refresh();
    reader.evidence(cache_key, start, end)
}

/// Read one journal in full. `None` means it vanished (NotFound) and holds nothing.
fn read_leaf(path: &Path) -> Option<LeafState> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(_) => return Some(LeafState::Unavailable),
    };
    let Ok(content) = String::from_utf8(bytes) else {
        return Some(LeafState::Indeterminate);
    };
    // Every append ends with a newline, so a final segment without one is a torn
    // (in-progress or interrupted) write, whatever it parses as.
    if !content.is_empty() && !content.ends_with('\n') {
        return Some(LeafState::Indeterminate);
    }
    let mut adds = HashSet::new();
    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        // The journal line parser: the same `serde_json` decode of `JournalEntry` every
        // journal reader uses.
        let entry: JournalEntry = match serde_json::from_str(line) {
            Ok(entry) => entry,
            Err(_) => return Some(LeafState::Indeterminate),
        };
        if entry.operation == JournalOperation::Add {
            adds.insert((
                entry.cache_key,
                entry.range_spec.start,
                entry.range_spec.end,
            ));
        }
    }
    Some(LeafState::Parsed(adds))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache_types::RangeSpec;
    use crate::compression::CompressionAlgorithm;
    use tempfile::TempDir;

    const KEY: &str = "test-bucket/scn1/object";

    fn journals(temp: &TempDir) -> PathBuf {
        let dir = temp.path().join("metadata").join("_journals");
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn line(key: &str, start: u64, end: u64, operation: JournalOperation) -> String {
        let now = SystemTime::now();
        let entry = JournalEntry {
            timestamp: now,
            instance_id: "peer".to_string(),
            cache_key: key.to_string(),
            range_spec: RangeSpec::new(
                start,
                end,
                format!("b/x_{start}-{end}.bin"),
                CompressionAlgorithm::Lz4,
                end - start + 1,
                end - start + 1,
            ),
            operation,
            range_file_path: format!("b/x_{start}-{end}.bin"),
            metadata_version: 1,
            new_ttl_secs: None,
            object_ttl_secs: None,
            access_increment: None,
            object_metadata: None,
            fill_witness: None,
        };
        let mut text = serde_json::to_string(&entry).unwrap();
        text.push('\n');
        text
    }

    #[test]
    fn journal_present_when_an_add_is_pending() {
        let temp = TempDir::new().unwrap();
        let dir = journals(&temp);
        std::fs::write(
            dir.join("a.journal"),
            line(KEY, 0, 99, JournalOperation::Add),
        )
        .unwrap();
        assert_eq!(
            journal_evidence(temp.path(), KEY, 0, 99),
            JournalEvidence::JournalPresent
        );
        // A different extent, and a non-Add for this one, are not evidence.
        std::fs::write(
            dir.join("a.journal"),
            line(KEY, 100, 199, JournalOperation::Add)
                + &line(KEY, 0, 99, JournalOperation::Remove),
        )
        .unwrap();
        assert_eq!(
            journal_evidence(temp.path(), KEY, 0, 99),
            JournalEvidence::ConfirmedAbsent
        );
    }

    #[test]
    fn confirmed_absent_only_when_every_journal_was_read() {
        let temp = TempDir::new().unwrap();
        let dir = journals(&temp);
        std::fs::write(
            dir.join("a.journal"),
            line("other/key", 0, 99, JournalOperation::Add),
        )
        .unwrap();
        std::fs::write(dir.join("b.journal"), "").unwrap();
        std::fs::write(dir.join("a.journal.lock"), "").unwrap();
        let evidence = journal_evidence(temp.path(), KEY, 0, 99);
        assert_eq!(evidence, JournalEvidence::ConfirmedAbsent);
        assert!(evidence.permits_reclamation());
    }

    /// A listed journal that cannot be read (here: a `.journal` that is a directory, which
    /// fails the read with a non-NotFound error whoever runs the test) is a peer whose
    /// pending Adds are unknown. A fail-open reader that skips it, as
    /// `unfolded_journal_add_ranges` does, would answer `ConfirmedAbsent`.
    #[test]
    fn peer_unavailable_when_a_listed_journal_cannot_be_read() {
        let temp = TempDir::new().unwrap();
        let dir = journals(&temp);
        std::fs::write(
            dir.join("a.journal"),
            line("other/key", 0, 99, JournalOperation::Add),
        )
        .unwrap();
        std::fs::create_dir(dir.join("peer.journal")).unwrap();
        let evidence = journal_evidence(temp.path(), KEY, 0, 99);
        assert_eq!(
            evidence,
            JournalEvidence::PeerUnavailable,
            "an unreadable peer journal must never read as confirmed_absent"
        );
        assert!(!evidence.permits_reclamation());
    }

    #[test]
    fn indeterminate_when_the_listing_fails() {
        let temp = TempDir::new().unwrap();
        // No `_journals` directory at all: the readdir fails.
        assert_eq!(
            journal_evidence(temp.path(), KEY, 0, 99),
            JournalEvidence::Indeterminate
        );
        // And a reader that has never refreshed knows nothing.
        let reader = JournalEvidenceReader::new(temp.path());
        assert_eq!(reader.evidence(KEY, 0, 99), JournalEvidence::Indeterminate);
    }

    #[test]
    fn indeterminate_on_a_torn_line() {
        let temp = TempDir::new().unwrap();
        let dir = journals(&temp);
        let full = line("other/key", 0, 99, JournalOperation::Add);
        // A complete line followed by half of the next append.
        let torn = full.clone() + &full[..full.len() / 2];
        std::fs::write(dir.join("a.journal"), torn).unwrap();
        assert_eq!(
            journal_evidence(temp.path(), KEY, 0, 99),
            JournalEvidence::Indeterminate
        );
        // An unparseable but newline-terminated line is indeterminate too.
        std::fs::write(dir.join("a.journal"), full + "{not json\n").unwrap();
        assert_eq!(
            journal_evidence(temp.path(), KEY, 0, 99),
            JournalEvidence::Indeterminate
        );
    }

    #[test]
    fn indeterminate_when_a_journal_tmp_is_present() {
        let temp = TempDir::new().unwrap();
        let dir = journals(&temp);
        std::fs::write(dir.join("a.journal"), "").unwrap();
        std::fs::write(dir.join("a.journal.tmp"), "").unwrap();
        assert_eq!(
            journal_evidence(temp.path(), KEY, 0, 99),
            JournalEvidence::Indeterminate
        );
    }

    /// The snapshot lets a reclaimer re-read only changed journals; a change is seen.
    #[test]
    fn refresh_rereads_only_changed_leaves() {
        let temp = TempDir::new().unwrap();
        let dir = journals(&temp);
        std::fs::write(
            dir.join("a.journal"),
            line("other/key", 0, 9, JournalOperation::Add),
        )
        .unwrap();
        std::fs::write(dir.join("b.journal"), "").unwrap();
        let mut reader = JournalEvidenceReader::new(temp.path());
        assert_eq!(reader.refresh(), 2);
        let names: Vec<String> = reader.snapshot().into_iter().map(|s| s.name).collect();
        assert_eq!(
            names,
            vec!["a.journal".to_string(), "b.journal".to_string()]
        );
        assert_eq!(
            reader.evidence(KEY, 0, 99),
            JournalEvidence::ConfirmedAbsent
        );
        assert_eq!(reader.refresh(), 0, "unchanged leaves are not re-read");

        // A peer appends the Add (length changes).
        std::fs::write(
            dir.join("b.journal"),
            line(KEY, 0, 99, JournalOperation::Add),
        )
        .unwrap();
        assert_eq!(reader.refresh(), 1, "only the changed leaf is re-read");
        assert_eq!(reader.evidence(KEY, 0, 99), JournalEvidence::JournalPresent);

        // The journal is consolidated away (deleted): the evidence goes with it.
        std::fs::remove_file(dir.join("b.journal")).unwrap();
        reader.refresh();
        assert_eq!(reader.snapshot().len(), 1);
        assert_eq!(
            reader.evidence(KEY, 0, 99),
            JournalEvidence::ConfirmedAbsent
        );
    }
}
