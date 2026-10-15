# Changelog

## [1.0.1] - 2026-10-15

### Performance

- Reuse unchanged metadata, content hashes, chunk references, and Merkle nodes
  during consecutive commits, including when staging an entire directory.
- Resolve user/group identity once per commit and cache owner/group name lookups.
- Persist the committed repository-log checkpoint to replay only pending staging
  records after restarting the CLI.
- Finalize new chunk references with one transaction-scoped bulk update and copy
  unchanged references within SQLite.
- Batch staging-log writes with one fsync and stream general FastCDC chunks
  without changing their boundaries.
- Local benchmarks reduced median consecutive commit time from 9.191 s to
  0.187 s for one changed file among 500 files, and from 2.215 s to 0.686 s for
  the binary workload. See [benchmark details](plots/consecutive-commit-improvements.md)
  for workloads, methodology, and limitations.

### Added

- `dsi-vcs commit --rehash` to refresh metadata and content for every staged path
  instead of reusing its stat fingerprint.

### Fixed

- Include required ancestor nodes for nested files and track empty directories
  so subsequent commits retain their membership.
- Preserve the committed parent state of unstaged edits and locally missing
  tracked files; stage deletions explicitly with `delete`.
- Abort denied writes or source changes during chunking without losing staging.
- Preserve pending records when a log commit marker is written but its SQLite
  transaction fails.
- Advance branch heads after commits and resolve staged paths relative to the
  repository, including when using the Python API from another directory.


## [1.0.0] - 2026-08-27

### Initial release

- Repository Log.
- Merkle Tree per version.
- File Chunking Hash for deduplication.
  - FastCDC chunking in general. 
  - File format specifc chunking for `npy`, `csv`, `db`, `sqlite`, `sqlitle3`, `json`, `xml`, and `xlsx`.
- Basic versioning commands: .
  -  init: initialize a versioning repository in a root folder
  -  add : add file(s) to the staging area for the next commit
  -  remove : remove file(s) from the staging area without touching the actual files
  -  delete : delete file(s) from the staging area for the next commit
  -  commit : commit a new version with the staged file(s) and an optional message describing the version
  -  branch : create a new branch with an optional starting point (commit hash)
  -  merge : merge a branch into the current branch with an optional target commit hash
  -  list-branch : list all branches in the versioning repository
  -  switch : switch to a different branch in the versioning repository
  -  log : list versions
  -  diff: diff between two versions. If no version is provided, diff the current version with the previous version
  -  restore: restore a version with commit hash
  -  clone  : clone a remote versioning repository
  -  status : Show current branch, commit, and staged files
- For details, see [Readme.md](README.md)
