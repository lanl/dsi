# Changelog

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
