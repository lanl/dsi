# dsi-vcs: Content Version Control with Full Linux Metadata

## Overview

**dsi-vcs** is a lightweight content (Large data file, code) version control system designed for capturing and preserving complete Linux file metadata. Unlike traditional VCS tools, dsi-vcs focuses on:

- **Full metadata capture**: permissions, ownership, ACLs, extended attributes, and SELinux contexts
- **Rolling hash chunking**: Uses a rolling hash for efficient file chunking and deduplication
- **Merkle commit chain**: SHA-256 commit IDs with per-path Merkle nodes for pruning unchanged subtrees
- **Branch support**: Multiple branches with parent-child commit relationships
- **SQLite database**: structured metadata storage for querying and diffing
- **Complete file history**: Chunk-based content addressing with full metadata tracking

---

## CLI Commands

Initialize dsi-vcs in the current directory:

```bash
dsi-vcs init
```

### Stage Files for Commit

**Add files/directories:**

```bash
dsi-vcs add <path> [<path> ...]
dsi-vcs add ./data
dsi-vcs add file1.txt file2.txt
```

**Stage files for deletion:**

```bash
dsi-vcs delete <path> [<path> ...]
dsi-vcs delete ./old_data/
```

**Remove files from staging (without deleting):**

```bash
dsi-vcs remove <path> [<path> ...]
dsi-vcs remove file1.txt
```

### Commit Changes

Create a snapshot with an optional message:

```bash
dsi-vcs commit
dsi-vcs commit "Initial data import"
```

### Branch Management

**Create a new branch:**

```bash
dsi-vcs branch <branch_name> [<start_point>]
dsi-vcs branch feature-branch
dsi-vcs branch hotfix abc123def456
```

**List all branches:**

```bash
dsi-vcs list-branch
```

**Switch to a branch:**

```bash
dsi-vcs switch <branch_name>
dsi-vcs switch main
dsi-vcs switch feature-branch
```

**Merge a branch:**

```bash
dsi-vcs merge <branch_name> [<target_commit>]
dsi-vcs merge feature-branch
dsi-vcs merge feature-branch abc123def456
```

### View History

List all commits:

```bash
dsi-vcs log
dsi-vcs log feature-branch
```

Example output:

```shell
───────────────────────────────────────────────────────────────────────────────────────────────────────────────────
COMMIT HASH                        OWNER            DATE/TIME (UTC)                FILES           BYTES  MESSAGE
───────────────────────────────────────────────────────────────────────────────────────────────────────────────────
f826177ae78f4e48a8c08054e2bb9a71   owner1           2026-04-28 21:10:05.000 UTC      7          15,557  first commit
4af9e3d4dc854d699b96b5a84f913ac0   owner2           2026-04-28 21:19:47.000 UTC      7          15,559  second commit
```

### Compare Versions

Diff two commits (shows added, modified, deleted files).

Parameters:

- Provide upto two commit hash as paramerter.
- No parameter will compare with the latest changes with the last commit.
- One parameter will compare with the latest change with the provided commit version.
- Two parameters will compare between the provided versions.

```bash
dsi-vcs diff <version1> <version2>
dsi-vcs diff f826177ae78f4e48a8c08054e2bb9a71
```

Example output:

```shell
Diff f826177ae78f4e48a8c08054e2bb9a71 → latest  (./root_folder)  
                                                
STATUS     PATH                                 
──────────────────────────────────────────────────────────────────────  
MODIFIED   file_new  [owner]                    
MODIFIED   file_schema.json  [owner]            
diff result: 2c2                                
<    "genesis_datacard": {                      
---                                             
>    2"genesis_datacard": {                     
26c26                                           
< }                                             
\ No newline at end of file                     
---                                             
> }                                             
MODIFIED   schema2.json  [content, size]        
                                                
Summary: +0 added  -0 deleted  ~3 modified  =4 unchanged
```

### Restore a Version

Restore the entire repository to a previous commit:

```Shell
dsi-vcs restore <version>
dsi-vcs restore abc123def456
```



### Clone a DSI-VCS Repository

Clone a previously initialized DSI-VCS repostory to the current working directory:

```Shell
dsi-vcs clone pathe_to_repository
```

## Python API

Integrated within DSI. User versioning with the function `version(command, args)`

### Usage

```python
obj.version(command: str, args: str = None)
```

### Parameters

| Parameter   | Type    | Description                                              |
| ----------- | ------- | -------------------------------------------------------- |
| `command` | `str` | The versioning operation to perform (see commands below) |
| `args`    | `str` | Optional or required arguments depending on the command  |

---

### `init`

Initializes a versioning repository in a root folder.

```python
obj.version("init", "my_project_folder")
```

**Args (required):** Name of the root folder for the versioning repository.

---

### `add`

Adds one or more files to the staging area for the next commit.

```python
obj.version("add", "file1.py file2.py")
```

**Args (required):** Space-separated file paths to stage.

---

### `remove`

Removes one or more files from the staging area **without** deleting the actual files.

```python
obj.version("remove", "file1.py")
```

**Args (required):** Space-separated file paths to unstage.

---

### `delete`

Marks one or more files for deletion in the next commit.

```python
obj.version("delete", "file1.py file2.py")
```

**Args (required):** Space-separated file paths to delete.

---

### `commit`

Commits all staged changes as a new version snapshot.

```python
obj.version("commit", "Initial release")  # with message
obj.version("commit")                      # without message
```

**Args (optional):** A descriptive message for the version being committed.

---

### `branch`

Creates a new branch from a specified commit or current HEAD.

```python
obj.version("branch", "feature-branch")                    # from current HEAD
obj.version("branch", "hotfix abc123def456")              # from specific commit
```

**Args (required):** Branch name, optionally followed by a space and start point commit hash.

---

### `merge`

Merges a branch into the current HEAD.

```python
obj.version("merge", "feature-branch")                     # merge entire branch
obj.version("merge", "feature-branch abc123def456")       # merge up to specific commit
```

**Args (required):** Branch name to merge, optionally followed by a space and target commit hash.

---

### `list-branch`

Lists all branches in the repository.

```python
obj.version("list-branch")
```

**Args:** None.

---

### `switch`

Switches to a different branch.

```python
obj.version("switch", "main")
obj.version("switch", "feature-branch")
```

**Args (required):** Name of the branch to switch to.

---

### `log`

Lists recent committed versions.

```python
obj.version("log")            # shows logs in descending date order
obj.version("log", "feature") # shows logs from feature branch only
```

**Args (optional):** Number of recent versions to display. Defaults to `5`.

---

### `diff`

Shows the differences between two versions.

```python
obj.version("diff")                        # current vs previous
obj.version("diff", "abc123")              # specific commit vs its previous
obj.version("diff", "abc123 def456")       # between two specific commits
```

**Args (optional):** Zero, one, or two commit hashes separated by a space.

| # of hashes | Behavior                                           |
| ----------- | -------------------------------------------------- |
| 0           | Diffs current version against previous             |
| 1           | Diffs specified commit against its previous        |
| 2           | Diffs the two specified commits against each other |

---

### `restore`

Restores the repository to a previously committed version.

```python
obj.version("restore", "abc123def456")
```

**Args (required):** The commit hash of the version to restore.

Example : Basic Workflow

```python
from dsi.utils.version_control import Version

# Initialize dsi
dsi = DSI()

# Initialize repo
dsi.version("init", "/data/archive")

# Stage files
dsi.version("add", "./documents config.json")

# Commit
dsi.version("commit", "Initial archive")

# View history
dsi.version("log")

# Create a new branch
dsi.version("branch", "feature-work")

# Switch to the new branch
dsi.version("switch", "feature-work")

# Modify files and commit again
dsi.version("add", "./documents")
dsi.version("commit", "Updated documents")

# List all branches
dsi.version("list-branch")

# Switch back to main and merge
dsi.version("switch", "main")
dsi.version("merge", "feature-work")

# Compare two versions
dsi.version("diff")
```

---

### `clone`

Restores the repository to a previously committed version.

```python
obj.version("clone", "path_to_repository")
```

## Database Schema

### `versions` Table

Stores metadata for each committed version.

| Column         | Type       | Description                         |
| -------------- | ---------- | ----------------------------------- |
| id             | INTEGER PK | Auto-increment ID                   |
| root_folder    | TEXT       | Repository root path                |
| commit_hash    | TEXT       | SHA-256 Merkle commit hash          |
| root_tree_hash | TEXT       | SHA-256 hash of root tree           |
| hash_algorithm | TEXT       | Merkle format identifier            |
| committed_at   | INTEGER    | UTC timestamp (seconds since epoch) |
| owner_name     | TEXT       | Username of the committer           |
| message        | TEXT       | Optional commit message             |
| file_count     | INTEGER    | Number of files in commit           |
| total_bytes    | INTEGER    | Total size in bytes                 |

**Constraints:**

- `UNIQUE(root_folder, commit_hash)`

### `merkle_nodes` Table

Stores the content-addressed tree node for each committed path, including the synthetic root path `.`.

| Column              | Type       | Description                                        |
| ------------------- | ---------- | -------------------------------------------------- |
| id                  | INTEGER PK | Auto-increment                                     |
| version_id          | INTEGER FK | References`versions(id)` ON DELETE CASCADE       |
| root_folder         | TEXT       | Partition key                                      |
| relative_path       | TEXT       | Path relative to root                              |
| file_type           | TEXT       | file/dir/symlink/etc                               |
| node_hash           | TEXT       | SHA-256 hash for this path node                    |
| metadata_hash       | TEXT       | SHA-256 hash of stable metadata                    |
| content_hash_sha256 | TEXT       | SHA-256 file content hash (NULL for directories)   |
| subtree_file_count  | INTEGER    | File count below this node                         |
| subtree_total_bytes | INTEGER    | Total bytes below this node                        |
| child_count         | INTEGER    | For directories: immediate children; Files: chunks |

**Constraints:**

- `UNIQUE(version_id, relative_path)`

**Indexes:**

- `idx_merkle_nodes_root_path` ON `(root_folder, version_id, relative_path)`
- `idx_merkle_nodes_hash` ON `(root_folder, node_hash)`

---

### `branches` Table

Tracks branch information and their HEAD commits.

| Column           | Type       | Description                                |
| ---------------- | ---------- | ------------------------------------------ |
| id               | INTEGER PK | Auto-increment ID                          |
| root_folder      | TEXT       | Repository root path                       |
| branch_name      | TEXT       | Name of the branch                         |
| head_commit_hash | TEXT       | Commit hash at the HEAD of the branch      |
| is_latest        | INTEGER    | Flag indicating if this is latest (0 or 1) |
| created_at       | INTEGER    | UTC timestamp (seconds since epoch)        |

**Constraints:**

- `UNIQUE(root_folder, branch_name)`

---

### `branch_links` Table

Stores parent-child relationships between commits across branches.

| Column             | Type       | Description                           |
| ------------------ | ---------- | ------------------------------------- |
| id                 | INTEGER PK | Auto-increment ID                     |
| parent_commit_hash | TEXT       | Hash of parent commit (NULL for root) |
| child_commit_hash  | TEXT       | Hash of child commit                  |
| child_branch_name  | TEXT       | Branch name of the child commit       |
| created_at         | INTEGER    | UTC timestamp (seconds since epoch)   |

**Constraints:**

- `UNIQUE(parent_commit_hash, child_commit_hash, child_branch_name)`

---

### `chunk_store` Table

Stores content-addressable chunks for deduplicated file storage.

| Column             | Type       | Description                                     |
| ------------------ | ---------- | ----------------------------------------------- |
| id                 | INTEGER PK | Auto-increment ID                               |
| chunk_hash         | TEXT       | Hash of the chunk content                       |
| chunk_size         | INTEGER    | Size of the chunk in bytes                      |
| created_at         | INTEGER    | UTC timestamp (seconds since epoch)             |
| commit_hash        | TEXT       | Commit hash (NULL until commit is finalized)    |
| relative_file_path | TEXT       | Relative path of the file this chunk belongs to |
| chunk_index        | INTEGER    | Index of this chunk within the file             |

**Constraints:**

- `UNIQUE(chunk_hash, commit_hash)`

**Indexes:**

- `idx_chunk_store_commit_file` ON `(commit_hash, relative_file_path, chunk_index)`

---

## Directory Structure

```
root_folder/
└── .dsi_vcs_snapshots/      # Snapshot and metadata directory
    ├── .dsi_vcs.db          # SQLite metadata database
    ├── .dsi_vcs_chunks/     # Deduplicated chunk storage (content-addressed)
    │   ├── ab235232452.     # Chunk file named by hash
    │   └── de2453453...
    └── repo-123.log	     # Repository log file maintains append only records
```

---

## Features

### Content-Addressable Storage

- Files are chunked using rolling hash algorithm
- Chunks are stored once and referenced by hash
- Deduplication across commits and files

### Merkle Tree Structure

- Each file and directory has a Merkle node
- Changes propagate up the tree
- One Merkle tree per version

### Branch Management

- Support for multiple branches per repository
- Track parent-child relationships between commits
- Branch HEAD pointers maintained automatically

### Metadata Preservation

- Complete Linux file metadata captured
- Permissions, ownership, ACLs
- Extended attributes (xattrs)
- SELinux security contexts
- Symbolic link targets
