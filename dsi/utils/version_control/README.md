# dsi-vcs: rsync-based File Version Control with Full Linux Metadata

## Overview

**dsi-vcs** is a lightweight file version control system designed for capturing and preserving complete Linux file metadata. Unlike traditional VCS tools, dsi-vcs focuses on:

- **Full metadata capture**: permissions, ownership, ACLs, extended attributes, and SELinux contexts
- **Rsync-based snapshots**: efficient storage with hard-link deduplication
- **SQLite database**: structured metadata storage for querying and diffing
- **Complete file history**: MD5 hashes, file stats, and all metadata changes tracked

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

### View History

List all commits:

```bash
dsi-vcs log
```

Example output:

```shell
COMMIT HASH                        OWNER            DATE/TIME (UTC)                FILES           BYTES  MESSAGE
────────────────────────────────────────────────────────────────────────────────────────────────────
f826177ae78f4e48a8c08054e2bb9a71   owner1           2026-04-28T21:10:05.339321+00:00      7          15,557  first commit
4af9e3d4dc854d699b96b5a84f913ac0   owner2           2026-04-28T21:19:47.167081+00:00      7          15,559  second commit
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
Diff f826177ae78f4e48a8c08054e2bb9a71 → None  (./root_folder)  
                                                          
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

```bash
dsi-vcs restore <version>
dsi-vcs restore abc123def456
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

### `log`

Lists recent committed versions.

```python
obj.version("log")       # shows last 5 versions (default)
obj.version("log", "10") # shows last 10 versions
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
dsi.version("add", ["./documents", "config.json"])

# Commit
dsi.version("commit", "Initial archive")

# View history
dsi.version("log")

# Modify files and commit again
dsi.version(["./documents"])
dsi.version("commit", "Updated documents")

# Compare two versions
dsi.version("diff")
```

## Database Schema

### `versions` Table

| Column        | Type       | Description                 |
| ------------- | ---------- | --------------------------- |
| id            | INTEGER PK | Auto-increment ID           |
| root_folder   | TEXT       | Repository root path        |
| commit_hash   | TEXT       | UUID4 hex (32 chars)        |
| committed_at  | TEXT       | ISO-8601 timestamp          |
| owner_name    | TEXT       | Linux user who committed    |
| message       | TEXT       | Optional commit message     |
| snapshot_path | TEXT       | Path to rsync snapshot copy |
| file_count    | INTEGER    | Number of files in commit   |
| total_bytes   | INTEGER    | Total size in bytes         |

### `file_entries` Table

Stores metadata for each file in each commit.

| Column                 | Type       | Description                      |
| ---------------------- | ---------- | -------------------------------- |
| id                     | INTEGER PK | Auto-increment                   |
| version_id             | INTEGER FK | References versions(id)          |
| root_folder            | TEXT       | Partition key                    |
| relative_path          | TEXT       | Path relative to root            |
| absolute_path          | TEXT       | Full path                        |
| file_name              | TEXT       | Filename only                    |
| file_type              | TEXT       | file/dir/symlink/etc             |
| md5_hash               | TEXT       | Content hash (files only)        |
| lstat                  | TEXT       | JSON of os.lstat() result        |
| permissions_octal      | TEXT       | e.g., "0o755"                    |
| permissions_str        | TEXT       | e.g., "rwxr-xr-x"                |
| owner_name             | TEXT       | Username                         |
| group_name             | TEXT       | Group name                       |
| setuid, setgid, sticky | INTEGER    | Special bits (0/1)               |
| acl_text               | TEXT       | Raw getfacl output               |
| xattrs                 | TEXT       | JSON dict of extended attributes |
| security_context       | TEXT       | SELinux context                  |
| symlink_target         | TEXT       | Target of symlink                |

### `staging` Table

Temporary storage for files to be committed.

| Column        | Type       | Description             |
| ------------- | ---------- | ----------------------- |
| id            | INTEGER PK | Auto-increment          |
| root_folder   | TEXT       | Partition key           |
| absolute_path | TEXT       | Full file path (UNIQUE) |
| action        | TEXT       | "add" or "delete"       |
| added_at      | TEXT       | ISO-8601 timestamp      |

---

## Directory Structure

```
root_folder/
└── .dsi_vcs_snapshots/      # rsync snapshot directory
    ├── .dsi_vcs.db          # SQLite metadata database
    ├── abc123def456/        # Snapshot for commit abc123def456
    ├── xyz789abc123/        # Snapshot for commit xyz789abc123
    └── ...
```
