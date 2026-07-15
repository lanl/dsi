#!/usr/bin/env bash
set -euo pipefail

REPO_SSH="ssh://git@lisdi-git.lanl.gov:10022/dsi/gan_data_sources.git"
BRANCH="main"
CSV_PATH="gan_data_path.csv"

git archive --remote="$REPO_SSH" "$BRANCH" "$CSV_PATH" | tar -x -C "."