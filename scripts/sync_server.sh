#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CFG="$ROOT_DIR/configs/sources.yaml"
ENV_FILE="$ROOT_DIR/.env"

# Load environment variables from .env file if it exists
if [ -f "$ENV_FILE" ]; then
  set -a  # automatically export all variables
  source "$ENV_FILE"
  set +a  # turn off automatic export
  echo "Loaded config from .env file"
else
  echo ".env file not found, falling back to sources.yaml"
  # Fallback to YAML reads (for backwards compatibility)
  SSH_HOST=${SSH_HOST:-$(awk '/host:/{print $2}' "$CFG" | head -n1)}
  SSH_USER=${SSH_USER:-$(awk '/user:/{print $2}' "$CFG" | head -n1)}
  REMOTE_PATH=${REMOTE_PATH:-$(awk '/remote_path:/{print $2}' "$CFG" | head -n1)}
  ICLOUD_BACKUP=${ICLOUD_BACKUP:-$(awk '/icloud_backup:/{print $2}' "$CFG" | head -n1)}
  ICLOUD_DEST=${ICLOUD_DEST:-$(awk -F'"' '/icloud_dest:/{print $2}' "$CFG" | head -n1)}
fi

# Use environment variables
host="$SSH_HOST"
user="$SSH_USER" 
remote_path="$REMOTE_PATH"
icloud_backup="$ICLOUD_BACKUP"
icloud_dest="$ICLOUD_DEST"

raw_dir="$ROOT_DIR/data/raw/cloud-11"
snap_dir="$raw_dir/snapshots"
log_dir="$ROOT_DIR/logs"
mkdir -p "$snap_dir" "$log_dir"

# Prevent overlapping runs
lock="$log_dir/.sync.lock"
if ( set -o noclobber; echo $$ > "$lock" ) 2> /dev/null; then
  trap 'rm -f "$lock"; exit $?' INT TERM EXIT
else
  echo "Another sync is running; exiting."
  exit 0
fi

ts="$(date -u +"%Y-%m-%dT%H-%M-%SZ")"
tmp="$snap_dir/.run_results.${ts}.tmp.csv"
dst="$snap_dir/run_results.${ts}.csv"

echo "Pulling from $user@$host:$remote_path"

# 1) First rsync to a temp file (resumable, read-only, gentle)
#    --bwlimit optional if you want to throttle (uncomment to use)
#    --bwlimit=20000   # ~20 MB/s
rsync -av --human-readable --partial --inplace \
  "${user}@${host}:${remote_path}" "$tmp" \
  > "$log_dir/rsync_${ts}.log" 2>&1

# 2) Verify stability: run rsync again; if it changes size, wait and retry (up to 2 retries)
retries=2
while [ $retries -ge 0 ]; do
  before=$(stat -f%z "$tmp" 2>/dev/null || echo 0)
  sleep 1
  rsync -av --human-readable --partial --inplace \
    "${user}@${host}:${remote_path}" "$tmp" \
    >> "$log_dir/rsync_${ts}.log" 2>&1
  after=$(stat -f%z "$tmp" 2>/dev/null || echo 0)
  if [ "$before" = "$after" ] && [ "$after" -gt 0 ]; then
    break
  fi
  retries=$((retries-1))
done

# 3) Atomic local move
mv "$tmp" "$dst"
echo "Snapshot: $dst"

# 4) Update absolute 'latest' symlink
latest="$raw_dir/latest.csv"
rm -f "$latest"
ln -s "$(cd "$(dirname "$dst")" && pwd)/$(basename "$dst")" "$latest"
echo "Updated latest -> $latest"

# 5) Optional iCloud mirror to your exact folder
if [[ "${icloud_backup:-false}" == "true" && -n "${icloud_dest:-}" ]]; then
  mkdir -p "$icloud_dest"
  cp "$dst" "$icloud_dest/run_results.${ts}.csv"
  cp "$dst" "$icloud_dest/latest.csv"
  echo "iCloud mirror updated: $icloud_dest"
fi

echo "Sync complete."
rm -f "$lock"
trap - INT TERM EXIT