#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ENSURE_SCRIPT="$SCRIPT_DIR/ensure_feeds_running.sh"

if [ ! -x "$ENSURE_SCRIPT" ]; then
  chmod +x "$ENSURE_SCRIPT"
fi

START_MARK="# BEGIN_DEEPWATER_FEEDS"
END_MARK="# END_DEEPWATER_FEEDS"

existing="$(crontab -l 2>/dev/null || true)"
filtered="$(printf '%s\n' "$existing" | sed "/$START_MARK/,/$END_MARK/d")"

cron_block=$(cat <<EOF
$START_MARK
@reboot $ENSURE_SCRIPT >> $ROOT_DIR/feeds/logs/cron-supervise.log 2>&1
*/5 * * * * $ENSURE_SCRIPT >> $ROOT_DIR/feeds/logs/cron-supervise.log 2>&1
$END_MARK
EOF
)

printf '%s\n%s\n' "$filtered" "$cron_block" | crontab -
echo "installed cron watchdog for coinbase-dw + bsc-dw"
echo "check: crontab -l | sed -n '/$START_MARK/,/$END_MARK/p'"
