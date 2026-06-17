#!/usr/bin/env bash
# Launch the Rust lighter-mm bot LIVE with full stdout+stderr capture.
#
# Single-instance is enforced in-process by a per-(account,api-key) flock; this script also
# refuses to start if a lighter-mm process is already running. We `exec` into the binary so the
# running process is named `lighter-mm` (so `pkill -INT -x lighter-mm` delivers a clean SIGINT
# that runs the shutdown cancel-all+verify). Credentials come from the Python project's .env,
# sourced into the environment because the bot reads creds from env vars (its CWD has no .env).
set -euo pipefail
cd /home/ubuntu/lighter_MM_RUST
if pgrep -x lighter-mm >/dev/null; then
  echo "REFUSING: a lighter-mm process is already running (pid $(pgrep -x lighter-mm | tr '\n' ' '))"
  exit 1
fi
set -a
# shellcheck disable=SC1091
source /home/ubuntu/lighter_MM/.env
set +a
mkdir -p logs
LOG="${1:-logs/live_$(date +%Y%m%d_%H%M%S).log}"
echo "launching lighter-mm live -> $LOG"
exec ./target/release/lighter-mm --symbol BTC --live >"$LOG" 2>&1
