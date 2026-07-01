# Docker Operations

This project has two Docker run modes with different safety behavior. The default compose service is
LIVE trading.

## Live Trading Run

`docker compose up -d` starts real trading. It uses credentials from
`/home/ubuntu/lighter_MM/.env` by default and can place real orders after startup checks and warmup
complete.

```bash
mkdir -p logs
docker compose build
docker compose up -d
```

To use a different credentials file:

```bash
LIGHTER_ENV_FILE=/path/to/.env docker compose up -d
```

Live startup behavior:

- Refuses to start on a missing or malformed `config.json` (live never falls back to defaults)
  and validates leverage / levels / sizing values.
- Immediately loads the signer and acquires the per-account live instance lock.
- Sends an update-leverage transaction so the venue's margin model matches
  `trading.leverage` / `trading.margin_mode` (disable with `trading.set_leverage_on_startup=false`
  if the account is already configured and the tx is rejected).
- Connects the transaction websocket and private account streams (`account_all`, `user_stats`,
  and `account_orders` for instant exchange-id resolution).
- Runs cancel-all and verifies `0 active orders` before enabling the strategy.
- Logs `LIVE mode: order sending ENABLED` once live infrastructure is active.
- Still observes `trading.vol_obi.warmup_seconds` from `config.json` before normal quote placement.

While running, a dead-man switch cancels all resting orders and pauses trading if the market-data
feed goes stale for more than `safety.md_deadman_sec` (default 10s); trading resumes automatically
once the feed and reconcile are healthy again.

With the current `config.json`, `warmup_seconds` is `600`, so normal live quote orders should not be
placed for roughly the first 10 minutes after the market loop starts. If the account already holds
inventory, the bot may still emit passive reduce-only exit orders during warmup; when flat, it sends
no normal quote orders until warmup completes.

## No-Order Validation Run

To validate market-data connectivity and quote calculation without submitting exchange orders, run
the binary's `--dry-run` mode explicitly. Logs show this as `mode=DryRun`.

```bash
docker compose run --rm --no-deps lighter-mm --symbol BTC --config /app/config.json --dry-run
```

## Stop And Verify

Stop the compose-managed live container with:

```bash
docker compose stop lighter-mm
```

For live runs, confirm the logs contain clean shutdown evidence:

- `shutdown signal received`
- `cancel-all OK`
- `verified 0 active orders`
- `PNL_SUMMARY`

Useful checks while the container is running:

```bash
docker inspect -f 'status={{.State.Status}} restart={{.RestartCount}} oom={{.State.OOMKilled}}' lighter-mm
docker stats --no-stream lighter-mm
docker logs --since 5m lighter-mm
```

## Latest Validation

On 2026-06-19, a Docker BTC live run completed with the expected 600-second warmup followed by live
order placement. The run sent orders after warmup, recorded fills, emitted PnL health/fill/summary
events, and shut down cleanly with `cancel-all OK` plus `verified 0 active orders`.
