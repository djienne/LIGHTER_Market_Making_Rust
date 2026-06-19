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

- Immediately loads the signer and acquires the per-account live instance lock.
- Connects the transaction websocket and private account streams.
- Runs cancel-all and verifies `0 active orders` before enabling the strategy.
- Logs `LIVE mode: order sending ENABLED` once live infrastructure is active.
- Still observes `trading.vol_obi.warmup_seconds` from `config.json` before normal quote placement.

With the current `config.json`, `warmup_seconds` is `600`, so normal live quote orders should not be
placed for roughly the first 10 minutes after the market loop starts. If the account already holds
inventory, the bot may still emit passive reduce-only exit orders during warmup; when flat, it sends
no normal quote orders until warmup completes.

## No-Order Validation Run

To validate market-data connectivity and quote calculation without submitting exchange orders, run
the binary's `--shadow` mode explicitly. Logs show this as `mode=Shadow`.

```bash
docker compose run --rm --no-deps lighter-mm --symbol BTC --config /app/config.json --shadow
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
