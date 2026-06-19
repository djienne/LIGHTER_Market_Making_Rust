# Docker Operations

This project has two Docker run modes with different safety behavior.

## No-Order Warmup Run

The default compose service is a no-order warmup and validation run. It connects to live market data
and computes strategy quotes, but it never submits exchange orders. The current binary implements
this with the `--shadow` flag and logs it as `mode=Shadow`; operator docs refer to it as the
no-order warmup run.

```bash
mkdir -p logs
docker compose build
docker compose up
```

Use this mode to validate Docker packaging, market-data connectivity, and quote calculation without
placing orders.

## Live Trading Run

Live trading is explicit. It uses real credentials and can place real orders after startup checks and
warmup complete.

```bash
docker compose --env-file /path/to/.env run --rm --name lighter-mm-live lighter-mm \
  --symbol BTC --config /app/config.json --live
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

## Stop And Verify

Stop the foreground live process with `Ctrl-C`, or stop a named container with:

```bash
docker stop --signal SIGINT lighter-mm-live
```

For live runs, confirm the logs contain clean shutdown evidence:

- `shutdown signal received`
- `cancel-all OK`
- `verified 0 active orders`
- `PNL_SUMMARY`

Useful checks while the container is running:

```bash
docker inspect -f 'status={{.State.Status}} restart={{.RestartCount}} oom={{.State.OOMKilled}}' lighter-mm-live
docker stats --no-stream lighter-mm-live
docker logs --since 5m lighter-mm-live
```
