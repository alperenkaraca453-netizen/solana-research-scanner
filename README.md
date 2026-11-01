# Solana Research Scanner

Research-only Solana token liquidity and volume scanner using DexScreener public endpoints. No trading, no guaranteed profitability.

## Run

```bash
python main.py
```

## Deploy

This is a long-running worker (not a web server). On Render, select a Docker background worker and set `POLL_SECONDS=120`. Persistent storage is required if retaining SQLite observations across deployments. The provided `render.yaml` selects a paid Starter worker; review charges before provisioning.
