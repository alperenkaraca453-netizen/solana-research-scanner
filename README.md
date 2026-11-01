# Solana Research Scanner v1.1 — read-only market measurement

Keeps v1.0 Raydium AMM v4 / CPMM detection and adds `/analytics` with third-party price and liquidity sampling for **instruction-verified** CPMM pools. The scanner never trades.

## Endpoints
- `/health`: status and connections
- `/candidates`: verified pool initialization transactions
- `/analytics`: first observed and latest market price, liquidity in USD when provided, 5/15/60-minute price change, illustrative return with assumed costs
- `/report`: counters and analytics totals
- `/export.csv`: verified pool list (original v1.0 export)

## Important limitations
- DexScreener may not index a newly created pool immediately. A pool can remain `pool_not_indexed`; missing price is **not zero**.
- Price tracking begins when a market price is **first observed**, not at pool creation. 5/15/60-minute checkpoints are elapsed since that observation and can be late due to polling.
- `illustrative_net_pct = observed price change % - PAPER_TOTAL_COST_BPS / 100`. Default 100 bps (1%) round trip is a user-configurable assumption, **not an executable trading PnL**, and ignores actual fill prices, variable slippage, MEV, and taxes.
- Risk/security checks are **not implemented**. The `/analytics` `safety` field reports `not_checked`; it must never be treated as a passing risk score.
- `/tmp/solana_analytics.jsonl` and in-memory data are **ephemeral** on Render Free; they may disappear after sleep/restart/deploy. Add persistent storage before relying on historical results.
- WebSocket 1006 errors and missed messages remain possible on public Solana RPC.
- Only CPMM pool addresses are mapped for market matching. AMM v4 initialization events remain available in `/candidates`, but price tracking may be unsupported.

## Deploy
Upload `main.py`, `analytics.py`, `Dockerfile`, `requirements.txt` and `README.md` to repository root. Render's Dockerfile uses `COPY . .` to include `analytics.py`. Do not upload a nested folder.

Optional env: `PRICE_POLL_SECONDS=60`, `PAPER_TOTAL_COST_BPS=100`, `MAX_TRACKED_POOLS=30`, `ANALYTICS_JSONL=/tmp/solana_analytics.jsonl`.
