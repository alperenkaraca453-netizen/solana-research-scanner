# Solana Raydium Research Scanner v1.0

Research-only, **never sends orders**. Detects instruction-verified Raydium AMM v4 and CPMM pool initialization events.

## Endpoints
- `/health`: counters, connections, process uptime and last-event age
- `/candidates`: verified candidates (in-memory, max 500)
- `/report`: concise machine-readable test results; paper trades and P&L intentionally not fabricated
- `/export.csv`: downloadable verified pool CSV with CPMM token-pair mint accounts

## Limitations
- Render free services may sleep; in-memory candidates and counters reset on restart.
- `/export.csv` is a snapshot, not durable storage.
- Liquidity, risk, and P&L remain null until separately verified market data and simulation are implemented.
- Transaction-wide token balances are **not** assumed to be the trading pair.
- No profit or safety claim is made.

## Deploy
Upload all four files to GitHub repo root, replacing existing files. Render auto-deploys from main. Validate `/health`, `/report`, `/export.csv` after deployment.
