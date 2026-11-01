# Solana Raydium scanner v0.6

Research-only; no orders, wallet keys or trading.

Fixes: separates failed Solana transactions from filtered successful ones; tracks initialization markers, duplicates and null RPC results; emits a 60-second heartbeat. `/health` exposes all counters; `/candidates` exposes decoded candidates. Zero candidates is not necessarily an error: it may mean no qualifying Raydium AMM v4 initialization occurred.

Upload `main.py` to the GitHub repository root, replacing the existing file. Render automatically deploys from main. Verify `/health` reports `version: v0.6`.
