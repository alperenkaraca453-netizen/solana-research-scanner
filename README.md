# Solana Raydium AMM v4 Research Monitor v0.4

**Research only. No wallet, no keys, no real orders, no profitability claim.**

WebSocket `logsSubscribe` watches Raydium AMM v4 and queues only transactions whose logs contain `initialize2`/`initialize_2`. A separately rate-limited RPC worker fetches candidate transactions and records observed mint addresses and verification status. This is **not** a complete or validated new-pool decoder: Raydium program instructions are often unparsed, and mints observed in a transaction do not establish the new pool pair. The monitor deliberately labels such events *unverified*.

## Routes
- `/health` and `/`: connection, events, candidates, decoded, RPC errors, queue depth.
- `/candidates`: last 30 decoded candidates, newest first.

## Environment
- `SOLANA_PROGRAM_ID`: `675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8`
- `SOLANA_WS_URL`: `wss://api.mainnet-beta.solana.com/`
- `SOLANA_HTTP_URL`: `https://api.mainnet-beta.solana.com`
- `RPC_MAX_RPM`: `12` (maximum 60, rate-limited; public RPC may still respond 429)
- `PORT`: Render-provided port

## Deployment
Replace `main.py`, `Dockerfile`, `requirements.txt`, `README.md` in repository root, commit to `main`; Render will auto-deploy or use Deploy latest commit if webhook does not fire. The existing Render env var `SOLANA_PROGRAM_ID` can stay.

## Limitations and next stage
The filter can miss pool creations if log wording differs. Candidates may be false positives. To reliably identify newly created pools, decode Raydium initialize2 instruction accounts using a verified IDL/layout and validate vaults/mints/liquidity against on-chain account data. Then measure latency and evaluate paper-trading strategies with slippage and fees. Do not enable live trading.
