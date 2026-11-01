# Solana Raydium Pool Scanner v0.9

Research-only. **No wallet, no orders, no live trading.**

- Watches AMM v4 and CPMM program logs independently.
- Prioritizes pool-initialization log markers, verifies the actual on-chain instruction (AMM v4 byte 1; CPMM Anchor `sha256("global:initialize")[:8]`).
- Default `SAMPLE_EVERY=0`: does not waste RPC quota on arbitrary swaps. Set e.g. `SAMPLE_EVERY=1000` for diagnostic sampling.
- RPC rate budget `RPC_MAX_RPM=12` shared across both programs.
- `/health` reports independent connections and counters; `/candidates` contains only instruction-verified creations.
- `token_mints_observed` is a set of mints seen in post-transaction token balances, **not necessarily an exact pool pair**. Confirm account mappings before trading.

Upload the four ZIP files into the root of the existing GitHub repository, replacing `main.py`. Render will deploy the latest commit if auto-deploy is enabled; otherwise deploy manually.

Note: logs-based detection can miss events without recognizable log markers; CPMM and AMM v4 only (not CLMM or LaunchLab). No performance/profit claim.
