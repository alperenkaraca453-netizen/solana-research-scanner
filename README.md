# Solana event monitor v0.3
Research-only, no wallet, no trading. Streams confirmed transaction logs mentioning Raydium AMM v4 over Solana WebSocket. A transaction event is **not** a verified new token or a profitable signal.

Deploy as Render Docker web service. GET `/health` reports `connected`, `events`, `last_event`, and `last_error`. `SOLANA_WS_URL` can be set to a provider's WebSocket endpoint. Public Solana RPC endpoints may throttle or disconnect; this monitor reconnects with backoff. Render Free can sleep, so it is not suitable for guaranteed 24/7 operation. Logs are ephemeral; no historical performance or profit claims.

Next: parse pool creation instructions, identify token mint addresses, validate liquidity/permissions, persist events to durable database, and measure executable bid/ask prices with fees in paper trading. Live trading remains disabled.
