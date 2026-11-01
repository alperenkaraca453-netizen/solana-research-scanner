# Solana scanner v0.7 — instruction verification

Research only, no trading. Corrects the v0.6 escaped regex bug.

Samples one in every SAMPLE_EVERY (default 250) successful notifications even without a matching log, then decodes Raydium AMM v4 instructions via getTransaction. Discriminator 1 is Initialize2.

`/health`: sampled, decoded, verified_inits, non_init_decoded, init_markers, rpc_errors. `/candidates`: **verified** AMM v4 pool-init instructions only.

Default RPC_MAX_RPM=12, SAMPLE_EVERY=250. Monitoring only Raydium AMM v4; does not include CPMM, CLMM or PumpSwap.
