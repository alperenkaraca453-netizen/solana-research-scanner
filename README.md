# Solana Scanner v0.8

Fixes Solana RPC error -32015 for transaction version 1 by setting `maxSupportedTransactionVersion` to 1. Adds `version_errors` diagnostics. Set `MAX_SUPPORTED_TX_VERSION` environment variable if future network transactions require a higher version. Research only; live trading disabled.

Upload `main.py` to repository root and deploy latest commit on Render. Check `/health` for version v0.8, decoded, rpc_errors, version_errors, verified_inits.
