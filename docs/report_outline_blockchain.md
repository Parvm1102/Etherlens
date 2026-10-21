# Report outline — Blockchain Technologies

Section headings, what each must say, and which evidence to use. Real-data numbers come from
`results_gcp/` (January 2025), local-chain numbers from `results/chain/`.

## 1. Introduction
- Problem: analytics about a blockchain are computed off-chain, so readers must trust the
  publisher. Goal: make every published number independently checkable and impossible to change
  later.
- Contribution: `ReportRegistry` contract storing one Merkle root per day; Python and Solidity
  implementations of the same Merkle rules; deployed on Sepolia with real January 2025 reports.

## 2. Background
- Hash functions; Keccak-256 vs NIST SHA-3 (padding byte 0x01 vs 0x06).
- Merkle trees and inclusion proofs; link to Ethereum's Merkle Patricia tries (state, receipts).
- Ethereum accounts (EOA vs contract), nonce, gas, gwei/wei, 21,000-gas transfers.
- Smart contracts, EVM, Solidity lifecycle (write, compile, test, deploy, call).
- The oracle problem.

## 3. Design
- Daily report format: sorted text rows `day|kind|key|value` (summary, top_sender, whale,
  flagged_active); why deterministic (ties broken by address).
- Merkle rules (from `src/etherlens/chain/merkle.py`): leaf = keccak(utf8(row)); parent =
  keccak(min ‖ max); odd node promoted; proof = sibling hashes bottom-up.
- Contract (`contracts/src/ReportRegistry.sol`): `Owned` base (inheritance), `publishRoot`
  (owner-only, `AlreadyPublished`, `EmptyRoot`), `verifyRow` / `verifyLeaf` (view), events.
- Trust model: integrity guaranteed, correctness reduced to "anyone can rebuild from public data".
- Why only the root is stored (cost), why a testnet (free, public explorer).

## 4. Implementation
- Python tools: `merkle.py` (build trees, proofs, CLI), `deploy.py`, `publish.py`
  (incl. `--gas-experiment`), `verify.py` (`--tamper`); web3.py v7.
- Foundry tests (9 pass): golden vector, 3- and 5-leaf proofs, double publish reverts,
  non-owner reverts, empty root reverts, unknown day reverts, ownership transfer.
  Gas snapshot: `results/chain/gas_snapshot.txt`.
- Python ↔ Solidity consistency: golden vector `["a","b","c"]` → root `0x5842…46a7`
  (`tests/test_merkle.py`), and every proof verified on a local chain (`tests/test_chain_local.py`).
- Safety: the tools refuse to run with `CHAIN=sepolia` unless the RPC reports chain id 11155111.

## 5. Deployment and results
- Sepolia contract `0x3d6707d3Ca4d5b7F2A39B0FD6b3B68e561E1964A`; links in
  `results_gcp/chain/sepolia_links.md` (screenshot the Etherscan pages for the report).
- Published days: 2025-01-29 (2,886 rows, 573,630 gas, first publish), 2025-01-30 (2,942 rows,
  377,828 gas), 2025-01-31 (3,029 rows, 377,840 gas).
- Verification: `2025-01-31|summary|tx_count|1172912` (index 2958) → true; one digit changed
  → false. Same via the Streamlit Blockchain tab and the in-browser verifier on the overview page.
- Gas vs report size (local chain, `gas_vs_rows.png`): 10 rows 139,542 (first publish, fresh
  storage), 1,000 rows 105,428, 100,000 rows 105,464, 1,000,000 rows 105,476. Explain the
  first-publish effect and the occasional extra slot for the `_days` array.
- Proof size: about 12 hashes for 3,000 rows (log₂ n), so verifying stays cheap as reports grow.
- Total cost: 0.0000063 SepoliaETH from a free faucet; no real money.

## 6. Connection to the analytics
- Which numbers are anchored: daily summary (tx count, active senders, gas), top 50 senders,
  top 20 whales, flagged addresses active that day (from KMeans).
- How a reader checks a figure in the BDA report against the chain (walk through one row).

## 7. Discussion
- What a malicious publisher could still do (publish wrong numbers once) and how public source
  data limits it.
- Alternatives: store full reports on IPFS and the root on-chain; a decentralised oracle network
  with several publishers.
- Limits: owner key is a single point of trust; Sepolia is a test network.

## 8. Conclusion

## Appendix
- Contract source, test list, commands (`docs/demo_script.md`, Demo B).
