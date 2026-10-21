# EtherLens — Claude Code Operating Rules

Read PROJECT_SPEC.md before doing anything.

## Rules

1. Execute phases strictly in order. A phase is done only when its Acceptance checks pass.
2. Phases 0–7 are local-only on mock data. Never touch GCP or a public chain before Phase 8/9.
3. At every CHECKPOINT: stop, tell the user exactly what to do (copy-pasteable), and wait.
4. Never run anything that can cost money without explicit approval in the current session: creating Dataproc clusters, BigQuery queries/exports (always dry-run first and show the byte estimate). Sepolia transactions use free test ETH but need the user's wallet — ask first.
5. Secrets live only in `.env` (gitignored). Never print or log private keys or API keys. Never commit `.env`, `data/`, or large files in `results/`.
6. Keep code simple and readable — this is a student project that will be explained in a viva. Prefer clear functions over clever abstractions. Every Spark job is a CLI script with `--env local|gcp`.
7. After each phase, append an entry to `docs/PROGRESS.md`: what was built, the exact commands to run it, key numbers produced, and any deviation from this spec with the reason.
8. If a version, flag, path or API in this spec turns out to be wrong in the real environment, fix it pragmatically and record the deviation in `docs/PROGRESS.md`. Do not silently drop a component.
9. Priorities: P0 must ship. P1 should ship. P2 only if everything else is green and the user asks.
10. Modules `etherlens/mock/*`, `etherlens/sketches/*`, `etherlens/chain/*` and `etherlens/stream/poller.py` must NOT import `pyspark` (they run in plain Python environments).
11. Execution model (Windows host + Docker): you run on Windows. Never install Python packages, Java, Spark, Foundry or gcloud on the host. Every project command runs inside the project image: `docker compose run --rm dev <command>`. Long-running services use `docker compose up -d <service>`. Use forward slashes in paths passed to containers.
12. Line endings: every file you write must use LF (`\n`), never CRLF — shell scripts with CRLF fail inside Linux containers. `.gitattributes` enforces this for git.
