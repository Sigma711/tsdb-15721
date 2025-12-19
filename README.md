# TSDB 12-week labs

Rust workspace for the TSDB lab series. The repo is structured as:

- `crates/`: reusable engine components.
- `labs/`: weekly lab packages with binaries, tests, and benches.
- `tools/`: helpers for data generation, inspection, and profiling.
- `docs/`: syllabus, notes, and paper summaries.
- `data/`: local data (ignored from git).
- `scripts/`: simple bench/report helpers.

Each lab is a standalone Cargo package inside the workspace. Start with `cargo build` to ensure the toolchain works, then jump into `labs/lab01_chunk`.
