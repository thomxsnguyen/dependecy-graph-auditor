# Dependency audit reference workload

Submit `dependency_audit` with a public GitHub URL and optional ref. The root
worker checks the repository root for these existing ecosystems only:

| Manifest | Registry path |
| --- | --- |
| `package.json` | npm |
| `pyproject.toml`, `requirements.txt` | PyPI |
| `go.mod` | Go module proxy |

Each discovered coordinate creates an internal, idempotent child job. Children
may discover more children. The root remains `waiting` until no descendants are
active. Package findings, parent-child relationships, policy verdicts, child
counts, attempts, and lifecycle events are persisted and returned by root-job
detail.

This workload demonstrates dynamic work expansion, bounded concurrency,
deduplication, retry classification, persistence, and crash recovery. It is not
a generalized workflow engine and does not perform source-file or runtime-call
analysis.
