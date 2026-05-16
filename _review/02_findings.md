# Phase 2 — Findings

Findings are appended by parallel reviewer agents, then merged here by the orchestrator. Each finding follows the template from the review prompt.

Per-group raw outputs:
- `_review/02_findings_G1.md` — core data plane
- `_review/02_findings_G2.md` — ops engine
- `_review/02_findings_G3.md` — indexes
- `_review/02_findings_G4.md` — recovery + redo
- `_review/02_findings_G5.md` — wire protocol + dispatch
- `_review/02_findings_G6.md` — HTTP server + observability + metrics
- `_review/02_findings_G7.md` — replication
- `_review/02_findings_G8.md` — cluster control plane
- `_review/02_findings_G9.md` — storage tiers
- `_review/02_findings_G10.md` — binaries + config + lib root

After all per-group files are present, the orchestrator merges them into the consolidated `REVIEW_REPORT.md` and renumbers findings F-001 onward in severity order.
