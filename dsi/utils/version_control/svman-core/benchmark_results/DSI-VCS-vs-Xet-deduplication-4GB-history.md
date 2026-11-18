# svman vs Xet: deduplication across 19 versions

A **4 GB initial file** is followed by **18 consecutive edited versions**. At the beginning, middle and end, the workload applies 64 KiB and 128 MiB remove/add/update operations. Each tool retains all versions in one local repository, without garbage collection, using one process and three fresh-store trials. The charts reuse the saved November 12, 2026 results.

**Deduplication ratio = (logical history size − current stored size) / logical history size.** Logical history size sums all retained file versions. Stored size includes payload, metadata, caches and version pointers, measured as file lengths rather than allocated disk blocks. GB is decimal.

![Deduplication ratio across all 19 versions](2026-11-12-dsi-xet-dedup-4GB-jlk3bL/file-history-dedup-ratio-4GB.png)

Both tools reuse most of the history. Savings rise as more similar versions are retained; the ratio can start slightly negative because the first store includes metadata.

![Cumulative storage in GB for all 19 versions](2026-11-12-dsi-xet-dedup-4GB-jlk3bL/file-history-stored-size-4GB.png)

This chart shows the **complete retained repository after each version**, rather than the size of that version alone. Points are three-trial medians; the inset magnifies the difference. Retaining history means deleting file content does not remove chunks needed by earlier versions.

After version 19, logical history totals **75.5972 GB**:

| Tool | Stored history (GB) | Storage saved |
| --- | ---: | ---: |
| svman | 5.0337 | 93.3414% |
| Xet | 4.8209 | 93.6229% |

Xet stores **0.2128 GB less**, a **0.2815 percentage-point** advantage in savings. These are whole-repository savings, including native packing and metadata effects.

All **114 historical-version restore checks** passed. Chart versions 1–19 correspond to saved version indices 0–18.

[Full benchmark report](2026-11-12-dsi-xet-dedup-4GB-jlk3bL/README.md) · [All 19 storage values](2026-11-12-dsi-xet-dedup-4GB-jlk3bL/file-history-storage-by-version.tsv) · [Storage chart PDF](2026-11-12-dsi-xet-dedup-4GB-jlk3bL/file-history-stored-size-4GB.pdf).

