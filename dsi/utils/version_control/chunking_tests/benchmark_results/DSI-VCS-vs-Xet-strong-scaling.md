# DSI-VCS vs Xet: strong scaling

Storing (performing commit for versioning) a fixed **4 GB or 50 GB synthesized file** using **1, 2, 4, 8 or 12 worker processes**, with **three trials per count** using tools **Xet** and **DSI-VCS**. Both tools use identical input bytes, balanced file ranges and a fresh independent local store per worker. The host is an Apple M2 Max with 12 cores and 64 GB RAM. Each worker has one compute thread and two async threads.

Store time includes process startup/shutdown, input reads, chunking, hashing, local storage and manifest publication. Input generation, preflight hashing and full-file restore verification are excluded. GB and MB are decimal.

Each point is a **median**; error bars show **min–max**. Lower store time and higher throughput are better. Speedup is each tool's own one-process median divided by its current median; efficiency is speedup divided by process count, expressed as a percentage.

![4 GB: Xet and DSI-VCS store time, throughput, speedup and efficiency](2026-11-13-dsi-partition-process-4GB-EtB7yv/dsi-vs-xet-partition-4GB.png)

**4 GB:** DSI-VCS is faster at every tested count. Its best median is at **12 processes: 797 ms, 5,017 MB/s, 6.68× speedup**. Xet's best median is at **8 processes: 1,888 ms, 2,118 MB/s, 4.03× speedup**.

![50 GB: Xet and DSI-VCS store time, throughput, speedup and efficiency](2026-11-13-dsi-vcs-partition-process-50GB-6VbwEn/dsi-vcs-vs-xet-partition-50GB.png)

**50 GB:** Both tools achieve their best medians at **4 processes**: DSI-VCS takes **36,639 ms at 1,365 MB/s (2.02×)**; Xet takes **27,821 ms at 1,797 MB/s (3.38×)**. DSI-VCS is faster at 1–2 processes; Xet is faster at 4–12. Increasing beyond four processes does not improve either tool's median.
