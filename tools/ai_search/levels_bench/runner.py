"""Parallel benchmark runner.

Each (model, task) pair is an independent job executed in a pool of persistent worker
processes, so the slow part -- waiting on the LLM portal -- overlaps across tasks and models.
Workers are separate processes (not threads) because the agent stack keeps per-task state in
process globals (see agent.py). A worker is replaced if it crashes, is hard-killed for
exceeding its time budget, or abandoned a timed-out agent thread.
"""
from __future__ import annotations

import multiprocessing as mp
import os
import sys
import time
import traceback
from dataclasses import dataclass, field
from multiprocessing.connection import wait
from typing import Callable, Dict, List, Optional, Sequence

from .config import AI_SEARCH_DIR, CONFIG, snapshot
from .core import BenchmarkTask, TaskResult, failed_result
from .results import append_partial, load_partial, partial_path, save_results


_STARTUP_ALLOWANCE_SEC = 300


@dataclass
class _Job:
    job_id: int
    model: str
    tasks: List[BenchmarkTask]
    share_session: bool

    @property
    def budget_sec(self) -> float:
        """Generous wall-clock budget before the parent hard-kills the worker."""
        total = 0.0
        for t in self.tasks:
            turn_timeout = t.timeout_sec if t.timeout_sec is not None else CONFIG.task_timeout_sec
            n_turns = len(t.turns) + (1 if t.setup_query else 0)
            total += n_turns * turn_timeout + CONFIG.hard_timeout_slack_sec
        return total


@dataclass
class _Worker:
    proc: mp.Process
    conn: object
    job: Optional[_Job] = None
    deadline: float = 0.0
    received: set = field(default_factory=set)


def _worker_main(conn, cfg: dict, ai_search_dir: str, log_path: str) -> None:
    if ai_search_dir not in sys.path:
        sys.path.insert(0, ai_search_dir)
    os.chdir(ai_search_dir)
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    log = open(log_path, "a", buffering=1, encoding="utf-8")
    sys.stdout = sys.stderr = log

    from levels_bench import config as worker_config
    worker_config.configure(**cfg)
    from levels_bench import agent

    while True:
        try:
            msg = conn.recv()
        except EOFError:
            break
        if msg is None:
            break
        job: _Job = msg
        conn.send(("start", job.job_id, None))
        print(f"\n=== job {job.job_id}: {job.model} x {[t.task_id for t in job.tasks]} ===", flush=True)
        try:
            agent.execute_tasks(
                job.tasks, job.model, share_session=job.share_session,
                on_result=lambda r: conn.send(("result", job.job_id, r)),
            )
        except BaseException:
            conn.send(("job_error", job.job_id, traceback.format_exc()))
        recycle = agent.abandoned_threads > 0
        conn.send(("done", job.job_id, recycle))
        if recycle:
            break
    log.close()


def _fmt_secs(s: float) -> str:
    s = int(s)
    return f"{s // 3600}h{s % 3600 // 60:02d}m" if s >= 3600 else f"{s // 60}m{s % 60:02d}s"


class _Progress:
    def __init__(self, total: int, show_timing: bool) -> None:
        self.total = total
        self.done = 0
        self.start = time.time()
        self.show_timing = show_timing
        self.counts: Dict[str, int] = {}

    def __call__(self, r: TaskResult) -> None:
        self.done += 1
        self.counts[r.status] = self.counts.get(r.status, 0) + 1
        elapsed = time.time() - self.start
        eta = elapsed / self.done * (self.total - self.done)
        score = "-" if r.score is None else f"{r.score:.2f}"
        print(
            f"[{self.done:>{len(str(self.total))}}/{self.total}] {r.status:<7} {r.model} | {r.task_id}"
            f"  score={score}  {r.wall_sec:.0f}s   (elapsed {_fmt_secs(elapsed)}, ~{_fmt_secs(eta)} left)",
            flush=True,
        )
        if self.show_timing and r.timing_log:
            from tool_utils import format_timing_summary
            print(format_timing_summary(r.timing_log))

    def summary(self) -> str:
        parts = ", ".join(f"{k}={v}" for k, v in sorted(self.counts.items()))
        return f"Finished {self.done}/{self.total} in {_fmt_secs(time.time() - self.start)} ({parts})"


def _run_in_pool(jobs: List[_Job], n_workers: int, on_result: Callable[[TaskResult], None]) -> None:
    ctx = mp.get_context("spawn")
    cfg = snapshot()
    os.chdir(AI_SEARCH_DIR)
    if str(AI_SEARCH_DIR) not in sys.path:
        sys.path.insert(0, str(AI_SEARCH_DIR))

    pending = list(jobs)
    workers: List[_Worker] = []
    spawned = 0

    def spawn() -> _Worker:
        nonlocal spawned
        parent_conn, child_conn = ctx.Pipe()
        log_path = str(CONFIG.run_dir / "logs" / f"worker_{spawned:02d}.log")
        proc = ctx.Process(target=_worker_main, args=(child_conn, cfg, str(AI_SEARCH_DIR), log_path), daemon=True)
        proc.start()
        child_conn.close()
        spawned += 1
        w = _Worker(proc, parent_conn)
        workers.append(w)
        return w

    def fail_rest(w: _Worker, notes: str) -> None:
        for t in w.job.tasks:
            if t.task_id not in w.received:
                on_result(failed_result(t, w.job.model, notes))

    def retire(w: _Worker, kill: bool = False) -> None:
        if not kill:
            w.proc.join(timeout=5)
        if w.proc.is_alive():
            w.proc.kill()
            w.proc.join(timeout=5)
        w.conn.close()
        workers.remove(w)

    try:
        while pending or any(w.job for w in workers):
            idle = [w for w in workers if w.job is None]
            while pending and (idle or len(workers) < n_workers):
                w = idle.pop() if idle else spawn()
                w.job, w.received = pending.pop(0), set()
                # Until the worker reports "start", also allow for its import time.
                w.deadline = time.time() + w.job.budget_sec + _STARTUP_ALLOWANCE_SEC
                w.conn.send(w.job)

            busy = [w for w in workers if w.job is not None]
            for conn in wait([w.conn for w in busy], timeout=1.0):
                w = next(x for x in busy if x.conn is conn)
                try:
                    kind, job_id, payload = conn.recv()
                except (EOFError, OSError):
                    fail_rest(w, f"worker_error: worker process exited unexpectedly (exit code {w.proc.exitcode})")
                    retire(w, kill=True)
                    continue
                if kind == "start":
                    w.deadline = time.time() + w.job.budget_sec
                elif kind == "result":
                    w.received.add(payload.task_id)
                    on_result(payload)
                elif kind == "job_error":
                    fail_rest(w, f"worker_error: {payload.strip().splitlines()[-1]}")
                    print(payload, file=sys.stderr)
                elif kind == "done":
                    w.job = None
                    if payload:  # recycle: an abandoned agent thread is still running in it
                        retire(w)

            now = time.time()
            for w in [x for x in workers if x.job is not None and now > x.deadline]:
                fail_rest(w, f"timeout: worker exceeded its {w.job.budget_sec:.0f}s budget and was killed")
                retire(w, kill=True)
    finally:
        for w in list(workers):
            try:
                if w.job is None:
                    w.conn.send(None)
            except OSError:
                pass
        for w in list(workers):
            retire(w, kill=w.job is not None)


def run_benchmark(
    tasks: Sequence[BenchmarkTask],
    label: str,
    *,
    models: Optional[List[str]] = None,
    max_workers: Optional[int] = None,
    share_session: bool = False,
    resume: bool = False,
    show_timing: bool = False,
) -> Dict[str, List[TaskResult]]:
    """Run every task against every model in parallel and save `<run_id>_<label>.json`.

    Returns {model: [TaskResult, ...]} in task order.

    max_workers: concurrent worker processes (default CONFIG.max_workers); 0 runs everything
        in this process, sequentially, with live output (handy for debugging).
    share_session: False (default) gives every task a fresh agent session so tasks can run
        concurrently and can't leak state into each other. True reproduces the original
        notebooks' Level 2/3 behavior (all of a model's tasks in one session, in order); only
        models then run in parallel.
    resume: reuse finished results from the newest earlier run of this label and only run the
        (model, task) pairs that are missing or ended in an error.
    """
    models = list(models or CONFIG.models_to_run)
    n_workers = CONFIG.max_workers if max_workers is None else max_workers
    tasks = list(tasks)

    reused: Dict[tuple, TaskResult] = {}
    if resume:
        for r in load_partial(label):
            if r.status != "ERROR":
                reused[(r.model, r.task_id)] = r
        if reused:
            print(f"Resuming: reusing {len(reused)} finished results")

    collected: Dict[tuple, TaskResult] = {}
    for key, r in reused.items():
        if key[0] in models:
            collected[key] = r
            append_partial(r, label)

    todo = {m: [t for t in tasks if (m, t.task_id) not in collected] for m in models}
    if share_session:
        jobs = [(m, ts) for m, ts in todo.items() if ts]
    else:
        # Interleave models so early progress covers every model.
        jobs = [(m, [t]) for t in tasks for m in models if t in todo[m]]
    jobs = [_Job(i, m, ts, share_session) for i, (m, ts) in enumerate(jobs)]
    n_runs = sum(len(j.tasks) for j in jobs)

    where = "in-process" if n_workers == 0 else f"{min(n_workers, len(jobs))} workers"
    print(f"{label}: {len(tasks)} tasks x {len(models)} models = {len(tasks) * len(models)} runs"
          f" ({n_runs} to run{', ' + where if n_runs else ''})")
    progress = _Progress(n_runs, show_timing)

    def on_result(r: TaskResult) -> None:
        collected[(r.model, r.task_id)] = r
        append_partial(r, label)
        progress(r)

    try:
        if n_workers == 0:
            from .agent import execute_tasks
            for job in jobs:
                execute_tasks(job.tasks, job.model, share_session=share_session, live=True, on_result=on_result)
        elif jobs:
            _run_in_pool(jobs, n_workers, on_result)
    except KeyboardInterrupt:
        print(f"\nInterrupted. Finished results are in {partial_path(label)}; "
              f"re-run with resume=True to continue.")
        raise
    print(progress.summary())

    by_model = {
        m: [collected[(m, t.task_id)] for t in tasks if (m, t.task_id) in collected]
        for m in models
    }
    save_results([r for rs in by_model.values() for r in rs], label)
    return by_model


def debug_task(task: BenchmarkTask, model: Optional[str] = None) -> TaskResult:
    """Run and score one task in this process with live output, then print its scores."""
    import json

    from .agent import execute_tasks, format_dsi_call_log

    model = model or CONFIG.llm_model
    tr = execute_tasks([task], model, live=True)[0]
    print(f"Model: {tr.model} | {tr.status} | score: {tr.score}")
    print(f"Tools: {[c['name'] for c in tr.tool_calls]}")
    print(f"DSI API calls:\n{format_dsi_call_log(tr.dsi_calls)}")
    if tr.notes:
        print(f"Notes: {tr.notes}")
    if tr.judge_scores:
        print(json.dumps(tr.judge_scores, indent=2, default=str))
    return tr
