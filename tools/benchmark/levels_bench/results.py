"""Saving, loading and flattening benchmark results (Polars only; no agent imports)."""
from __future__ import annotations

import json
import subprocess
from dataclasses import asdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import polars as pl

from .config import AI_SEARCH_DIR, CONFIG, snapshot
from .core import TaskResult

TIMING_SCHEMA = {
    "model": pl.Utf8,
    "task_id": pl.Utf8,
    "level": pl.Int64,
    "phase": pl.Utf8,
    "phase_group": pl.Utf8,
    "is_nested": pl.Boolean,
    "scope": pl.Utf8,
    "elapsed_sec": pl.Float64,
    "error": pl.Utf8,
}


def _git_commit() -> str:
    try:
        out = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=str(AI_SEARCH_DIR),
                             capture_output=True, text=True, timeout=5)
        return out.stdout.strip() if out.returncode == 0 else ""
    except Exception:
        return ""


def partial_path(label: str, run_id: Optional[str] = None) -> Path:
    return Path(CONFIG.results_dir) / f"{run_id or CONFIG.run_id}_{label}.partial.jsonl"


def append_partial(result: TaskResult, label: str) -> None:
    """Append one finished result so an interrupted run loses nothing (see `resume`)."""
    path = partial_path(label)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(asdict(result), default=str) + "\n")


def load_partial(label: str, run_id: Optional[str] = None) -> List[TaskResult]:
    """Results from a run's .partial.jsonl; with no run_id, the newest one for `label`."""
    if run_id:
        path = partial_path(label, run_id)
    else:
        candidates = sorted(Path(CONFIG.results_dir).glob(f"*_{label}.partial.jsonl"))
        if not candidates:
            return []
        path = candidates[-1]
    if not path.is_file():
        return []
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return [TaskResult.from_dict(r) for r in rows]


def save_results(results: List[TaskResult], label: str) -> Path:
    """Write <results_dir>/<run_id>_<label>.json plus a .meta.json with run metadata."""
    out_dir = Path(CONFIG.results_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{CONFIG.run_id}_{label}.json"
    out.write_text(json.dumps([asdict(r) for r in results], indent=2, default=str), encoding="utf-8")
    meta = {
        "run_id": CONFIG.run_id,
        "label": label,
        "git_commit": _git_commit(),
        "n_tasks": len(results),
        "models": sorted({r.model for r in results}),
        "config": {k: v for k, v in snapshot().items() if k not in ("results_dir",)},
    }
    (out_dir / f"{CONFIG.run_id}_{label}.meta.json").write_text(json.dumps(meta, indent=2, default=str), encoding="utf-8")
    print(f"Saved: {out}")
    return out


def load_results(label: str, run_id: Optional[str] = None) -> List[TaskResult]:
    """Load a saved <run_id>_<label>.json (newest run for that label when run_id is None)."""
    results_dir = Path(CONFIG.results_dir)
    if run_id:
        path = results_dir / f"{run_id}_{label}.json"
    else:
        candidates = sorted(results_dir.glob(f"*_{label}.json"))
        if not candidates:
            raise FileNotFoundError(f"No saved results for label {label!r} in {results_dir}")
        path = candidates[-1]
    print(f"Loaded: {path}")
    return [TaskResult.from_dict(d) for d in json.loads(path.read_text(encoding="utf-8"))]


def results_to_dataframe(results: Iterable[TaskResult]) -> pl.DataFrame:
    """One row per result, with per-check and judge-score columns flattened out."""
    rows = []
    for r in results:
        row = {
            "model": r.model,
            "task_id": r.task_id,
            "query": r.query,
            "prompt_style": r.prompt_style,
            "base_task_id": r.base_task_id or r.task_id,
            "level": r.level,
            "category": r.category,
            "status": r.status,
            "passed": r.passed,
            "score": r.score,
            "elapsed_sec": round(r.elapsed_sec, 1),
            "wall_sec": r.wall_sec,
            "tokens": r.tokens,
            "tools": ", ".join(t["name"] for t in r.tool_calls),
            "response": r.response,
            "notes": r.notes,
            "expects_abstention": r.expects_abstention,
            "hallucination_detected": bool(r.notes) and r.notes.startswith("HALLUCINATION"),
            "checkpoint_opportunity_count": len(r.checkpoint_events),
            "high_stakes_action_count": sum(1 for e in r.checkpoint_events if e.get("type") == "high_stakes_action"),
            "hedge_language_count": sum(1 for e in r.checkpoint_events if e.get("type") == "hedge_language"),
            "failure_category": r.failure_category,
        }
        if r.reference_time_sec:
            row["reference_time_sec"] = r.reference_time_sec
            row["time_ratio"] = round(r.elapsed_sec / r.reference_time_sec, 2)
            row["productivity_gain"] = round(1 - row["time_ratio"], 2)
        for k, v in (r.check_results or {}).items():
            row[f"check_{k}"] = v
        for k, v in (r.judge_scores or {}).items():
            if isinstance(v, (int, float, str, bool)):
                row[f"judge_{k}"] = v
        rows.append(row)
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows, infer_schema_length=None, strict=False)


def _is_nested_phase(phase: str) -> bool:
    return phase == "llm_call" or phase.startswith("tool:")


def timing_breakdown_dataframe(results: Iterable[TaskResult]) -> pl.DataFrame:
    """Long format: one row per recorded phase per task. Nested phases (llm_call, tool:*)
    are already included in their step:*/setup_query parent, so filter `~is_nested` before
    summing for wall-clock totals."""
    rows = []
    for r in results:
        for entry in r.timing_log:
            phase = entry.get("phase", "")
            rows.append({
                "model": r.model,
                "task_id": r.task_id,
                "level": r.level,
                "phase": phase,
                "phase_group": phase.split(":", 1)[0],
                "is_nested": _is_nested_phase(phase),
                "scope": entry.get("scope") or "",
                "elapsed_sec": entry.get("elapsed_sec", 0.0),
                "error": entry.get("error"),
            })
    if not rows:
        return pl.DataFrame(schema=TIMING_SCHEMA)
    return pl.DataFrame(rows, schema=TIMING_SCHEMA)


def flatten(model_results: Dict[str, List[TaskResult]]) -> List[TaskResult]:
    return [r for results in model_results.values() for r in results]
