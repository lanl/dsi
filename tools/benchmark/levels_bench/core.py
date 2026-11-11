"""Benchmark task / result data types.

Kept free of agent/LLM imports so task catalogs and saved results can be loaded
(e.g. in the summary notebook) without importing LangChain or DSI.
"""
from __future__ import annotations

from dataclasses import dataclass, field, fields, replace
from typing import Any, Dict, List, Optional


@dataclass
class BenchmarkTask:
    """Definition of a single benchmark task (query, rubric, and scoring hints)."""
    task_id: str
    level: int
    category: str
    query: str
    # Level 1 checks
    expected_tools: List[str] = field(default_factory=list)
    expected_args: Dict[str, Any] = field(default_factory=dict)
    expected_path: Optional[str] = None  # canonicalized and compared to the tool arg "path"
    expected_count: Optional[int] = None  # must appear in a tool result or the response
    response_keywords: List[str] = field(default_factory=list)
    forbidden_keywords: List[str] = field(default_factory=list)
    reference_path: List[str] = field(default_factory=list)  # most-efficient human DSI CLI commands, for LLM-judged efficiency
    reference_time_sec: Optional[float] = None  # human wall-clock seconds for reference_path (RQ1); must be measured, not estimated
    setup_query: Optional[str] = None
    timeout_sec: Optional[float] = None  # per-turn override; falls back to config.task_timeout_sec
    steps: List[str] = field(default_factory=list)  # ordered turns sent to the SAME explorer; empty = single-turn `query`
    # Level 2/3 rubric
    success_criteria: str = ""  # free-text rubric for score_level2's process-quality judge
    ground_truth_hypothesis: Optional[Dict[str, str]] = None  # {"context", "variables", "relationship"} for score_level3
    # Calibration (RQ4): abstaining / asking for clarification IS the pass condition
    expects_abstention: bool = False
    abstention_keywords: List[str] = field(default_factory=list)  # falls back to DEFAULT_ABSTENTION_PHRASES when empty
    requires_network: bool = False
    skip: bool = False
    # Prompt robustness: style -> str (replaces `query`) or List[str] (replaces `steps`)
    prompt_variants: Dict[str, Any] = field(default_factory=dict)
    prompt_style: str = "standard"
    base_task_id: str = ""

    @property
    def turns(self) -> List[str]:
        return list(self.steps) if self.steps else [self.query]


@dataclass
class TaskResult:
    """Outcome of running and scoring one benchmark task."""
    task_id: str
    level: int
    category: str
    query: str
    response: str
    tool_calls: List[Dict[str, Any]]
    elapsed_sec: float
    tokens: int
    passed: Optional[bool] = None
    score: Optional[float] = None
    judge_scores: Optional[Dict[str, Any]] = None
    notes: str = ""
    tool_results: List[str] = field(default_factory=list)
    dsi_calls: List[Dict[str, Any]] = field(default_factory=list)
    timing_log: List[Dict[str, Any]] = field(default_factory=list)
    model: str = ""
    checkpoint_events: List[Dict[str, Any]] = field(default_factory=list)  # RQ3/RQ7
    failure_category: Optional[str] = None  # RQ5
    failure_rationale: str = ""
    check_results: Optional[Dict[str, bool]] = None
    # Copied from the task so saved results are self-describing without the task catalog.
    prompt_style: str = "standard"
    base_task_id: str = ""
    expects_abstention: bool = False
    reference_time_sec: Optional[float] = None
    transcript: str = ""  # markdown log of the run (query, per-step replies, tools, DSI calls)
    wall_sec: float = 0.0  # total wall-clock for run + scoring, as measured by the worker

    @property
    def status(self) -> str:
        if self.notes == "skipped":
            return "SKIP"
        if self.notes.startswith("timeout:"):
            return "TIMEOUT"
        if self.notes.startswith(("error:", "model_error:", "worker_error:")):
            return "ERROR"
        return "PASS" if self.passed else "FAIL"

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TaskResult":
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in d.items() if k in known})


def new_result(task: BenchmarkTask, model: str = "", **overrides: Any) -> TaskResult:
    """Empty TaskResult pre-filled with the task's identifying fields."""
    base = dict(
        task_id=task.task_id, level=task.level, category=task.category, query=task.query,
        response="", tool_calls=[], elapsed_sec=0.0, tokens=0, model=model,
        prompt_style=task.prompt_style, base_task_id=task.base_task_id or task.task_id,
        expects_abstention=task.expects_abstention, reference_time_sec=task.reference_time_sec,
    )
    base.update(overrides)
    return TaskResult(**base)


def failed_result(task: BenchmarkTask, model: str, notes: str, elapsed_sec: float = 0.0) -> TaskResult:
    return new_result(task, model, passed=False, score=0.0, notes=notes, elapsed_sec=elapsed_sec)


def expand_prompt_variants(tasks: List[BenchmarkTask]) -> List[BenchmarkTask]:
    """Expand each task's `prompt_variants` into separate copies, one per style plus
    "standard" (the original wording), so the same ground truth is tested against several
    phrasings. Each copy gets task_id `<id>__<style>` (unchanged for "standard"),
    `prompt_style`, and `base_task_id` for grouping variants back together.
    """
    expanded: List[BenchmarkTask] = []
    for t in tasks:
        standard = list(t.steps) if t.steps else t.query
        styles: Dict[str, Any] = {"standard": standard, **t.prompt_variants}
        for style, variant in styles.items():
            new_id = t.task_id if style == "standard" else f"{t.task_id}__{style}"
            if isinstance(variant, list):
                expanded.append(replace(
                    t, task_id=new_id, steps=variant, query=(variant[-1] if variant else t.query),
                    prompt_variants={}, prompt_style=style, base_task_id=t.task_id,
                ))
            else:
                expanded.append(replace(
                    t, task_id=new_id, query=variant, steps=[],
                    prompt_variants={}, prompt_style=style, base_task_id=t.task_id,
                ))
    return expanded
