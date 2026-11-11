"""Scoring: Level 1 checks, Level 2/3 LLM-judge ensembles, calibration, checkpoint
detection (RQ3/RQ7) and failure attribution (RQ5)."""
from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional

from tool_utils import get_timing_log, timed

from .config import CONFIG, make_llm
from .core import BenchmarkTask, TaskResult


def canonicalize_db_path(path: str) -> str:
    """Normalize a DB path for exact Level-1 path matching."""
    s = Path(str(path)).as_posix()
    if "oceans_11/" in s:
        return s[s.index("oceans_11/"):]
    return Path(s).name


def _resolve_path_arg(call_args: Dict[str, Any]) -> str:
    """Resolve a load_dsi_tool 'path' arg the way load_dsi() does: a bare filename plus a
    separate `master_db_folder` kwarg refers to <folder>/<path>."""
    path = str(call_args.get("path", ""))
    folder = call_args.get("master_db_folder")
    if folder and not Path(path).is_absolute():
        return str(Path(str(folder)) / path)
    return path


def _count_found(expected: int, blob: str) -> bool:
    patterns = [
        rf'["\']?(?:count|missing_count)["\']?\s*[:=]\s*{expected}\b',
        rf"\b{expected}\b",
    ]
    return any(re.search(p, blob) for p in patterns)


def score_level1(task: BenchmarkTask, result: TaskResult) -> TaskResult:
    """7 independent binary checks (tool, args, path, count, keywords, forbidden keywords,
    nonempty). score = fraction passed; passed = all checks passed."""
    tool_names = [t["name"] for t in result.tool_calls]
    matching_calls = [t for t in result.tool_calls if t["name"] in (task.expected_tools or tool_names)]
    notes = []

    tool_ok = True
    if task.expected_tools:
        tool_ok = any(t in tool_names for t in task.expected_tools)
        if not tool_ok:
            notes.append(f"Expected one of {task.expected_tools}, got {tool_names}")

    args_ok = True
    if task.expected_args and tool_ok:
        def _arg_match(call_args: Dict[str, Any], key: str, want: Any) -> bool:
            if key == "path":
                return canonicalize_db_path(_resolve_path_arg(call_args)) == canonicalize_db_path(str(want))
            return str(call_args.get(key)) == str(want)

        args_ok = any(
            all(_arg_match(call["args"], k, v) for k, v in task.expected_args.items())
            for call in matching_calls
        )
        if not args_ok:
            notes.append(f"Expected args {task.expected_args}, got {[c['args'] for c in matching_calls]}")

    path_ok = True
    if task.expected_path is not None and tool_ok:
        want = canonicalize_db_path(task.expected_path)
        path_ok = any(
            (got := _resolve_path_arg(call["args"])) and canonicalize_db_path(got) == want
            for call in matching_calls
        )
        if not path_ok:
            notes.append(f"Expected canonical path {want}, got {[c['args'].get('path') for c in matching_calls]}")

    quant_ok = True
    if task.expected_count is not None:
        quant_ok = _count_found(task.expected_count, " ".join(result.tool_results + [result.response]))
        if not quant_ok:
            notes.append(f"Expected count {task.expected_count} not found in tool results/response")

    tools_blob = " ".join(result.tool_results).lower() + " " + result.response.lower()
    keyword_ok = True
    if task.response_keywords:
        keyword_ok = any(kw.lower() in tools_blob for kw in task.response_keywords)
        if not keyword_ok:
            notes.append(f"Response missing keywords: {task.response_keywords}")

    forbidden_ok = True
    if task.forbidden_keywords:
        hits = [kw for kw in task.forbidden_keywords if kw.lower() in tools_blob]
        if hits:
            forbidden_ok = False
            notes.append(f"Forbidden distractors present: {hits}")

    # Atomic tools may return short structured answers; allow short responses when tools ran.
    nonempty = len(result.response.strip()) > 0 or bool(result.tool_calls)
    if not nonempty:
        notes.append("Response too short or empty")

    checks = {
        "tool_ok": tool_ok, "args_ok": args_ok, "path_ok": path_ok, "quant_ok": quant_ok,
        "keyword_ok": keyword_ok, "forbidden_ok": forbidden_ok, "nonempty_ok": nonempty,
    }
    result.check_results = checks
    result.passed = all(checks.values())
    result.score = round(sum(checks.values()) / len(checks), 3)
    result.notes = "; ".join(notes)
    return result


def llm_judge(prompt: str, llm) -> dict:
    """Ask one judge LLM and parse JSON from its reply ({"raw": text} if not JSON)."""
    try:
        text = llm.invoke(prompt).content.strip()
    except Exception as e:
        return {"raw": f"judge error: {e!r}"}
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    return {"raw": text}


def llm_judge_ensemble(prompt: str) -> List[Dict[str, Any]]:
    """Run the same prompt against every judge model (concurrently when
    CONFIG.parallel_judges) and return each verdict tagged with "_judge_model"."""
    models = list(CONFIG.judge_models)

    def _one(model_name: str) -> Dict[str, Any]:
        verdict = llm_judge(prompt, make_llm(model_name, temperature=0.0))
        verdict["_judge_model"] = model_name
        return verdict

    if CONFIG.parallel_judges and len(models) > 1:
        with ThreadPoolExecutor(max_workers=len(models)) as pool:
            return list(pool.map(_one, models))
    return [_one(m) for m in models]


def _avg_ensemble_field(verdicts: List[Dict[str, Any]], key: str) -> Optional[float]:
    vals = [float(v[key]) for v in verdicts if isinstance(v.get(key), (int, float))]
    return round(sum(vals) / len(vals), 3) if vals else None


def _format_ensemble_reasoning(verdicts: List[Dict[str, Any]], reasoning_key: str = "reasoning") -> str:
    return " | ".join(f"[{v.get('_judge_model')}] {v.get(reasoning_key, v.get('raw', ''))}" for v in verdicts)


def _store_judged(result: TaskResult, verdicts: List[Dict[str, Any]], key: str, reasoning_name: str) -> Optional[float]:
    value = _avg_ensemble_field(verdicts, key)
    result.judge_scores = result.judge_scores or {}
    result.judge_scores[key] = value
    result.judge_scores[f"{key}_by_judge"] = {v.get("_judge_model"): v.get(key) for v in verdicts}
    result.judge_scores[reasoning_name] = _format_ensemble_reasoning(verdicts)
    return value


def score_efficiency(task: BenchmarkTask, result: TaskResult) -> TaskResult:
    """LLM-judge the agent's path against the human reference CLI path (0.0-1.0)."""
    if not task.reference_path:
        return result
    agent_tool_calls = [f"{c['name']}({c['args']})" for c in result.tool_calls]
    agent_dsi_calls = [f"{c['method']}({', '.join(c.get('args', []))})" for c in result.dsi_calls]
    prompt = f"""You are judging the EFFICIENCY of an AI agent's path for a data-management task,
against the most efficient path a human expert would take at the DSI command line.

Task: {task.query}

Reference path (ground truth, most efficient -- one command per line):
{chr(10).join(task.reference_path)}

Agent's actual tool calls (in order):
{json.dumps(agent_tool_calls, indent=2)}

Agent's actual underlying DSI API calls (in order):
{json.dumps(agent_dsi_calls, indent=2)}

Score the agent's efficiency from 0.0 (far less efficient than the reference -- redundant
queries, unnecessary exploration, retries, or extra DSI calls beyond what the reference
path needed) to 1.0 (matched the reference path's number of essential steps with no waste).
A correct final answer reached via a wasteful path should still score low here.

Respond with ONLY JSON: {{"efficiency_score": <float 0.0-1.0>, "reasoning": "<one sentence>"}}
"""
    _store_judged(result, llm_judge_ensemble(prompt), "efficiency_score", "efficiency_reasoning")
    return result


def score_level2(task: BenchmarkTask, result: TaskResult) -> TaskResult:
    """Every expected tool must be used and every keyword present (correctness, 50%), plus an
    LLM-judged process-quality score against success_criteria (50%). passed = correctness."""
    tool_names = [t["name"] for t in result.tool_calls]
    notes = []

    tools_ok = True
    if task.expected_tools:
        missing = [t for t in task.expected_tools if t not in tool_names]
        tools_ok = not missing
        if missing:
            notes.append(f"Missing required tools for this workflow: {missing} (called: {tool_names})")

    blob = " ".join(result.tool_results + [result.response])
    blob_lower = blob.lower()

    quant_ok = True
    if task.expected_count is not None:
        quant_ok = _count_found(task.expected_count, blob)
        if not quant_ok:
            notes.append(f"Expected count {task.expected_count} not found in tool results/response")

    keyword_ok = True
    if task.response_keywords:
        missing_kw = [kw for kw in task.response_keywords if kw.lower() not in blob_lower]
        keyword_ok = not missing_kw
        if missing_kw:
            notes.append(f"Response missing required facts/keywords: {missing_kw}")

    forbidden_ok = True
    if task.forbidden_keywords:
        hits = [kw for kw in task.forbidden_keywords if kw.lower() in blob_lower]
        if hits:
            forbidden_ok = False
            notes.append(f"Forbidden distractors present: {hits}")

    nonempty = len(result.response.strip()) > 0
    if not nonempty:
        notes.append("Response too short or empty")

    correctness_ok = all([tools_ok, quant_ok, keyword_ok, forbidden_ok, nonempty])

    process_score = None
    if task.success_criteria:
        prompt = f"""You are judging the METHOD QUALITY of an AI agent's multi-step workflow for a
data-management task, against a rubric describing what a good approach looks like.

Task: {task.query}

Rubric (what a high-quality workflow should do): {task.success_criteria}

Agent's tool calls (in order): {json.dumps([f"{c['name']}({c['args']})" for c in result.tool_calls], indent=2)}

Agent's final response: {result.response}

Score process quality from 0.0 (ignored the rubric, wrong/haphazard approach) to 1.0 (followed
a sound, efficient multi-step approach matching the rubric's intent).

Respond with ONLY JSON: {{"process_quality_score": <float 0.0-1.0>, "reasoning": "<one sentence>"}}
"""
        process_score = _store_judged(
            result, llm_judge_ensemble(prompt), "process_quality_score", "process_quality_reasoning"
        )

    result.passed = correctness_ok
    correctness = 1.0 if correctness_ok else 0.0
    if isinstance(process_score, (int, float)):
        result.score = round(0.5 * correctness + 0.5 * float(process_score), 3)
    else:
        result.score = correctness
    result.notes = "; ".join(notes)
    return result


def score_level3(task: BenchmarkTask, result: TaskResult) -> TaskResult:
    """DiscoveryBench-style faceted judge (context / variables / relationship, each 0-1).
    score = mean facet score; passed = score >= 0.6."""
    gt = task.ground_truth_hypothesis or {}
    if not gt:
        result.notes = "no ground_truth_hypothesis defined on this task; skipping level-3 scoring"
        result.score = None
        result.passed = None
        return result

    prompt = f"""You are judging a scientific HYPOTHESIS an AI agent proposed after analyzing a DSI
scientific dataset, against a ground-truth hypothesis written by a human expert who examined the
same data (DiscoveryBench-style faceted scoring). Score each facet from 0.0 (not addressed or
contradicted) to 1.0 (fully matches in substance, wording may differ).

Task: {task.query}

Ground-truth hypothesis facets:
- context: {gt.get('context', '')}
- variables: {gt.get('variables', '')}
- relationship: {gt.get('relationship', '')}

Agent's final response:
{result.response}

Respond with ONLY JSON: {{"context_score": <0.0-1.0>, "variables_score": <0.0-1.0>,
"relationship_score": <0.0-1.0>, "reasoning": "<1-2 sentences noting any facet missed or contradicted>"}}
"""
    verdicts = llm_judge_ensemble(prompt)
    facets = {k: _avg_ensemble_field(verdicts, k) for k in ("context_score", "variables_score", "relationship_score")}
    reasoning = _format_ensemble_reasoning(verdicts)
    result.judge_scores = result.judge_scores or {}
    result.judge_scores.update({**facets, "hypothesis_reasoning": reasoning})
    facet_scores = [s for s in facets.values() if isinstance(s, (int, float))]
    result.score = round(sum(facet_scores) / len(facet_scores), 3) if facet_scores else 0.0
    result.passed = result.score >= 0.6
    result.notes = reasoning
    return result


DEFAULT_ABSTENTION_PHRASES = [
    "not found", "no such", "does not exist", "doesn't exist", "cannot find", "can't find",
    "could not find", "couldn't find", "unable to determine", "unable to locate",
    "no information", "not available", "not specified", "no matching", "no records",
    "none found", "no datasets", "not documented", "could you clarify", "could you specify",
    "which dataset", "can you clarify", "not sure", "i don't know", "i do not know",
    "no such column", "no such field", "unclear", "does not specify", "not provided",
]


def score_calibration(task: BenchmarkTask, result: TaskResult) -> TaskResult:
    """RQ4 trap tasks: pass iff the response abstains / asks for clarification (keyword
    heuristic); otherwise notes are tagged HALLUCINATION."""
    phrases = [p.lower() for p in (task.abstention_keywords or DEFAULT_ABSTENTION_PHRASES)]
    abstained = any(p in result.response.lower() for p in phrases)
    result.passed = abstained
    result.score = 1.0 if abstained else 0.0
    result.notes = (
        "correctly abstained / flagged uncertainty" if abstained
        else "HALLUCINATION: answered confidently despite missing, nonexistent, or ambiguous data"
    )
    return result


HIGH_STAKES_TOOLS = {"download_file_tool", "send_email_tool", "upload_paper_tool"}
HEDGE_PHRASES = [
    "i think", "probably", "not certain", "not sure", "may be", "might be", "possibly",
    "i believe", "seems like", "appears to be", "could be wrong", "unclear", "i'm not 100%",
]


def detect_checkpoint_opportunities(result: TaskResult) -> TaskResult:
    """Metrics-only (RQ3/RQ7): flag high-stakes tool calls and hedge language where a
    human-in-the-loop checkpoint would plausibly have fired."""
    events = [
        {"type": "high_stakes_action", "tool": c["name"], "args": c.get("args", {})}
        for c in result.tool_calls if c.get("name") in HIGH_STAKES_TOOLS
    ]
    response_lower = result.response.lower()
    events += [{"type": "hedge_language", "phrase": p} for p in HEDGE_PHRASES if p in response_lower]
    result.checkpoint_events = events
    return result


FAILURE_CATEGORIES = [
    "MODEL_REASONING",    # tools/data were available; the model chose poorly or misread a result
    "TOOL_FRAMEWORK",     # a tool exception, agent-framework bug, or timeout occurred
    "TASK_SPECIFICATION", # the task/rubric is ambiguous or the scorer is too strict
    "PROMPT_GAP",         # the agent's system prompt lacked guidance that would have helped
    "ENVIRONMENT_DATA",   # network/API/file unavailability outside the agent's control
]


def classify_failure(task: BenchmarkTask, result: TaskResult) -> TaskResult:
    """RQ5: judge the root cause of a failed task. Categories can't be averaged, so the
    ensemble must agree; otherwise the category is UNKNOWN (every judge's pick is kept)."""
    if result.passed:
        return result
    prompt = f"""An AI agent failed a data-management benchmark task. Classify the MOST LIKELY
root cause into exactly one of these categories:

- MODEL_REASONING: tools/data were available; the model picked the wrong approach or misread a result
- TOOL_FRAMEWORK: a tool exception, agent-framework bug, or timeout occurred
- TASK_SPECIFICATION: the task or its scoring rubric is ambiguous / overly strict for a reasonable answer
- PROMPT_GAP: the agent's system prompt lacked guidance that would have prevented this
- ENVIRONMENT_DATA: network, external API, or file unavailability outside the agent's control

Task query: {task.query}
Scorer notes: {result.notes}
Agent's tool calls: {json.dumps(result.tool_calls, default=str)[:2000]}
Agent's final response: {result.response[:1000]}

Respond with ONLY JSON: {{"failure_category": "<one of the five category names above, exactly as spelled>", "rationale": "<one sentence>"}}
"""
    verdicts = llm_judge_ensemble(prompt)
    valid = [v.get("failure_category") for v in verdicts if v.get("failure_category") in FAILURE_CATEGORIES]
    result.failure_category = valid[0] if valid and len(set(valid)) == 1 else "UNKNOWN"
    result.failure_rationale = " | ".join(
        f"[{v.get('_judge_model')}] {v.get('failure_category')}: {v.get('rationale', v.get('raw', ''))}"
        for v in verdicts
    )
    return result


def score_task(task: BenchmarkTask, result: TaskResult) -> TaskResult:
    """Route to the level's scorer (calibration tasks by expects_abstention), then add
    efficiency, checkpoint detection and failure classification."""
    if task.expects_abstention:
        with timed("score:calibration"):
            result = score_calibration(task, result)
    elif task.level == 1:
        with timed("score:level1"):
            result = score_level1(task, result)
    elif task.level == 2:
        with timed("score:level2"):
            result = score_level2(task, result)
    else:
        with timed("score:level3"):
            result = score_level3(task, result)
    with timed("score:efficiency"):
        result = score_efficiency(task, result)
    with timed("score:checkpoint_opportunities"):
        result = detect_checkpoint_opportunities(result)
    with timed("score:classify_failure"):
        result = classify_failure(task, result)
    result.timing_log = get_timing_log()
    return result
