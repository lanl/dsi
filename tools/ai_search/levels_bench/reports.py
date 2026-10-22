"""Notebook display helpers shared by every level notebook."""
from __future__ import annotations

import textwrap
from typing import Iterable, List, Optional

import pandas as pd
from IPython.display import HTML, Markdown, display

from .core import BenchmarkTask, TaskResult
from .results import timing_breakdown_dataframe

STYLE_ORDER = ["standard", "novice", "expert", "terse", "verbose", "casual"]


def _mean_or_nan(df: pd.DataFrame, col: str) -> pd.Series:
    return df[col] if col in df.columns else pd.Series(float("nan"), index=df.index)


def task_table(tasks: Iterable[BenchmarkTask]) -> None:
    """Compact overview of a task list."""
    display(pd.DataFrame([{
        "task_id": t.task_id, "level": t.level, "category": t.category,
        "turns": len(t.turns), "network": t.requires_network,
        "variants": len(t.prompt_variants), "query": textwrap.shorten(t.query, 90),
    } for t in tasks]))


def model_summary(df: pd.DataFrame, extra_judge_cols: Optional[List[str]] = None) -> None:
    """Per-model pass rate / score / efficiency / cost, pass rate by category, and the
    task x model pass matrix."""
    df = df.copy()
    df["judge_efficiency_score"] = _mean_or_nan(df, "judge_efficiency_score")
    aggs = dict(
        pass_rate=("passed", "mean"),
        avg_score=("score", "mean"),
        avg_efficiency=("judge_efficiency_score", "mean"),
        avg_tokens=("tokens", "mean"),
        avg_elapsed_sec=("elapsed_sec", "mean"),
        n_tasks=("task_id", "count"),
        n_errors=("status", lambda s: int(s.isin(["ERROR", "TIMEOUT"]).sum())),
    )
    for col in extra_judge_cols or []:
        if col in df.columns:
            aggs[col.removeprefix("judge_")] = (col, "mean")
    df["passed"] = df["passed"].astype(float)

    display(Markdown("### Overall summary by model"))
    display(df.groupby("model").agg(**aggs).round(2).sort_values("pass_rate", ascending=False))

    display(Markdown("### Pass rate by model and category"))
    display(df.pivot_table(index="model", columns="category", values="passed", aggfunc="mean").round(2))

    display(Markdown("### Pass/fail matrix by task and model"))
    pivot = df.pivot_table(index="task_id", columns="model", values="status", aggfunc="first")
    display(HTML(pivot.to_html(escape=False)))


def failures(df: pd.DataFrame) -> None:
    """Failed / errored rows with the scorer's notes and the judged root cause."""
    cols = [c for c in ["model", "task_id", "status", "score", "notes", "failure_category"] if c in df.columns]
    bad = df[df["status"] != "PASS"][cols]
    display(Markdown(f"### Non-passing runs ({len(bad)})"))
    if bad.empty:
        print("(none)")
    else:
        with pd.option_context("display.max_colwidth", 200):
            display(bad)


def timing_breakdown(results: Iterable[TaskResult]) -> None:
    """Where the time went across a whole run. Top-level phases sum to real wall-clock;
    nested llm_call/tool:* time is shown separately because it is already inside a step."""
    tdf = timing_breakdown_dataframe(results)
    display(Markdown("### Where does the time go?"))
    if tdf.empty:
        print("(no timing data)")
        return
    top = tdf[~tdf["is_nested"]]
    agg = dict(total_sec="sum", mean_sec="mean", count="count")
    display(Markdown("#### Top-level phases (real wall-clock, no double counting)"))
    display(top.groupby("phase_group")["elapsed_sec"].agg(**agg).round(3).sort_values("total_sec", ascending=False))
    display(Markdown("#### By model x top-level phase group (total seconds)"))
    display(top.groupby(["model", "phase_group"])["elapsed_sec"].sum().unstack("phase_group").round(2))
    nested = tdf[tdf["is_nested"]]
    if not nested.empty:
        display(Markdown("#### Nested inside steps (already counted above)"))
        display(nested.groupby("phase")["elapsed_sec"].agg(**agg).round(3).sort_values("total_sec", ascending=False))


def level3_facets(df: pd.DataFrame) -> None:
    cols = [c for c in ["judge_context_score", "judge_variables_score", "judge_relationship_score"] if c in df.columns]
    if not cols:
        return
    display(Markdown("### Hypothesis facet scores (mean across judges)"))
    display(df.pivot_table(index="task_id", columns="model", values=cols).round(2))


def calibration(df: pd.DataFrame) -> None:
    display(Markdown("### Abstention accuracy by model (share of trap tasks correctly abstained on)"))
    display(df.groupby("model")
              .agg(abstention_accuracy=("passed", "mean"),
                   hallucinations=("hallucination_detected", "sum"),
                   n_traps=("task_id", "count"))
              .round(2))
    display(Markdown("### Hallucinated (overconfident) responses"))
    hallucinated = df[df["hallucination_detected"]][["model", "task_id", "response"]]
    if hallucinated.empty:
        print("(none)")
    else:
        display(hallucinated)


def prompt_styles(df: pd.DataFrame) -> None:
    """RQ: does phrasing matter? Pass rate by style, per task, and per-model sensitivity."""
    df = df.copy()
    df["passed"] = df["passed"].astype(float)
    display(Markdown("### Pass rate / avg score by prompt style (all tasks and models)"))
    display(df.groupby("prompt_style")
              .agg(pass_rate=("passed", "mean"), avg_score=("score", "mean"), n=("task_id", "count"))
              .round(3)
              .reindex([s for s in STYLE_ORDER if s in set(df["prompt_style"])]))

    display(Markdown("### Pass rate by base task and prompt style"))
    pivot = df.pivot_table(index="base_task_id", columns="prompt_style", values="passed", aggfunc="mean")
    display(pivot[[c for c in STYLE_ORDER if c in pivot.columns]])

    display(Markdown(
        "### Phrasing sensitivity (spread in pass rate across styles)\n\n"
        "0 = same outcome regardless of wording; higher = more sensitive to how the task was asked."
    ))
    sensitivity = (
        df.groupby(["base_task_id", "model", "prompt_style"])["passed"].mean().reset_index()
          .groupby(["base_task_id", "model"])["passed"].agg(lambda s: s.max() - s.min())
          .reset_index(name="pass_rate_spread")
          .sort_values("pass_rate_spread", ascending=False)
    )
    display(sensitivity)
    display(Markdown("### Mean sensitivity by model"))
    display(sensitivity.groupby("model")["pass_rate_spread"].mean().round(3))


def transcript(results: Iterable[TaskResult], task_id: str, model: Optional[str] = None) -> None:
    """Render the recorded agent transcript(s) for one task."""
    found = False
    for r in results:
        if r.task_id == task_id and (model is None or r.model == model):
            found = True
            display(Markdown(f"#### {r.model} — {r.status} (score {r.score})"))
            display(Markdown(r.transcript or f"(no transcript; notes: {r.notes})"))
    if not found:
        print(f"No results for task {task_id!r}" + (f" and model {model!r}" if model else ""))


def effective_productivity(df: pd.DataFrame) -> pd.DataFrame:
    """RQ1: a task is a productivity win only if the agent passed AND beat the human
    reference_time_sec. Needs reference_time_sec on the tasks."""
    if "time_ratio" not in df.columns or df["time_ratio"].dropna().empty:
        print("No reference_time_sec set on any task yet; a human must time each task's "
              "reference_path once (see BENCHMARK_METHODOLOGY.md, RQ1).")
        return pd.DataFrame()
    sub = df.dropna(subset=["time_ratio"])
    return (
        sub.groupby("model")
        .apply(lambda g: pd.Series({
            "pass_rate": g["passed"].mean(),
            "mean_productivity_gain_when_passed": g.loc[g["passed"] == True, "productivity_gain"].mean(),
            "effective_productivity": g["passed"].mean() * g.loc[g["passed"] == True, "productivity_gain"].mean(),
            "n_tasks": len(g),
        }))
        .round(3)
    )


def research_questions(summary_df: pd.DataFrame) -> None:
    """Cross-level RQ1-RQ7 tables for the combined results of every level."""
    if summary_df.empty:
        print("No results loaded; run the level notebooks first.")
        return
    df = summary_df.copy()
    df["passed"] = df["passed"].astype(float)
    df["judge_efficiency_score"] = _mean_or_nan(df, "judge_efficiency_score")
    leveled = df[df["level"].isin([1, 2, 3]) & ~df["expects_abstention"].astype(bool)]

    display(Markdown("### RQ2 — Pass rate / score / efficiency / latency by complexity level and model"))
    display(leveled.groupby(["model", "level"]).agg(
        pass_rate=("passed", "mean"), avg_score=("score", "mean"),
        avg_efficiency=("judge_efficiency_score", "mean"),
        avg_elapsed_sec=("elapsed_sec", "mean"), avg_tokens=("tokens", "mean"),
        n_tasks=("task_id", "count"),
    ).round(2))
    try:
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(6, 4))
        for model_name, g in leveled.groupby("model"):
            by_level = g.groupby("level")["passed"].mean()
            ax.plot(by_level.index, by_level.values, marker="o", label=model_name)
        ax.set_xlabel("Complexity level")
        ax.set_ylabel("Pass rate")
        ax.set_xticks([1, 2, 3])
        ax.set_title("Pass rate vs. task complexity (RQ2)")
        ax.legend(fontsize=8)
        plt.show()
    except Exception as e:
        print(f"(skipping plot: {e!r})")

    cal = df[df["expects_abstention"].astype(bool)]
    if not cal.empty:
        display(Markdown("### RQ4 — Calibration: abstention accuracy on trap tasks"))
        display(cal.groupby("model").agg(
            abstention_accuracy=("passed", "mean"),
            hallucinations=("hallucination_detected", "sum"),
            n_traps=("task_id", "count"),
        ).round(2))

    failed = df[df["passed"] == 0]
    if not failed.empty:
        display(Markdown("### RQ5 — Failure-category distribution by model"))
        display(failed.pivot_table(index="model", columns="failure_category", values="task_id",
                                   aggfunc="count", fill_value=0))

    display(Markdown("### RQ3/RQ7 — Checkpoint-opportunity counts vs. outcome"))
    display(df.groupby("model").agg(
        avg_checkpoint_opportunities=("checkpoint_opportunity_count", "mean"),
        avg_high_stakes_actions=("high_stakes_action_count", "mean"),
        avg_hedge_language=("hedge_language_count", "mean"),
        pass_rate=("passed", "mean"),
    ).round(2))

    display(Markdown("### RQ1 — Productivity vs. human baseline"))
    display(effective_productivity(df))
