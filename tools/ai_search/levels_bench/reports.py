"""Notebook display helpers shared by every level notebook."""
from __future__ import annotations

import textwrap
from typing import Iterable, List, Optional

import polars as pl
from IPython.display import Markdown, display

from .core import BenchmarkTask, TaskResult
from .results import timing_breakdown_dataframe

STYLE_ORDER = ["standard", "novice", "expert", "terse", "verbose", "casual"]


def _ensure_float_col(df: pl.DataFrame, col: str) -> pl.DataFrame:
    if col in df.columns:
        return df.with_columns(pl.col(col).cast(pl.Float64, strict=False))
    return df.with_columns(pl.lit(None).cast(pl.Float64).alias(col))


def _round_floats(df: pl.DataFrame, n: int) -> pl.DataFrame:
    float_cols = [c for c, dt in df.schema.items() if dt.is_float()]
    return df.with_columns([pl.col(c).round(n) for c in float_cols]) if float_cols else df


def _order_by(df: pl.DataFrame, col: str, order: List[str]) -> pl.DataFrame:
    """Stable sort of `col` into `order`, dropping values that aren't in the list."""
    ranking = pl.DataFrame({col: order}).with_row_index("_ord")
    return df.join(ranking, on=col, how="inner").sort("_ord").drop("_ord")


def _select_existing(df: pl.DataFrame, cols: Iterable[str]) -> List[str]:
    return [c for c in cols if c in df.columns]


def task_table(tasks: Iterable[BenchmarkTask]) -> None:
    """Compact overview of a task list."""
    display(pl.DataFrame([{
        "task_id": t.task_id, "level": t.level, "category": t.category,
        "turns": len(t.turns), "network": t.requires_network,
        "variants": len(t.prompt_variants), "query": textwrap.shorten(t.query, 90),
    } for t in tasks]))


def model_summary(df: pl.DataFrame, extra_judge_cols: Optional[List[str]] = None) -> None:
    """Per-model pass rate / score / efficiency / cost, pass rate by category, and the
    task x model pass matrix."""
    df = _ensure_float_col(df, "judge_efficiency_score")
    df = df.with_columns(pl.col("passed").cast(pl.Float64, strict=False))
    aggs = [
        pl.col("passed").mean().alias("pass_rate"),
        pl.col("score").mean().alias("avg_score"),
        pl.col("judge_efficiency_score").mean().alias("avg_efficiency"),
        pl.col("tokens").mean().alias("avg_tokens"),
        pl.col("elapsed_sec").mean().alias("avg_elapsed_sec"),
        pl.col("task_id").count().alias("n_tasks"),
        pl.col("status").is_in(["ERROR", "TIMEOUT"]).sum().alias("n_errors"),
    ]
    for col in extra_judge_cols or []:
        if col in df.columns:
            aggs.append(pl.col(col).mean().alias(col.removeprefix("judge_")))

    display(Markdown("### Overall summary by model"))
    display(_round_floats(df.group_by("model").agg(aggs), 2).sort("pass_rate", descending=True))

    display(Markdown("### Pass rate by model and category"))
    display(_round_floats(
        df.pivot(index="model", on="category", values="passed", aggregate_function="mean"), 2
    ))

    display(Markdown("### Pass/fail matrix by task and model"))
    display(df.pivot(index="task_id", on="model", values="status", aggregate_function="first"))


def failures(df: pl.DataFrame) -> None:
    """Failed / errored rows with the scorer's notes and the judged root cause."""
    cols = _select_existing(df, ["model", "task_id", "status", "score", "notes", "failure_category"])
    bad = df.filter(pl.col("status") != "PASS").select(cols)
    display(Markdown(f"### Non-passing runs ({bad.height})"))
    if bad.is_empty():
        print("(none)")
    else:
        with pl.Config(fmt_str_lengths=200, tbl_width_chars=200):
            display(bad)


def timing_breakdown(results: Iterable[TaskResult]) -> None:
    """Where the time went across a whole run. Top-level phases sum to real wall-clock;
    nested llm_call/tool:* time is shown separately because it is already inside a step."""
    tdf = timing_breakdown_dataframe(results)
    display(Markdown("### Where does the time go?"))
    if tdf.is_empty():
        print("(no timing data)")
        return
    top = tdf.filter(~pl.col("is_nested"))
    phase_aggs = [
        pl.col("elapsed_sec").sum().alias("total_sec"),
        pl.col("elapsed_sec").mean().alias("mean_sec"),
        pl.col("elapsed_sec").count().alias("count"),
    ]
    display(Markdown("#### Top-level phases (real wall-clock, no double counting)"))
    display(_round_floats(top.group_by("phase_group").agg(phase_aggs), 3).sort("total_sec", descending=True))
    display(Markdown("#### By model x top-level phase group (total seconds)"))
    display(_round_floats(
        top.group_by(["model", "phase_group"]).agg(pl.col("elapsed_sec").sum())
        .pivot(index="model", on="phase_group", values="elapsed_sec", aggregate_function="first"),
        2,
    ))
    nested = tdf.filter(pl.col("is_nested"))
    if not nested.is_empty():
        display(Markdown("#### Nested inside steps (already counted above)"))
        display(_round_floats(nested.group_by("phase").agg(phase_aggs), 3).sort("total_sec", descending=True))


def level3_facets(df: pl.DataFrame) -> None:
    cols = _select_existing(df, ["judge_context_score", "judge_variables_score", "judge_relationship_score"])
    if not cols:
        return
    scored = df.filter(pl.any_horizontal([pl.col(c).is_not_null() for c in cols]))
    if scored.is_empty():
        return
    display(Markdown("### Hypothesis facet scores (mean across judges)"))
    display(_round_floats(
        scored.pivot(index="task_id", on="model", values=cols, aggregate_function="mean"), 2
    ))


def calibration(df: pl.DataFrame) -> None:
    display(Markdown("### Abstention accuracy by model (share of trap tasks correctly abstained on)"))
    display(_round_floats(df.group_by("model").agg(
        pl.col("passed").mean().alias("abstention_accuracy"),
        pl.col("hallucination_detected").sum().alias("hallucinations"),
        pl.col("task_id").count().alias("n_traps"),
    ), 2))
    display(Markdown("### Hallucinated (overconfident) responses"))
    hallucinated = df.filter(pl.col("hallucination_detected")).select("model", "task_id", "response")
    if hallucinated.is_empty():
        print("(none)")
    else:
        display(hallucinated)


def prompt_styles(df: pl.DataFrame) -> None:
    """RQ: does phrasing matter? Pass rate by style, per task, and per-model sensitivity."""
    df = df.with_columns(pl.col("passed").cast(pl.Float64, strict=False))
    present = [s for s in STYLE_ORDER if s in df.get_column("prompt_style").unique().to_list()]

    display(Markdown("### Pass rate / avg score by prompt style (all tasks and models)"))
    display(_round_floats(_order_by(df.group_by("prompt_style").agg(
        pl.col("passed").mean().alias("pass_rate"),
        pl.col("score").mean().alias("avg_score"),
        pl.col("task_id").count().alias("n"),
    ), "prompt_style", present), 3))

    display(Markdown("### Pass rate by base task and prompt style"))
    pivot = df.pivot(index="base_task_id", on="prompt_style", values="passed", aggregate_function="mean")
    display(pivot.select(["base_task_id"] + [c for c in STYLE_ORDER if c in pivot.columns]))

    display(Markdown(
        "### Phrasing sensitivity (spread in pass rate across styles)\n\n"
        "0 = same outcome regardless of wording; higher = more sensitive to how the task was asked."
    ))
    sensitivity = (
        df.group_by(["base_task_id", "model", "prompt_style"])
        .agg(pl.col("passed").mean())
        .group_by(["base_task_id", "model"])
        .agg((pl.col("passed").max() - pl.col("passed").min()).alias("pass_rate_spread"))
        .sort("pass_rate_spread", descending=True)
    )
    display(sensitivity)
    display(Markdown("### Mean sensitivity by model"))
    display(_round_floats(
        sensitivity.group_by("model").agg(pl.col("pass_rate_spread").mean()), 3
    ))


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


def effective_productivity(df: pl.DataFrame) -> pl.DataFrame:
    """RQ1: a task is a productivity win only if the agent passed AND beat the human
    reference_time_sec. Needs reference_time_sec on the tasks."""
    if "time_ratio" not in df.columns or df.filter(pl.col("time_ratio").is_not_null()).is_empty():
        print("No reference_time_sec set on any task yet; a human must time each task's "
              "reference_path once (see BENCHMARK_METHODOLOGY.md, RQ1).")
        return pl.DataFrame()
    sub = df.filter(pl.col("time_ratio").is_not_null())
    return _round_floats(sub.group_by("model").agg(
        pl.col("passed").mean().alias("pass_rate"),
        pl.col("productivity_gain").filter(pl.col("passed").cast(pl.Boolean, strict=False)).mean().alias("mean_productivity_gain_when_passed"),
        pl.len().alias("n_tasks"),
    ).with_columns(
        (pl.col("pass_rate") * pl.col("mean_productivity_gain_when_passed")).alias("effective_productivity")
    ), 3)


def research_questions(summary_df: pl.DataFrame) -> None:
    """Cross-level RQ1-RQ7 tables for the combined results of every level."""
    if summary_df.is_empty():
        print("No results loaded; run the level notebooks first.")
        return
    df = _ensure_float_col(summary_df, "judge_efficiency_score")
    df = df.with_columns(pl.col("passed").cast(pl.Float64, strict=False))
    leveled = df.filter(pl.col("level").is_in([1, 2, 3]) & ~pl.col("expects_abstention").fill_null(False))

    display(Markdown("### RQ2 — Pass rate / score / efficiency / latency by complexity level and model"))
    display(_round_floats(leveled.group_by(["model", "level"]).agg(
        pl.col("passed").mean().alias("pass_rate"),
        pl.col("score").mean().alias("avg_score"),
        pl.col("judge_efficiency_score").mean().alias("avg_efficiency"),
        pl.col("elapsed_sec").mean().alias("avg_elapsed_sec"),
        pl.col("tokens").mean().alias("avg_tokens"),
        pl.col("task_id").count().alias("n_tasks"),
    ).sort(["model", "level"]), 2))
    try:
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(6, 4))
        for model_name in leveled.get_column("model").unique().to_list():
            by_level = (
                leveled.filter(pl.col("model") == model_name)
                .group_by("level").agg(pl.col("passed").mean())
                .sort("level")
            )
            ax.plot(by_level["level"].to_list(), by_level["passed"].to_list(), marker="o", label=model_name)
        ax.set_xlabel("Complexity level")
        ax.set_ylabel("Pass rate")
        ax.set_xticks([1, 2, 3])
        ax.set_title("Pass rate vs. task complexity (RQ2)")
        ax.legend(fontsize=8)
        plt.show()
    except Exception as e:
        print(f"(skipping plot: {e!r})")

    cal = df.filter(pl.col("expects_abstention").fill_null(False))
    if not cal.is_empty():
        display(Markdown("### RQ4 — Calibration: abstention accuracy on trap tasks"))
        display(_round_floats(cal.group_by("model").agg(
            pl.col("passed").mean().alias("abstention_accuracy"),
            pl.col("hallucination_detected").sum().alias("hallucinations"),
            pl.col("task_id").count().alias("n_traps"),
        ), 2))

    failed = df.filter(pl.col("passed") == 0)
    if not failed.is_empty():
        display(Markdown("### RQ5 — Failure-category distribution by model"))
        display(
            failed.group_by(["model", "failure_category"]).agg(pl.len().alias("n"))
            .pivot(index="model", on="failure_category", values="n", aggregate_function="first")
            .fill_null(0)
        )

    display(Markdown("### RQ3/RQ7 — Checkpoint-opportunity counts vs. outcome"))
    display(_round_floats(df.group_by("model").agg(
        pl.col("checkpoint_opportunity_count").mean().alias("avg_checkpoint_opportunities"),
        pl.col("high_stakes_action_count").mean().alias("avg_high_stakes_actions"),
        pl.col("hedge_language_count").mean().alias("avg_hedge_language"),
        pl.col("passed").mean().alias("pass_rate"),
    ), 2))

    display(Markdown("### RQ1 — Productivity vs. human baseline"))
    display(effective_productivity(df))
