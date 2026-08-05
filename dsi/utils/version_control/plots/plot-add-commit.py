#!/usr/bin/env python3
"""Create a grouped bar chart from Git/Git LFS benchmark CSV files."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

import matplotlib.pyplot as plt
import pandas as pd


REQUIRED_COLUMNS = {
    "run",
    "add_ms",
    "commit_ms",
    "total_ms",
}

DATASETS = ["small", "medium", "large"]
TOOLS = ["rsnapshot", "git", "gitlfs", "dsivcs"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read one or more benchmark CSV files and plot mean Git add and "
            "commit times as stacked bars."
        )
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("add-commit-grouped.png"),
        help="Output image path (default: add-commit-grouped.png).",
    )
    parser.add_argument(
        "--title",
        default="Add and Commit Benchmark",
        help="Chart title.",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=300,
        help="Image resolution (default: 300 DPI).",
    )
    return parser.parse_args()


def read_results(csv_path: Path, dataset: str, tool: str) -> pd.DataFrame:
    try:
        data = pd.read_csv(csv_path)
    except (OSError, pd.errors.ParserError) as exc:
        raise ValueError(f"Cannot read {csv_path}: {exc}") from exc

    missing = REQUIRED_COLUMNS.difference(data.columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(f"{csv_path} is missing columns: {missing_text}")

    if data.empty:
        raise ValueError(f"{csv_path} contains no benchmark runs")

    for column in ("add_ms", "commit_ms", "total_ms"):
        data[column] = pd.to_numeric(data[column], errors="coerce")
        if data[column].isna().any():
            raise ValueError(f"{csv_path} contains a non-numeric value in {column}")
        if (data[column] < 0).any():
            raise ValueError(f"{csv_path} contains a negative value in {column}")

    summary = (
        data.groupby("source_directory", as_index=False, sort=False)
        .agg(
            add_ms=("add_ms", "mean"),
            commit_ms=("commit_ms", "mean"),
            total_ms=("total_ms", "mean"),
            runs=("run", "count"),
        )
    )
    summary[["add_ms", "commit_ms", "total_ms"]] /= 1000.0
    summary["dataset"] = dataset
    summary["tool"] = tool
    return summary


def add_value_labels(ax: plt.Axes, bars, values: Sequence[float]) -> None:
    largest_total = max(
        (patch.get_y() + patch.get_height() for patch in bars), default=0.0
    )
    minimum_height = largest_total * 0.04

    for patch, value in zip(bars, values):
        if patch.get_height() >= minimum_height:
            ax.text(
                patch.get_x() + patch.get_width() / 2,
                patch.get_y() + patch.get_height() / 2,
                f"{value:,.1f}",
                ha="center",
                va="center",
                fontsize=8,
                color="white",
                fontweight="bold",
            )


def plot(summary: pd.DataFrame, output: Path, title: str, dpi: int) -> None:
    dataset_order = DATASETS
    tool_order = TOOLS
    grouped = summary.groupby(["dataset", "tool"], sort=False).agg(
        add_ms=("add_ms", "mean"),
        commit_ms=("commit_ms", "mean"),
    ).reset_index()

    if grouped.empty:
        raise ValueError("No benchmark data available to plot.")

    group_count = len(dataset_order)
    tool_count = len(tool_order)
    bar_width = 0.8 / tool_count

    figure_width = max(8.0, 1.0 + group_count * 1.4)
    fig, ax = plt.subplots(figsize=(figure_width, 5.2), constrained_layout=True)

    x_positions = list(range(group_count))
    handles = []
    colors = plt.get_cmap("tab10")(range(tool_count))

    for tool_index, tool in enumerate(tool_order):
        tool_data = grouped[grouped["tool"] == tool].set_index("dataset")
        add_values = [tool_data.at[dataset, "add_ms"] if dataset in tool_data.index else 0.0 for dataset in dataset_order]
        commit_values = [tool_data.at[dataset, "commit_ms"] if dataset in tool_data.index else 0.0 for dataset in dataset_order]

        positions = [x + (tool_index - (tool_count - 1) / 2) * bar_width for x in x_positions]
        add_bars = ax.bar(
            positions,
            add_values,
            width=bar_width,
            label=f"{tool} add",
            color=colors[tool_index],
        )
        commit_bars = ax.bar(
            positions,
            commit_values,
            width=bar_width,
            bottom=add_values,
            label=f"{tool} commit",
            color=colors[tool_index],
            alpha=0.6,
        )
        handles.append((add_bars, commit_bars))

        add_value_labels(ax, add_bars, add_values)
        add_value_labels(ax, commit_bars, commit_values)

    ax.set_title(title, pad=14, fontweight="bold")
    ax.set_ylabel("Mean runtime (s)")
    ax.set_xticks(x_positions)
    ax.set_xticklabels(dataset_order)
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)

    legend_entries = []
    for tool in tool_order:
        legend_entries.append(plt.Line2D([0], [0], color="black", marker="s", linestyle="", label=tool))
    ax.legend(legend_entries, tool_order, frameon=False, ncols=min(tool_count, 3), loc="upper left")

    ymax = grouped["add_ms"].add(grouped["commit_ms"]).max() * 1.2
    ax.set_ylim(0, max(ymax, 1.0))

    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=dpi, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()

    if args.dpi <= 0:
        raise SystemExit("Error: --dpi must be a positive integer.")

    csv_files = []
    for dataset in DATASETS:
        for tool in TOOLS:
            csv_files.append((Path(f"{dataset}-{tool}-benchmark.csv"), dataset, tool))

    try:
        frames = [
            read_results(csv_path, dataset, tool)
            for csv_path, dataset, tool in csv_files
        ]
        summary = pd.concat(frames, ignore_index=True)
        plot(summary, args.output, args.title, args.dpi)
    except ValueError as exc:
        raise SystemExit(f"Error: {exc}") from exc

    print(f"Chart written to {args.output.resolve()}")


if __name__ == "__main__":
    main()
