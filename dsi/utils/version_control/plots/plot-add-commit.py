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

DATASETS = ["small", "medium", "large", "big"]
TOOLS = ["rsnapshot", "gitlfs", "dsivcs"]
PRETTY_TOOL_NAMES = ["rsnapshot", "Git LFS", "DSI VCS"]

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read one or more benchmark CSV files and plot mean Git add and "
            "commit times as stacked bars."
        )
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
    # for column in ("total_ms",):
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
            total_ms_std=("total_ms", "std"),
            space_gb=("space_gb", "mean"),
            space_gb_std=("space_gb", "std"),
            runs=("run", "count"),
        )
    )
    summary[["add_ms", "commit_ms", "total_ms", "total_ms_std"]] /= 1000.0
    summary["total_ms_std"] = summary["total_ms_std"].fillna(0.0)
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


def plot(summary: pd.DataFrame, output: Path, title: str, dpi: int, ispace: str = "time") -> None:
    dataset_order = DATASETS
    tool_order = TOOLS
    if ispace != "time":
        tool_order.insert(0, "base")
        PRETTY_TOOL_NAMES.insert(0, "Base")
        summary = pd.concat([
            pd.DataFrame(
                {
                    "dataset": dataset_order,
                    "tool": ["base"] * len(dataset_order),
                    "add_ms": [0.0] * len(dataset_order),
                    "commit_ms": [0.0] * len(dataset_order),
                    "total_ms": [0.0] * len(dataset_order),
                    "total_ms_std": [0.0] * len(dataset_order),
                    "space_gb": [0.384800, 2.436905, 45.911644, 322],
                    "space_gb_std": [0.0] * len(dataset_order),
                }
            ),
            summary,
        ], ignore_index=True)
    grouped = summary.groupby(["dataset", "tool"], sort=False).agg(
        add_ms=("add_ms", "mean"),
        commit_ms=("commit_ms", "mean"),
        total_ms=("total_ms", "mean"),
        total_ms_std=("total_ms_std", "mean"),
        space_gb=("space_gb", "mean"),
        space_gb_std=("space_gb_std", "mean"),
    ).reset_index()

    if grouped.empty:
        raise ValueError("No benchmark data available to plot.")

    group_count = len(dataset_order)
    tool_count = len(tool_order)
    bar_width = 0.8 / tool_count

    figure_width = max(8.0, 1.0 + group_count * 1.4)
    fig, ax = plt.subplots(figsize=(figure_width, 5.2), constrained_layout=True)

    min_space_by_dataset = grouped.groupby("dataset")["space_gb"].min()
    print("Minimum space by dataset:", min_space_by_dataset)
    x_positions = list(range(group_count))
    handles = []
    # colors = plt.get_cmap("tab10")(range(tool_count))
    colors = ['#FFC107', '#D81B60', '#1E88E5', '#43A047']  # Custom colors for each tool
    if ispace == "time":
        colors = ['#D81B60', '#1E88E5', '#43A047']  # Custom colors for each tool
    for tool_index, tool in enumerate(tool_order):
        tool_data = grouped[grouped["tool"] == tool].set_index("dataset")
        print(tool_data)
        # add_values = [tool_data.at[dataset, "add_ms"] if dataset in tool_data.index else 0.0 for dataset in dataset_order]
        # commit_values = [tool_data.at[dataset, "commit_ms"] if dataset in tool_data.index else 0.0 for dataset in dataset_order]
        total_values = [tool_data.at[dataset, "total_ms"] if dataset in tool_data.index else 0.0 for dataset in dataset_order]
        if ispace == "space":
            total_values = [tool_data.at[dataset, "space_gb"] if dataset in tool_data.index else 0.0 for dataset in dataset_order]
        elif ispace == "space-percent-change":
            total_values = [
                (tool_data.at[dataset, "space_gb"] - min_space_by_dataset.get(dataset, 0.0)) / min_space_by_dataset.get(dataset, 1.0) * 100
                if dataset in tool_data.index else 0.0
                for dataset in dataset_order
            ]
            print(total_values)
        positions = [x + (tool_index - (tool_count - 1) / 2) * bar_width for x in x_positions]
        # add_bars = ax.bar(
        #     positions,
        #     add_values,
        #     width=bar_width,
        #     label=f"{tool} add",
        #     color=colors[tool_index],
        # )
        # commit_bars = ax.bar(
        #     positions,
        #     commit_values,
        #     width=bar_width,
        #     bottom=add_values,
        #     label=f"{tool} commit",
        #     color=colors[tool_index],
        #     alpha=0.6,
        # )
        # handles.append((add_bars, commit_bars))

        # add_value_labels(ax, add_bars, add_values)
        # add_value_labels(ax, commit_bars, commit_values)
        # total_std = [
        #     tool_data.at[dataset, "total_ms_std"] if dataset in tool_data.index else 0.0
        #     for dataset in dataset_order
        # ]
        # if ispace == "space":
        #     total_std = [
        #         tool_data.at[dataset, "space_gb_std"] if dataset in tool_data.index else 0.0
        #         for dataset in dataset_order
        #     ]
        total_bars = ax.bar(
            positions,
            total_values,
            width=bar_width,
            label=f"{tool} total",
            color=colors[tool_index],
            # yerr=total_std,
            # capsize=4,
            # error_kw={"elinewidth": 1.5, "alpha": 0.85},
        )
        handles.append((total_bars,))

    ax.set_title(title, pad=10, fontweight="bold",fontsize=16)
    if ispace == "space":
        ax.set_ylabel("Space Used (GB)", fontsize=14)
    elif ispace == "space-percent-change":
        ax.set_ylabel("Space Percent Increase from Minimum (%)", fontsize=14)
    else:
        ax.set_ylabel("Mean Versioning Time (seconds)", fontsize=14)
    ax.set_xlabel("Dataset Size", fontsize=14)
    ax.set_xticks(x_positions)
    ax.set_xticklabels([i.capitalize() for i in dataset_order])
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    ax.set_axisbelow(True)
    ax.set_yscale("log")

    legend_entries = []
    for tool_index, tool in enumerate(tool_order):
        legend_entries.append(plt.Line2D([0], [0], 
                                         color=colors[tool_index], 
                                         marker="s", 
                                         linestyle="", 
                                         label=PRETTY_TOOL_NAMES[tool_index]))
    ax.legend(legend_entries, PRETTY_TOOL_NAMES, 
              fontsize=14, 
              markerscale=2.0,
              frameon=True, 
              ncols=min(tool_count, 1), 
              loc="upper left" if ispace != "space-percent-change" else "upper right")

    ymax = max(grouped["total_ms"].max(), 512)
    if ispace == "space":
        ymax = max(grouped["space_gb"].max(), 64)
    elif ispace == "space-percent-change":
        ymax = 20
    ymin = 1
    ax.set_ylim(ymin, ymax)
    if ispace == "space":
        ax.set_yticks([0.1, 0.25, 0.5, 1, 2, 4, 8, 16, 32, 64])
        ax.set_yticklabels(["0", "0.25", "0.5", "1", "2", "4", "8", "16", "32", "64"])
    elif ispace == "space-percent-change":
        ax.set_yticks([0.01, 0.02, 0.03, 0.05, 0.1, 0.2, 0.3, 0.5, 1, 2, 4, 8, 16, 32, 64])
        ax.set_yticklabels(["0", "0.02", "0.03", "0.05", "0.1", "0.2", "0.3", "0.5", "1", "2", "4", "8", "16", "32", "64"])
    else:
        ax.set_yticks([1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 1500])
        ax.set_yticklabels(["1", "2", "4", "8", "16", "32", "64", "128", "256", "512", "1024", "1500"])
    ax.tick_params(axis="x", labelsize=14)
    ax.tick_params(axis="y", labelsize=14)
    # plt.tight_layout()
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
            csv_files.append((Path(f"results-init/{dataset}-{tool}-benchmark.csv"), dataset, tool))

    try:
        frames = [
            read_results(csv_path, dataset, tool)
            for csv_path, dataset, tool in csv_files
        ]
        summary = pd.concat(frames, ignore_index=True)
        output = "versioning-benchmark-time.png"
        title = "Versioning Time Comparison"
        isspace = args.title.lower()
        if isspace == "space":
            output = "versioning-benchmark-space.png"
            title = "Versioning Storage Comparison"
        elif isspace == "space-percent-change":
            output = "versioning-benchmark-space-percent-change.png"
            title = "Versioning Storage Percent Change Comparison"
        plot(summary, Path(output), title, args.dpi, isspace)
        print(f"Chart written to {output}")
    except ValueError as exc:
        raise SystemExit(f"Error: {exc}") from exc


if __name__ == "__main__":
    main()
