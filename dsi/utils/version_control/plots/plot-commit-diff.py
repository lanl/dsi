#!/usr/bin/env python3
"""Create a vertical matrix of per-run benchmark charts from CSV files."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


REQUIRED_COLUMNS = {"run", "total_ms", "space_gb"}
DATASETS = ["small", "medium", "large"]
TOOLS = ["rsnapshot", "gitlfs", "dsivcs"]
TOOL_LABELS = {
    "rsnapshot": "rsnapshot",
    "gitlfs": "Git LFS",
    "dsivcs": "DSI VCS",
}
COLORS = ['#D81B60', '#1E88E5', '#43A047', '#FFC107']  # Custom colors for each tool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot total_ms for each benchmark tool across runs for each dataset."
    )
    parser.add_argument(
        "--output",
        default="versioning-benchmark-runs-time.png",
        help="Output image path for the time-based chart.",
    )
    parser.add_argument(
        "--space-output",
        default="versioning-benchmark-runs-space.png",
        help="Output image path for the storage-based chart.",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=300,
        help="Image resolution (default: 300 DPI).",
    )
    parser.add_argument(
        "--title",
        default="Versioning Benchmark by Run",
        help="Chart title.",
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

    data["run"] = pd.to_numeric(data["run"], errors="coerce")
    data["total_ms"] = pd.to_numeric(data["total_ms"], errors="coerce")
    data["space_gb"] = pd.to_numeric(data["space_gb"], errors="coerce")

    if data["run"].isna().any():
        raise ValueError(f"{csv_path} contains a non-numeric run value")
    if data["total_ms"].isna().any():
        raise ValueError(f"{csv_path} contains a non-numeric total_ms value")
    if data["space_gb"].isna().any():
        raise ValueError(f"{csv_path} contains a non-numeric space_gb value")
    if (data["run"] < 0).any():
        raise ValueError(f"{csv_path} contains a negative run value")
    if (data["total_ms"] < 0).any():
        raise ValueError(f"{csv_path} contains a negative total_ms value")
    if (data["space_gb"] < 0).any():
        raise ValueError(f"{csv_path} contains a negative space_gb value")

    result = (
        data[["run", "total_ms", "space_gb"]]
        .groupby("run", as_index=False)
        .mean()
        .sort_values("run")
    )
    result["dataset"] = dataset
    result["tool"] = tool
    return result


def plot_metric(results: pd.DataFrame, output: Path, title: str, dpi: int, metric: str) -> None:
    if results.empty:
        raise ValueError("No benchmark data available to plot.")

    fig, axes = plt.subplots(
        nrows=len(DATASETS),
        ncols=1,
        figsize=(12, 3.5 * len(DATASETS)),
        sharex=True,
        constrained_layout=True,
    )
    axes = axes.flatten()

    bar_width = 0.25
    dataset_max_values = {
        "total_ms": {"small": 10, "medium": 50, "large": 200},
        "space_gb": {"small": 5, "medium": 50, "large": 200},
    }
    y_label = "Versioning Time (seconds)" if metric == "total_ms" else "Storage Usage (GB)"
    for axis_index, dataset in enumerate(DATASETS):
        ax = axes[axis_index]
        dataset_results = results[results["dataset"] == dataset].copy()
        if dataset_results.empty:
            raise ValueError(f"No data for dataset {dataset}")

        pivot = (
            dataset_results.pivot_table(
                index="run",
                columns="tool",
                values=metric,
                aggfunc="mean",
            )
            .reindex(columns=TOOLS)
            .sort_index()
        )

        x_positions = list(range(len(pivot.index)))
        for tool_index, tool in enumerate(TOOLS):
            values = pivot[tool].to_numpy()
            if metric == "total_ms":
                values = values / 1000.0  # Convert milliseconds to seconds
            offsets = [pos + (tool_index - 1) * bar_width for pos in x_positions]
            ax.bar(
                offsets,
                values,
                width=bar_width,
                label=TOOL_LABELS[tool],
                color=COLORS[tool_index],
                alpha=0.9,
            )

        ax.text(
            0.5,
            0.98,
            dataset.capitalize(),
            transform=ax.transAxes,
            fontsize=14,
            fontweight="bold",
            va="top",
            ha="center",
        )
        ax.set_ylabel(y_label, fontsize=16)
        ax.set_xticks([pos + bar_width / 2 for pos in x_positions])
        ax.set_xticklabels([str(int(run)) for run in pivot.index], rotation=0, fontsize=14)
        ax.grid(axis="y", linestyle="--", alpha=0.35)
        ax.set_axisbelow(True)
        ax.set_yscale("log")
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f"{y:g}"))
        ymax = dataset_max_values[metric].get(dataset, max(1.0, float(dataset_results[metric].max() * 1.15)))
        ymin = 0.1
        ax.yaxis.set_minor_formatter(plt.FuncFormatter(lambda y, _: ""))
        if metric == "total_ms":
            if dataset == "small":
                til = [0.1, 0.2, 0.3, 0.4, 0.6, 1, 2, 3, 4, 6]
                ymax = 6
            elif dataset == "medium":
                til = [0.1, 0.2, 0.3, 0.5, 0.7, 1, 2, 3, 5, 7, 10, 20, 30]
                ymax = 30
            else:
                til = [1, 2, 3, 4, 6, 8, 10, 20, 30, 40, 60, 80, 100, 140, 200]
                ymin = 10
                ymax = 200
        else:
            if dataset == "small":
                til = [0.1, 0.2, 0.3, 0.4, 0.6, 0.8, 1.0, 1.5, 2.0, 3.0, 4.0]
                ymax = 4
            elif dataset == "medium":
                til = [1, 2, 3, 4, 5, 6, 8, 10, 12, 16, 20]
                ymin = 1
                ymax = 20
            else:
                til = [10, 20, 30, 40, 60, 80, 100, 150, 200, 300, 400]
                ymin = 10
                ymax = 400

        ax.set_yticks(til)
        ax.set_yticklabels(list(map(str, til)), fontsize=14)
        ax.set_ylim(ymin, ymax)

    axes[-1].set_xlabel("Commit Iteration", fontsize=16)
    handles, labels = axes[0].get_legend_handles_labels()
    if metric == "total_ms":
        title = "Versioning Time Comparision per Iteration"
        fig.legend(
            handles,
            labels,
            loc="lower right",
            frameon=True,
            bbox_to_anchor=(0.99, 0.23),
            fontsize=14,
        )
    else:
        title = "Versioning Space Comparision per Iteration"
        fig.legend(
            handles,
            labels,
            loc="upper left",
            frameon=True,
            bbox_to_anchor=(0.06, 0.97),
            fontsize=14,
        )
    fig.suptitle(title, fontsize=16, fontweight="bold")

    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=dpi, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()

    if args.dpi <= 0:
        raise SystemExit("Error: --dpi must be a positive integer.")

    base_dir = Path(__file__).resolve().parent
    results_dir = base_dir / "results-diff"
    csv_files = []
    for dataset in DATASETS:
        for tool in TOOLS:
            csv_path = results_dir / f"{dataset}-{tool}-benchmark.csv"
            if not csv_path.exists():
                raise SystemExit(f"Error: missing benchmark file {csv_path}")
            csv_files.append((csv_path, dataset, tool))

    try:
        frames = [read_results(csv_path, dataset, tool) for csv_path, dataset, tool in csv_files]
        combined = pd.concat(frames, ignore_index=True)
        time_output = Path(args.output)
        if not time_output.is_absolute():
            time_output = base_dir / time_output
        space_output = Path(args.space_output)
        if not space_output.is_absolute():
            space_output = base_dir / space_output

        plot_metric(combined, time_output, args.title, args.dpi, "total_ms")
        plot_metric(combined, space_output, f"{args.title} (Storage Usage)", args.dpi, "space_gb")
        print(f"Time chart written to {time_output}")
        print(f"Storage chart written to {space_output}")
    except ValueError as exc:
        raise SystemExit(f"Error: {exc}") from exc


if __name__ == "__main__":
    main()
