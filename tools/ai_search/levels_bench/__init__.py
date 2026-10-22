"""LEVELS benchmark harness for the DSI Data Explorer agent.

Notebook usage:

    from levels_bench import init, run_benchmark, results_to_dataframe, reports
    from levels_bench.tasks import LEVEL1_TASKS

    init(max_workers=4)
    results = run_benchmark(LEVEL1_TASKS, "level1_atomic")

Agent/LLM modules (agent, scoring) are imported lazily inside the workers, so importing
this package only needs pandas and IPython.
"""
from .config import CONFIG, POSSIBLE_MODELS, configure, init, make_llm
from .core import BenchmarkTask, TaskResult, expand_prompt_variants
from .results import (
    flatten,
    load_partial,
    load_results,
    results_to_dataframe,
    save_results,
    timing_breakdown_dataframe,
)
from .runner import debug_task, run_benchmark
from . import reports
