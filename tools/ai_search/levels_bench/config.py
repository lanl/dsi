"""Benchmark configuration shared by the notebook process and its worker processes.

Worker processes receive a snapshot of CONFIG (see `snapshot()` / `configure()`), so any
change made in a notebook via `configure(...)` before a run is what the workers use.
"""
from __future__ import annotations

import os
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

DEFAULT_API_BASE = "https://aiportal-api.aws.lanl.gov"

POSSIBLE_MODELS = [
    "aws-gov.gpt-5.4",
    "anthropic.claude-sonnet-4-5-20250929-v1:0",
    # "amazon.nova-pro-v1:0",
    # "meta.llama3-70b-instruct-v1:0",
    # "meta.llama3-8b-instruct-v1:0",
    # "mistral7b",
    # "llama3bv32instr",
    # "phi4",
    # "gpt-oss-120b",
    # "e5mistral7b",
    # "NVIDIA-Nemotron-3-Super-120B-A12B-FP8",
    # "gemma-4-31B-it",
]


def find_ai_search_dir() -> Path:
    """Locate tools/ai_search from this file, the cwd, or any parent of the cwd."""
    here = Path(__file__).resolve().parent.parent
    if (here / "dsi_explorer_agent.py").is_file():
        return here
    for directory in (Path.cwd(), *Path.cwd().parents):
        if (directory / "dsi_explorer_agent.py").is_file():
            return directory.resolve()
        nested = directory / "tools" / "ai_search" / "dsi_explorer_agent.py"
        if nested.is_file():
            return nested.parent.resolve()
    raise RuntimeError("Could not find tools/ai_search; open the notebook from the dsi repo")


AI_SEARCH_DIR = find_ai_search_dir()


@dataclass
class BenchConfig:
    api_base: str = os.environ.get("API_BASE", DEFAULT_API_BASE)
    llm_model: str = POSSIBLE_MODELS[0]  # default model for debug_task / make_explorer
    models_to_run: List[str] = field(default_factory=lambda: list(POSSIBLE_MODELS))
    # Every judged score is the average across these models, so a model under test never
    # judges its own (or a rival's) transcript alone.
    judge_models: List[str] = field(default_factory=lambda: list(POSSIBLE_MODELS))
    task_timeout_sec: float = 180  # per agent turn
    use_mcp: bool = False  # True if levels_mcp_server.py is running
    mcp_url: str = "http://127.0.0.1:8000/mcp"
    master_db: str = "data/oceans_11/ocean_11_datasets.db"
    results_dir: str = str(AI_SEARCH_DIR / "levels_benchmark_results")
    run_id: str = field(default_factory=lambda: datetime.now().strftime("%Y%m%d_%H%M%S"))
    # Parallelism: number of worker processes running agent tasks concurrently. Bounded by
    # the AI portal's rate limits rather than local CPU, since tasks are network-bound.
    max_workers: int = 4
    parallel_judges: bool = True  # query every judge model concurrently
    # Each task gets its own workspace (downloads, plots, run.log). When True, the workspace
    # is deleted after the task and only run.log is kept under <run>/logs/.
    cleanup_workspaces: bool = True
    skip_network: bool = os.environ.get("LEVELS_SKIP_NETWORK", "0") == "1"
    # Extra seconds the parent allows per task (for scoring) before hard-killing a worker
    # whose agent ignored its per-turn timeout.
    hard_timeout_slack_sec: float = 300

    @property
    def run_dir(self) -> Path:
        return Path(self.results_dir) / self.run_id


CONFIG = BenchConfig()


def configure(**overrides: Any) -> BenchConfig:
    """Update CONFIG in place. Unknown keys raise, so typos don't silently do nothing."""
    for key, value in overrides.items():
        if not hasattr(CONFIG, key):
            raise AttributeError(f"Unknown benchmark config option: {key!r}")
        setattr(CONFIG, key, value)
    return CONFIG


def snapshot() -> Dict[str, Any]:
    return asdict(CONFIG)


def load_api_key(key_file: str = "portal_api_key.txt") -> None:
    """Load the portal API key into API_KEY (inherited by worker processes)."""
    key_path = AI_SEARCH_DIR / key_file
    if not key_path.is_file():
        raise FileNotFoundError(
            f"Missing {key_path}; copy portal_api_key.txt.example and add your key."
        )
    os.environ.setdefault("API_KEY", key_path.read_text().strip())
    os.environ["API_BASE"] = CONFIG.api_base


def init(load_key: bool = True, **overrides: Any) -> BenchConfig:
    """Notebook entry point: put tools/ai_search on sys.path and make it the cwd, load the
    API key, and apply config overrides."""
    if str(AI_SEARCH_DIR) not in sys.path:
        sys.path.insert(0, str(AI_SEARCH_DIR))
    os.chdir(AI_SEARCH_DIR)
    if load_key:
        load_api_key()
    cfg = configure(**overrides)
    print(f"Working directory: {AI_SEARCH_DIR}")
    print(f"Run id: {cfg.run_id} | models: {cfg.models_to_run} | judges: {cfg.judge_models}")
    print(f"Workers: {cfg.max_workers} | turn timeout: {cfg.task_timeout_sec}s | skip_network: {cfg.skip_network}")
    return cfg


_llm_cache: Dict[tuple, Any] = {}


def make_llm(model_name: str, temperature: float = 0.1):
    """ChatOpenAI client for the LANL AI portal, cached per process."""
    key = (model_name, temperature, CONFIG.api_base)
    if key not in _llm_cache:
        from langchain_openai import ChatOpenAI

        _llm_cache[key] = ChatOpenAI(
            model=model_name,
            api_key=os.environ["API_KEY"],
            base_url=CONFIG.api_base,
            temperature=temperature,
        )
    return _llm_cache[key]
