"""Running benchmark tasks against a DSIExplorer agent (one task, one process at a time).

tool_utils keeps its DSI-call and timing logs in process-global lists, DSIExplorer changes
the process-wide cwd, and the tools swap sys.stdout -- so tasks are parallelized across
worker processes (see runner.py), never across threads within one process.
"""
from __future__ import annotations

import contextlib
import io
import json
import logging
import os
import re
import shutil
import sys
import threading
import time
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, List, Optional

from langchain_core.messages import AIMessage, ToolMessage

from dsi_explorer_agent import DSIExplorer
from tool_utils import (
    TimingCallbackHandler,
    get_dsi_call_log,
    get_timing_log,
    reset_dsi_call_log,
    reset_timing_log,
    timed,
)

from .config import AI_SEARCH_DIR, CONFIG, make_llm
from .core import BenchmarkTask, TaskResult, failed_result, new_result


class TaskTimeoutError(TimeoutError):
    """Raised when a single agent invocation exceeds its allotted timeout."""


# Set when an agent call was abandoned on timeout: its thread keeps running in this process
# and keeps writing to tool_utils' global logs, so the worker should be recycled.
abandoned_threads = 0


def safe_label(name: str) -> str:
    """Filename-safe version of a model name or task id."""
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name)


class BenchExplorer(DSIExplorer):
    """DSIExplorer with a caller-chosen workspace directory and explicit cleanup.

    The stock explorer names its workspace by the current minute (so concurrent runs would
    share and overwrite one folder) and never detaches its run.log handler from the shared
    logger (so every later explorer in the process also writes to every earlier log file).
    """

    def __init__(self, *args: Any, workspace_dir: str, **kwargs: Any) -> None:
        self._workspace_dir = os.path.abspath(workspace_dir)
        super().__init__(*args, **kwargs)

    def create_workspace(self) -> str:
        os.makedirs(self._workspace_dir, exist_ok=True)
        os.chdir(self._workspace_dir)
        return self._workspace_dir

    def close(self) -> None:
        log_file = os.path.abspath(os.path.join(self.wrsk_path, "run.log"))
        agent_logger = logging.getLogger("global_logger")
        for handler in list(agent_logger.handlers):
            if getattr(handler, "baseFilename", None) == log_file:
                agent_logger.removeHandler(handler)
                handler.close()
        with contextlib.suppress(Exception):
            self.checkpointer.conn.close()
        os.chdir(AI_SEARCH_DIR)


def make_explorer(llm=None, workspace_dir: Optional[str] = None) -> BenchExplorer:
    """Fresh explorer with the master DB loaded. Relative paths (MASTER_DB, run_path given to
    the agent) resolve against tools/ai_search exactly as in the original notebooks."""
    os.chdir(AI_SEARCH_DIR)
    if workspace_dir is None:
        workspace_dir = str(CONFIG.run_dir / "workspaces" / f"adhoc_{time.strftime('%H%M%S')}_{os.getpid()}")
    return BenchExplorer(
        llm=llm if llm is not None else make_llm(CONFIG.llm_model),
        db_index_name=CONFIG.master_db,
        output_mode="jupyter",
        workspace_dir=workspace_dir,
        **({"use_mcp": True, "mcp_url": CONFIG.mcp_url} if CONFIG.use_mcp else {}),
    )


def extract_tool_calls(result: dict) -> List[Dict[str, Any]]:
    calls = []
    for msg in result.get("messages", []):
        if isinstance(msg, AIMessage) and msg.tool_calls:
            for tc in msg.tool_calls:
                calls.append({"name": tc.get("name"), "args": tc.get("args", {})})
    return calls


def extract_tool_results(result: dict) -> List[str]:
    out = []
    for msg in result.get("messages", []):
        if isinstance(msg, ToolMessage):
            content = msg.content
            out.append(json.dumps(content, default=str) if isinstance(content, (list, dict)) else str(content))
    return out


def format_dsi_call_log(dsi_calls: List[Dict[str, Any]]) -> str:
    """Pretty-print recorded DSI Python API calls (method, args, kwargs, timing)."""
    if not dsi_calls:
        return "(no DSI API calls)"
    lines = []
    for i, call in enumerate(dsi_calls, 1):
        lines.append(
            f"[{i}] DSI.{call['method']}("
            f"{', '.join(call.get('args', []))}"
            f"{', ' if call.get('args') and call.get('kwargs') else ''}"
            f"{', '.join(f'{k}={v}' for k, v in call.get('kwargs', {}).items())})"
            f"  [{call.get('elapsed_sec', 0):.3f}s]"
            + (f"  ERROR: {call['error']}" if call.get("error") else "")
        )
    return "\n".join(lines)


def invoke_agent(explorer: DSIExplorer, query: str, timeout: Optional[float] = None, scope: str = "") -> dict:
    """Send one user query through the agent graph; raise TaskTimeoutError after `timeout`.

    The blocking invoke runs on a daemon thread so a stuck model can't hang the run. Python
    can't kill that thread, so on timeout it keeps running and `abandoned_threads` is bumped
    so the worker process gets recycled. Its stdout is buffered and replayed on the calling
    thread, since ipykernel misroutes output written from background threads.
    """
    global abandoned_threads
    start = time.time()
    msg = explorer.craft_message(query)
    timeout = CONFIG.task_timeout_sec if timeout is None else timeout
    graph_config = {
        "configurable": {"thread_id": explorer.thread_id},
        "callbacks": [TimingCallbackHandler(scope=scope)],
    }
    box: Dict[str, Any] = {}

    def _invoke() -> None:
        buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
                if explorer.use_mcp:
                    from dsi_explorer_agent import run_async
                    box["result"] = run_async(explorer.app.ainvoke(msg, config=graph_config))
                else:
                    box["result"] = explorer.app.invoke(msg, config=graph_config)
        except Exception as e:
            box["error"] = e
        finally:
            box["captured_output"] = buf.getvalue()

    worker = threading.Thread(target=_invoke, daemon=True)
    worker.start()
    worker.join(timeout=timeout)
    if worker.is_alive():
        abandoned_threads += 1
        raise TaskTimeoutError(f"Agent call exceeded {timeout}s timeout for query: {query[:80]!r}")

    if box.get("captured_output"):
        sys.stdout.write(box["captured_output"])
        sys.stdout.flush()
    if "error" in box:
        raise box["error"]
    result = box["result"]
    result["_elapsed"] = time.time() - start
    return result


def run_task(explorer: DSIExplorer, task: BenchmarkTask, *, model: str = "", live: bool = False) -> TaskResult:
    """Run one task (setup_query, then each turn in order on the same explorer) and capture
    response, tool calls, tokens, and a markdown transcript. The last turn's reply becomes
    result.response. With live=True the transcript is also displayed as it is produced
    (only useful when running in the notebook process, e.g. debug_task)."""
    if task.skip:
        return new_result(task, model, notes="skipped")

    transcript: List[str] = []

    def emit(md: str) -> None:
        transcript.append(md)
        if live:
            from IPython.display import Markdown, display
            display(Markdown(md))

    reset_dsi_call_log()
    reset_timing_log()  # score_task() reads the accumulated log, so it covers run + scoring

    if task.setup_query:
        with timed("setup_query"):
            invoke_agent(explorer, task.setup_query, timeout=task.timeout_sec, scope="setup_query")

    emit(f"### `{task.task_id}` — Level {task.level} / {task.category}\n\n**Query:** {task.query}")

    turns = task.turns
    result = new_result(task, model)
    for i, turn_query in enumerate(turns):
        if task.steps:
            emit(f"**Step {i + 1}/{len(turns)}:** {turn_query}")
        scope = f"step:{i + 1}"
        with timed(scope, query=turn_query[:60]):
            step = invoke_agent(explorer, turn_query, timeout=task.timeout_sec, scope=scope)
        result.response = (step.get("response") or "").strip()
        result.tool_calls.extend(extract_tool_calls(step))
        result.tool_results.extend(extract_tool_results(step))
        result.tokens += step.get("metadata", {}).get("token_usage", {}).get("total_tokens", 0)
        result.elapsed_sec += step["_elapsed"]
        emit(result.response)

    result.dsi_calls = get_dsi_call_log()
    emit(
        f"`Tools called: {[t['name'] for t in result.tool_calls]} | {result.elapsed_sec:.1f}s | {result.tokens} tokens`\n\n"
        f"```\n{format_dsi_call_log(result.dsi_calls)}\n```"
    )
    result.transcript = "\n\n".join(transcript)
    return result


def _archive_workspace(explorer: BenchExplorer, log_name: str) -> None:
    logs_dir = CONFIG.run_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    run_log = Path(explorer.wrsk_path, "run.log")
    if run_log.is_file():
        shutil.copyfile(run_log, logs_dir / f"{log_name}.log")
    shutil.rmtree(explorer.wrsk_path, ignore_errors=True)


def execute_tasks(tasks: List[BenchmarkTask], model: str, *, share_session: bool = False,
                  live: bool = False, on_result=None) -> List[TaskResult]:
    """Run + score tasks for one model, fully isolated (fresh explorer per task) unless
    share_session=True, in which case all tasks run in order on one explorer (the original
    notebooks' Level 2/3 behavior). Errors and timeouts become failed TaskResults."""
    from .scoring import score_task

    results: List[TaskResult] = []
    llm = make_llm(model)
    shared: Optional[BenchExplorer] = None
    model_dir = CONFIG.run_dir / "workspaces" / safe_label(model)

    try:
        for task in tasks:
            started = time.time()
            if task.requires_network and CONFIG.skip_network:
                task = replace(task, skip=True)
            explorer = None
            try:
                if share_session:
                    if shared is None:
                        shared = make_explorer(llm, str(model_dir / "shared_session"))
                    explorer = shared
                else:
                    explorer = make_explorer(llm, str(model_dir / safe_label(task.task_id)))
                tr = run_task(explorer, task, model=model, live=live)
                if tr.notes != "skipped":
                    tr = score_task(task, tr)
            except TaskTimeoutError as e:
                turn_timeout = task.timeout_sec if task.timeout_sec is not None else CONFIG.task_timeout_sec
                tr = failed_result(task, model, f"timeout: {e}", elapsed_sec=turn_timeout)
                tr.timing_log = get_timing_log()
            except Exception as e:
                tr = failed_result(task, model, f"error: {e!r}")
            finally:
                if explorer is not None and not share_session:
                    explorer.close()
                    if CONFIG.cleanup_workspaces:
                        _archive_workspace(explorer, f"{safe_label(model)}__{safe_label(task.task_id)}")
            tr.model = model
            tr.wall_sec = round(time.time() - started, 2)
            results.append(tr)
            if on_result is not None:
                on_result(tr)
    finally:
        if shared is not None:
            shared.close()
            if CONFIG.cleanup_workspaces:
                _archive_workspace(shared, f"{safe_label(model)}__shared_session")
    return results
