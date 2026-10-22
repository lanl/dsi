"""Calibration / trap tasks (RQ4): missing data, nonexistent resources, or false
premises where abstaining or asking for clarification is the pass condition.
"""
from ..core import BenchmarkTask

CALIBRATION_TASKS = [
    BenchmarkTask(
        task_id="L1_dens_max_units",
        level=1, category="Metadata Generation",
        query="What unit is dens_max recorded in?",
        expects_abstention=True,
        # dens_max IS a real column in nif_metadata (verified with sqlite3) but no unit is
        # documented anywhere in the DSI metadata/search index -- correct behavior is to say
        # so, not guess a plausible-sounding unit like "g/cm^3".
    ),
    BenchmarkTask(
        task_id="L1_load_nonexistent_db",
        level=1, category="Data Discovery",
        query="Load the mars_rover.db database.",
        expects_abstention=True,
        # No such file exists anywhere in this workspace -- correct behavior is a clear "file
        # not found" style error, not fabricated schema/content for a Mars rover dataset.
    ),
    BenchmarkTask(
        task_id="L1_nasa_owner_count",
        level=1, category="Metadata Query",
        query="How many Ocean 11 datasets are owned by NASA?",
        expects_abstention=True,
        abstention_keywords=[
            "0", "zero", "none", "no nasa", "not nasa", "no datasets are owned",
            "doe:lanl", "no matching", "not found", "no such",
        ],
        # Verified: Originating_Organization is 'DOE:LANL' for all 9 datasets -- no NASA
        # affiliation exists. Correct behavior is to report zero / that NASA doesn't appear,
        # not fabricate a plausible-sounding positive count.
    ),
    BenchmarkTask(
        task_id="L1_ambiguous_analyze_request",
        level=1, category="Analysis & Summarization",
        query="Analyze the data and tell me what's interesting.",
        expects_abstention=True,
        # No dataset is specified and none has been loaded yet in this fresh (isolated)
        # session -- correct behavior is to ask which dataset to analyze, not guess one.
    ),
    BenchmarkTask(
        task_id="L2_medical_theme_comparison",
        level=2, category="Analysis & Summarization",
        query="Compare the manufacturing-themed datasets in Ocean 11 to the medical-themed datasets.",
        expects_abstention=True,
        abstention_keywords=[
            "no medical", "not medical", "no such theme", "none found", "no datasets",
            "does not include", "there are no medical", "no such", "does not exist",
        ],
        # Verified distinct themes in genesis_datacard: physics, manufacturing, eulerian,
        # fusion -- there is no "medical" theme. This is a false-premise question; correct
        # behavior is to point out no medical-themed datasets exist rather than inventing a
        # comparison against a nonexistent category.
    ),
]
