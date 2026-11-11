"""Level 3 (discovery) tasks: DiscoveryBench-style hypotheses scored facet by facet
against a hand-verified ground truth (score_level3).
"""
from ..core import BenchmarkTask

LEVEL3_TASKS = [
    BenchmarkTask(
        task_id="L3_contact_owner_theme_pattern",
        level=3, category="Analysis & Summarization",
        query=(
            "Analyze the Ocean 11 catalog's contact/ownership metadata. Is dataset ownership "
            "concentrated among a small number of contact people, and if so, do those people's "
            "datasets share a common theme? State your finding as a hypothesis with supporting "
            "evidence."
        ),
        ground_truth_hypothesis={
            "context": "Ocean 11 genesis_datacard catalog (9 datasets), the Contact_Point_ field",
            "variables": "Contact_Point_ (owner/point of contact), Theme/Category_:_Domain",
            "relationship": (
                "Ownership is concentrated: 'Nathan DeBardeleben' (with a likely misspelled "
                "variant 'Nathan Debardebelen' on the Poisson record) is the contact for "
                "roughly half the catalog (4-5 of 9 datasets: Bowtie, Gray-Scott, Heat "
                "Equations, Monopoly, and Poisson), but these datasets do NOT share one theme "
                "-- they span both 'manufacturing' (Bowtie, Monopoly) and 'physics' (Gray-Scott, "
                "Heat Equations, Poisson), so ownership concentration does not imply thematic "
                "concentration."
            ),
        },
        reference_path=[
            "read data/oceans_11/ocean_11_datasets.db",
            "query \"SELECT Contact_Point_, \\\"Theme/Category_:_Domain\\\" FROM genesis_datacard\"",
        ],
    ),
    BenchmarkTask(
        task_id="L3_nif_density_trend",
        level=3, category="Analysis & Summarization",
        query=(
            "Analyze how the NIF simulation's peak density (dens_max) changes across its 20 "
            "recorded timesteps. Characterize the trend and relate it to the physical process "
            "being simulated."
        ),
        ground_truth_hypothesis={
            "context": "nif_metadata table, 20 timesteps (0-19), dens_max column",
            "variables": "timestep, dens_max",
            "relationship": (
                "dens_max rises sharply from about 2.7 at timestep 0 to a peak of about 7.95 "
                "around timestep 6, then declines fairly steadily back down to about 2.6 by "
                "timestep 19 -- a rise-then-fall pattern consistent with compression followed "
                "by rebound/expansion in an implosion simulation, rather than a monotonic "
                "increase or a constant value."
            ),
        },
        reference_path=[
            "read data/oceans_11/nif.db",
            'query "SELECT timestep, dens_max FROM nif_metadata ORDER BY timestep"',
        ],
    ),
    BenchmarkTask(
        task_id="L3_nif_grid_refinement_trend",
        level=3, category="Analysis & Summarization",
        query=(
            "Analyze how the NIF simulation's computational grid (the first number in the "
            "'shape' field) changes across its 20 timesteps. Does the grid resolution stay "
            "fixed, grow, shrink, or fluctuate over the simulation?"
        ),
        ground_truth_hypothesis={
            "context": "nif_metadata table, 20 timesteps (0-19), shape column (first dimension = active zone/cell count)",
            "variables": "timestep, shape (zone count)",
            "relationship": (
                "The zone count is not fixed: it grows overall from 6360 at timestep 0 to 9848 "
                "at timestep 19 (roughly a 55% increase), with some local dips along the way "
                "(e.g. around timesteps 4 and 6-7) rather than perfectly monotonic growth -- "
                "consistent with adaptive mesh refinement tracking the evolving simulation."
            ),
        },
        reference_path=[
            "read data/oceans_11/nif.db",
            'query "SELECT timestep, shape FROM nif_metadata ORDER BY timestep"',
        ],
    ),
]
