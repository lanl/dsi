"""Level 1 (atomic) tasks: a single DSI/API call from natural language, scored by
binary checks with partial credit (score_level1).
"""
from ..core import BenchmarkTask

# Level 1 — Atomic (single tool call, binary pass/fail + quantitative checks)
# Load data, store data, query data, extract data, search data, download data
#
# Each task's `reference_path` is the most-efficient sequence of DSI CLI commands a human
# would run to answer the same query. score_efficiency() LLM-judges the agent's actual
# tool_calls/dsi_calls against this reference to produce a judge_efficiency_score,
# independent of the binary correctness score from score_level1.

LEVEL1_TASKS = [
    BenchmarkTask(
        task_id="L1_load_ocean11",
        level=1, category="Data Access",
        query="Load the ocean_11_datasets.db database.",
        prompt_variants={
            "novice": "Hey, can you open up that ocean_11_datasets.db file for me? I just want to see what's in it.",
            "expert": "Instantiate a DSI load operation against the ocean_11_datasets.db backend so its schema/context is available for subsequent queries.",
            "terse": "load ocean_11_datasets.db",
            "verbose": "I'm working on a project that requires me to explore several scientific datasets, and the first one I need to look at is stored in a file called ocean_11_datasets.db -- could you please go ahead and load that database for me so that I can start examining its contents?",
            "casual": "yo can u load up ocean_11_datasets.db real quick, thx",
        },
        expected_tools=["load_dsi_tool"],
        expected_path= "oceans_11/ocean_11_datasets.db",
        expected_args={"path": "oceans_11/ocean_11_datasets.db"},
        reference_path=["read /Users/hhn/lanl/dsi/tools/ai_search/oceans11.db"],
    ),
    BenchmarkTask(
        task_id="L1_load_nif_direct",
        level=1, category="Data Access",
        query="Load the nif.db database.",
        prompt_variants={
            "novice": "Can you open the nif.db file so I can look at the NIF data?",
            "expert": "Perform a DSI load against the nif.db backend to bring its schema into context.",
            "terse": "load nif.db",
            "verbose": "For the next part of my analysis I specifically need access to the National Ignition Facility simulation data, which I believe is stored in a database file named nif.db -- would you be able to load that one for me now?",
            "casual": "can u pull up nif.db for me",
        },
        expected_tools=["load_dsi_tool"],
        expected_path="oceans_11/nif.db",
        expected_args={"path": "oceans_11/nif.db"},
        reference_path=["read data/oceans_11/nif.db"],
    ),
    BenchmarkTask(
        task_id="L1_load_poisson",
        level=1, category="Data Access",
        query="Load the poisson.db database.",
        prompt_variants={
            "novice": "Please open the poisson.db file.",
            "expert": "Execute a load operation on the poisson.db backend.",
            "terse": "load poisson.db",
            "verbose": "I'd like to begin looking at the electrostatics simulation results, which I understand live in a database called poisson.db -- could you load that database so I can start digging into it?",
            "casual": "load poisson.db pls",
        },
        expected_tools=["load_dsi_tool"],
        expected_path="oceans_11/poisson.db",
        expected_args={"path": "oceans_11/poisson.db"},
        reference_path=["read data/oceans_11/poisson.db"],
    ),
    BenchmarkTask(
        task_id="L1_search_implosion",
        level=1, category="Data Discovery",
        query="Do you have any implosion data in oceans 11 database?",
        prompt_variants={
            "novice": "I'm looking for anything about things imploding, like in a physics experiment. Got anything like that in the oceans 11 database?",
            "expert": "Query the Ocean 11 catalog for any dataset whose metadata indicates implosion-driven physics (e.g. inertial confinement fusion).",
            "terse": "implosion data in oceans11?",
            "verbose": "As part of a broader survey of high energy density physics datasets, I'm trying to determine whether the Ocean 11 catalog contains any resource related to implosion phenomena -- for instance, simulations of capsule or target implosions -- could you check and let me know what you find?",
            "casual": "yo is there any implosion stuff in the oceans 11 db",
        },
        expected_tools=["query_dsi_tool"],
        response_keywords=["National Ignition", "NIF", "FLASH"],
        forbidden_keywords=["Bowtie", "Deep Water Impact"],
        reference_path=[
            "read /Users/hhn/lanl/dsi/tools/ai_search/oceans11.db",
            "search oceans11 implosion",
        ],
    ),
    BenchmarkTask(
        task_id="L1_download_nif_sim",
        level=1, category="Data Retrieval",
        query="Download the simulation data for sim name: nifcylxyz_hdf5_plt_cnt_0000.flash",
        prompt_variants={
            "novice": "Can you grab the data file for the simulation called nifcylxyz_hdf5_plt_cnt_0000.flash?",
            "expert": "Resolve and retrieve the dataset artifact corresponding to sim_name='nifcylxyz_hdf5_plt_cnt_0000.flash' via download_file_tool.",
            "terse": "download nifcylxyz_hdf5_plt_cnt_0000.flash",
            "verbose": "I need to get my hands on the actual simulation output for one specific run -- the one identified by the simulation name nifcylxyz_hdf5_plt_cnt_0000.flash -- so could you please locate and download that particular file for me?",
            "casual": "can u download the file for nifcylxyz_hdf5_plt_cnt_0000.flash",
        },
        expected_tools=["download_file_tool"],
        expected_args={"url": "https://oceans11.lanl.gov/nif/N210201-001_3D_r2//nifcylxyz_hdf5_plt_cnt_0000.flash"},
        requires_network=True,
        reference_path=[
            "read data/oceans_11/ocean_11_datasets.db",
            "query \"SELECT Title, dsi_database FROM genesis_datacard WHERE Title LIKE '%NIF%' OR Title LIKE '%FLASH%'\"",
            "read data/oceans_11/nif.db",
            "query \"SELECT sim_name, link FROM nif_metadata WHERE sim_name LIKE '%nifcylxyz_hdf5_plt_cnt_0000%'\"",
            "pull_data url nif_sim https://oceans11.lanl.gov/nif/N210201-001_3D_r2//nifcylxyz_hdf5_plt_cnt_0000.flash",
        ],
    ),
    BenchmarkTask(
        task_id="L1_store_poisson_hdf5",
        level=1, category="Data Retrieval",
        query="Download this simulation data file: https://oceans11.lanl.gov/electrostaticEquations/LA-UR-25-29606-electrostatic_dataset_1k/data/electrostatic_poisson_32x32_1-1000.hdf5",
        prompt_variants={
            "novice": "Can you download this file for me? https://oceans11.lanl.gov/electrostaticEquations/LA-UR-25-29606-electrostatic_dataset_1k/data/electrostatic_poisson_32x32_1-1000.hdf5",
            "expert": "Issue a retrieval of the HDF5 artifact at https://oceans11.lanl.gov/electrostaticEquations/LA-UR-25-29606-electrostatic_dataset_1k/data/electrostatic_poisson_32x32_1-1000.hdf5",
            "terse": "download https://oceans11.lanl.gov/electrostaticEquations/LA-UR-25-29606-electrostatic_dataset_1k/data/electrostatic_poisson_32x32_1-1000.hdf5",
            "verbose": "I came across this link to what I believe is an HDF5 simulation data file and I'd like to have a local copy of it to work with -- the URL is https://oceans11.lanl.gov/electrostaticEquations/LA-UR-25-29606-electrostatic_dataset_1k/data/electrostatic_poisson_32x32_1-1000.hdf5 -- could you download it for me?",
            "casual": "can u dl this real quick https://oceans11.lanl.gov/electrostaticEquations/LA-UR-25-29606-electrostatic_dataset_1k/data/electrostatic_poisson_32x32_1-1000.hdf5",
        },
        expected_tools=["download_file_tool"],
        expected_args={"url": "https://oceans11.lanl.gov/electrostaticEquations/LA-UR-25-29606-electrostatic_dataset_1k/data/electrostatic_poisson_32x32_1-1000.hdf5"},
        requires_network=True,
        reference_path=[
            "pull_data url poisson_hdf5 https://oceans11.lanl.gov/electrostaticEquations/LA-UR-25-29606-electrostatic_dataset_1k/data/electrostatic_poisson_32x32_1-1000.hdf5",
        ],
    ),
    BenchmarkTask(
        task_id="L1_owner_field",
        level=1, category="Metadata Query",
        query=f'Who is the owner of the NIF dataset"',
        prompt_variants={
            "novice": "Who owns the NIF dataset? Like whose data is it?",
            "expert": "Identify the designated point-of-contact/owner of record for the NIF dataset per its catalog metadata.",
            "terse": "NIF dataset owner?",
            "verbose": "I need to know, for attribution and follow-up purposes, exactly who the responsible point of contact or owner is for the NIF dataset that's cataloged in this system -- could you tell me who that is?",
            "casual": "who owns the nif dataset lol",
        },
        expected_tools=["query_dsi_tool"],
        response_keywords=["Joshua", "Paul", "Sauppe"],
        reference_path=[
            "read data/oceans_11/ocean_11_datasets.db",
            "query \"SELECT Title, Contact_Point_, Email FROM genesis_datacard WHERE Title LIKE '%NIF%' OR Title LIKE '%FLASH%'\"",
        ],
    ),
    BenchmarkTask(
        task_id="L1_extract_dataset_count",
        level=1, category="Data Exploration",
        query="How many datasets are in the Ocean 11 catalog?",
        prompt_variants={
            "novice": "How many datasets does the Ocean 11 catalog have?",
            "expert": "Return the cardinality of the dataset collection in the Ocean 11 catalog (i.e. row count of the genesis_datacard table).",
            "terse": "# datasets in oceans11?",
            "verbose": "For a summary report I'm putting together, I need to state precisely how many individual datasets are currently cataloged within the Ocean 11 system -- could you tell me that total count?",
            "casual": "how many datasets we got in oceans 11",
        },
        expected_tools=["query_dsi_tool"],
        expected_count=9,
        reference_path=[
            "read data/oceans_11/ocean_11_datasets.db",
            'query "SELECT COUNT(*) FROM genesis_datacard"',
        ],
    ),
    BenchmarkTask(
        task_id="L1_extract_originating_org",
        level=1, category="Metadata Query",
        query="What organization is listed for the Ocean 11 datasets?",
        prompt_variants={
            "novice": "Which organization made the Ocean 11 datasets?",
            "expert": "Report the Originating_Organization field value(s) associated with the Ocean 11 catalog records.",
            "terse": "oceans11 org?",
            "verbose": "I'm trying to confirm, for a compliance note, which organization is officially listed as the originating body responsible for the datasets held in the Ocean 11 catalog -- can you tell me what that organization is?",
            "casual": "who's org is behind oceans 11 datasets",
        },
        expected_tools=["query_dsi_tool"],
        response_keywords=["DOE", "LANL"],
        reference_path=[
            "read data/oceans_11/ocean_11_datasets.db",
            'query "SELECT DISTINCT Originating_Organization FROM genesis_datacard"',
        ],
    ),
    BenchmarkTask(
        task_id="L1_extract_physics_theme_count",
        level=1, category="Metadata Query",
        query="How many datasets are categorized as physics?",
        prompt_variants={
            "novice": "How many of the datasets are about physics?",
            "expert": "Count the number of catalog records whose Theme/Category field equals 'physics'.",
            "terse": "# physics datasets?",
            "verbose": "I'm trying to narrow my search to just the physics-related holdings in this catalog, so as a first step could you tell me exactly how many datasets are categorized under the physics theme?",
            "casual": "how many of these are physics ones",
        },
        expected_tools=["query_dsi_tool"],
        expected_count=5,
        reference_path=[
            "read data/oceans_11/ocean_11_datasets.db",
            "query \"SELECT COUNT(*) FROM genesis_datacard WHERE LOWER(\\\"Theme/Category_:_Domain\\\") = 'physics'\"",
        ],
    ),
    BenchmarkTask(
        task_id="L1_nif_domain_extent",
        level=1, category="Data Exploration",
        query="What are the spatial bounds of the NIF simulation's computational domain?",
        prompt_variants={
            "novice": "How big is the area that the NIF simulation covers, like the physical size of the domain?",
            "expert": "Report the spatial extent (bounding dimensions) of the computational domain used in the NIF simulation.",
            "terse": "NIF domain size?",
            "verbose": "I need to understand the physical scale of the simulated region for the NIF run -- specifically, what are the spatial bounds or dimensions of its computational domain -- so I can compare it against another simulation's setup?",
            "casual": "how big is the nif sim domain",
        },
        expected_tools=["query_dsi_tool"],
        response_keywords=["3.072"],
        reference_path=[
            "read data/oceans_11/ocean_11_datasets.db",
            "search computational domain",
        ],
    ),
    BenchmarkTask(
        task_id="L1_nif_timestep_count",
        level=1, category="Data Exploration",
        query="How many discrete timesteps of the NIF simulation are recorded in NIF.",
        prompt_variants={
            "novice": "How many time steps does the NIF simulation have saved?",
            "expert": "Report the count of distinct timestep values recorded in the NIF simulation metadata.",
            "terse": "# NIF timesteps?",
            "verbose": "For my timeline analysis, I need to know exactly how many distinct timesteps were recorded over the course of the NIF simulation -- could you look that up for me?",
            "casual": "how many timesteps we got for nif",
        },
        expected_tools=["query_dsi_tool"],
        expected_count=20,
        reference_path=[
            "read data/oceans_11/nif.db",
            'query "SELECT COUNT(DISTINCT timestep) FROM nif_metadata"',
        ],
    ),
    BenchmarkTask(
        task_id="L1_nif_array_field_count",
        level=1, category="Data Exploration",
        query="How many distinct physical quantity arrays are recorded per simulation timestep in NIF.",
        prompt_variants={
            "novice": "How many different measurements does NIF record at each timestep?",
            "expert": "Report the number of distinct physical-quantity arrays captured per timestep in the NIF simulation (per array_and_types).",
            "terse": "# arrays per NIF timestep?",
            "verbose": "I'm trying to understand the breadth of the NIF dataset's physical measurements -- specifically, at each individual timestep, how many distinct physical quantity arrays (e.g. density, pressure, etc.) are being recorded?",
            "casual": "how many kinds of arrays does nif track per step",
        },
        expected_tools=["query_dsi_tool"],
        expected_count=9,
        response_keywords=["dens", "pres"],
        reference_path=[
            "read data/oceans_11/nif.db",
            'query "SELECT * FROM array_and_types"',
        ],
    ),
    # cd tools/ai_search
    # dsi
    # dsi> search dens
    # dsi> search dens_max *returns no results*
    # Too complicated for level 1
    # BenchmarkTask(
    #     task_id="L1_dens_max_units",
    #     level=1, category="Metadata Generation",
    #     query="What unit is dens_max recorded in?",
    #     expected_tools=["query_dsi_tool"],
    #     response_keywords=["g/cm"],
    # ),

]
