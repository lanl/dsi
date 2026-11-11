"""Level 2 (workflow) tasks: multi-step pipelines combining two or more
data-management categories, scored by score_level2 (all tools + all facts required,
plus an LLM-judged process-quality score).
"""
from ..core import BenchmarkTask

LEVEL2_TASKS = [
    # query catalog -> download file -> query a second DB (2 tools, cross-file)
    BenchmarkTask(
        task_id="L2_search_download_summarize",
        level=2, category="Analysis & Summarization",
        query=(
            "Find the NIF simulation dataset in the Ocean 11 catalog, download the file for "
            "sim name nifcylxyz_hdf5_plt_cnt_0000.flash, and then report who owns the dataset "
            "and how many discrete timesteps the simulation has."
        ),
        prompt_variants={
            "novice": "Can you find the NIF dataset in the Ocean 11 catalog, grab the file for the simulation named nifcylxyz_hdf5_plt_cnt_0000.flash, and then tell me who owns it and how many timesteps it has?",
            "expert": "Resolve the NIF dataset record in the Ocean 11 catalog, retrieve the artifact for sim_name=nifcylxyz_hdf5_plt_cnt_0000.flash via download_file_tool, then report the dataset's point-of-contact and the distinct timestep count.",
            "terse": "find NIF dataset, download nifcylxyz_hdf5_plt_cnt_0000.flash, report owner + timestep count",
            "verbose": "As part of a multi-step data provenance exercise, I first need you to locate the NIF simulation dataset within the Ocean 11 catalog; once you've found it, please download the specific simulation file associated with the sim name nifcylxyz_hdf5_plt_cnt_0000.flash; and finally, report back to me both who is listed as the owner of that dataset and how many discrete timesteps the overall simulation contains.",
            "casual": "yo find the nif dataset in oceans 11, dl the file for nifcylxyz_hdf5_plt_cnt_0000.flash, then lmk who owns it and how many timesteps it's got",
        },
        expected_tools=["query_dsi_tool", "download_file_tool"],
        response_keywords=["Sauppe", "20"],
        requires_network=True,
        success_criteria=(
            "Should query the Ocean 11 catalog to find the NIF dataset's DB link, download the "
            "specified simulation file, and query nif.db to report both the owner's name "
            "(Joshua Paul Sauppe) and the timestep count (20) without redundant re-exploration."
        ),
        reference_path=[
            "read data/oceans_11/ocean_11_datasets.db",
            "query \"SELECT Title, Contact_Point_, dsi_database FROM genesis_datacard WHERE Title LIKE '%NIF%' OR Title LIKE '%FLASH%'\"",
            "read data/oceans_11/nif.db",
            "query \"SELECT sim_name, link FROM nif_metadata WHERE sim_name LIKE '%nifcylxyz_hdf5_plt_cnt_0000%'\"",
            "pull_data url nif_sim https://oceans11.lanl.gov/nif/N210201-001_3D_r2//nifcylxyz_hdf5_plt_cnt_0000.flash",
            'query "SELECT COUNT(DISTINCT timestep) FROM nif_metadata"',
        ],
    ),
    # single-table filter + frequency aggregation (1 tool)
    BenchmarkTask(
        task_id="L2_physics_contact_report",
        level=2, category="Metadata Generation",
        query=(
            "List the titles of every physics-themed dataset in the Ocean 11 catalog "
            "(case-insensitive) along with each one's point of contact, and tell me which "
            "contact person appears most often among them."
        ),
        prompt_variants={
            "novice": "Can you list all the physics datasets in Ocean 11 along with who's in charge of each one, and tell me which person shows up the most?",
            "expert": "Enumerate all catalog records where Theme/Category_:_Domain matches 'physics' (case-insensitive), list each record's Contact_Point_, and identify the most frequently occurring contact.",
            "terse": "list physics datasets + contacts, who's most frequent?",
            "verbose": "I'm compiling a stakeholder report and need the complete list of titles for every dataset in the Ocean 11 catalog that falls under the physics theme or category, regardless of capitalization, along with the point of contact listed for each one; additionally, once you have that list, please identify which single contact person is associated with the largest number of those physics datasets.",
            "casual": "can u list out all the physics datasets in oceans11 plus their contacts and tell me who pops up most",
        },
        expected_tools=["query_dsi_tool"],
        expected_count=5,
        response_keywords=["Nathan"],
        success_criteria=(
            "Should identify all 5 physics-themed datasets (case-insensitive match on the theme "
            "field: Deep Water Impact, Higrad Firetex Wildfire, Gray-Scott reaction-diffusion, "
            "Heat Equations, and Poisson's Equations for Electrostatics), list their contacts, "
            "and correctly identify that Nathan DeBardeleben (or a near-identical spelling "
            "variant on the Poisson record) is associated with the most of them (3 of 5: "
            "Gray-Scott, Heat Equations, and Poisson)."
        ),
        reference_path=[
            "read data/oceans_11/ocean_11_datasets.db",
            "query \"SELECT Title, Contact_Point_ FROM genesis_datacard WHERE LOWER(\\\"Theme/Category_:_Domain\\\") = 'physics'\"",
        ],
    ),
    # load + inspect every field of one row for completeness (2 tools)
    BenchmarkTask(
        task_id="L2_poisson_field_completeness",
        level=2, category="Data Preparation",
        query=(
            "Load the Poisson database and check whether its single record has a value in "
            "every field. Report the number of empty, null, or missing fields you find."
        ),
        prompt_variants={
            "novice": "Can you open the Poisson database and check if its one record has everything filled in, or if anything's missing?",
            "expert": "Load the poisson.db backend and perform a completeness audit on its single record across all fields, reporting the count of null/empty/missing values.",
            "terse": "load poisson.db, check record for missing fields, report count",
            "verbose": "I'd like you to load the Poisson database and then carefully go through its single data record field by field to determine whether every field actually contains a value -- once you've done that, please report back the total number of fields you found to be empty, null, or otherwise missing data.",
            "casual": "can u load poisson.db and check if any fields on that one record are empty or missing, just gimme the count",
        },
        expected_tools=["load_dsi_tool", "query_dsi_tool"],
        expected_count=0,
        success_criteria=(
            "Should load poisson.db, inspect the single record across all 7 fields (name, LAUR, "
            "example, environment, argements, data, statistics), and correctly report that none "
            "are missing/empty/null -- all seven contain populated URLs."
        ),
        reference_path=[
            "read data/oceans_11/poisson.db",
            'query "SELECT * FROM poisson"',
        ],
    ),
    #3-turn conversation testing memory across turns (load, load, compare)
    BenchmarkTask(
        task_id="L2_cross_db_record_compare",
        level=2, category="Analysis & Summarization",
        query="Compare the number of discrete records in the NIF dataset vs. the Poisson dataset.",
        steps=[
            "Load nif.db and tell me how many discrete timesteps are recorded.",
            "Now load poisson.db and tell me how many records it contains.",
            "Of the two datasets you just examined, which has more discrete records, and by how many?",
        ],
        prompt_variants={
            "novice": [
                "Can you open up nif.db and tell me how many different timesteps it has?",
                "Okay now can you open poisson.db and tell me how many records are in it?",
                "Between those two you just looked at, which one has more records, and how many more?",
            ],
            "expert": [
                "Load the nif.db backend and report the cardinality of distinct timestep values.",
                "Now load the poisson.db backend and report its total record count.",
                "Compare the two record counts just obtained and report which dataset has the greater cardinality, and the magnitude of the difference.",
            ],
            "terse": [
                "load nif.db, # timesteps?",
                "load poisson.db, # records?",
                "which has more, by how much?",
            ],
            "verbose": [
                "To begin this comparison exercise, could you please load the nif.db database and tell me precisely how many discrete timesteps are recorded within it?",
                "Great, now building on that, could you also load the separate poisson.db database and tell me exactly how many records it contains in total?",
                "Now that you've examined both of those databases, could you tell me which of the two contains a greater number of discrete records, and specifically by what margin?",
            ],
            "casual": [
                "yo load up nif.db real quick and tell me how many timesteps it's got",
                "now do the same for poisson.db, how many records in that one",
                "so which one's got more records, and by how many",
            ],
        },
        expected_tools=["load_dsi_tool", "query_dsi_tool"],
        expected_count=19,
        response_keywords=["nif", "poisson"],
        success_criteria=(
            "Should recall (or re-verify) NIF's 20 timesteps and Poisson's 1 record from earlier "
            "in this same conversation and correctly conclude NIF has 19 more discrete records "
            "than Poisson, ideally without wastefully re-loading/re-querying data it already "
            "has the answer for."
        ),
        reference_path=[
            "read data/oceans_11/nif.db",
            'query "SELECT COUNT(DISTINCT timestep) FROM nif_metadata"',
            "read data/oceans_11/poisson.db",
            'query "SELECT COUNT(*) FROM poisson"',
        ],
    ),
    # full-table scan + counting a missing field (1 tool)
    BenchmarkTask(
        task_id="L2_dsi_database_link_audit",
        level=2, category="Metadata Generation",
        query=(
            "For every dataset in the Ocean 11 catalog, check whether it has a linked DSI "
            "sub-database (the dsi_database field) and report how many of the 9 datasets are "
            "missing one."
        ),
        prompt_variants={
            "novice": "Can you check all the datasets in Ocean 11 and tell me how many of the 9 don't have a linked DSI sub-database?",
            "expert": "Audit the dsi_database field across all 9 genesis_datacard records and report the count of records with a null/missing linked sub-database.",
            "terse": "# of 9 oceans11 datasets missing dsi_database link?",
            "verbose": "I'm conducting a data-completeness audit and need to go through every single dataset in the Ocean 11 catalog -- all 9 of them -- checking whether each one has an associated/linked DSI sub-database recorded in its dsi_database field, and then report back the total number of datasets that are missing that link.",
            "casual": "can u check all 9 oceans11 datasets and tell me how many don't have a dsi_database link",
        },
        expected_tools=["query_dsi_tool"],
        expected_count=7,
        success_criteria=(
            "Should query the dsi_database field across all 9 genesis_datacard records and "
            "correctly report that 7 of the 9 datasets have no linked DSI sub-database (only "
            "the NIF and Poisson entries do)."
        ),
        reference_path=[
            "read data/oceans_11/ocean_11_datasets.db",
            'query "SELECT Title, dsi_database FROM genesis_datacard"',
        ],
    ),
    # query -> download -> extract facts from a filename (2 tools)
    BenchmarkTask(
        task_id="L2_poisson_args_filename_facts",
        level=2, category="Data Preparation",
        query=(
            "Download the Poisson dataset's simulation-arguments file (the 'argements' field "
            "in the Poisson catalog record) and, from its filename or contents, report the "
            "grid size and number of samples in this dataset."
        ),
        prompt_variants={
            "novice": "Can you download the arguments file for the Poisson dataset and tell me the grid size and how many samples there are, based on the file?",
            "expert": "Retrieve the simulation-arguments artifact referenced by the Poisson record's 'argements' field, and extract the grid resolution and sample count from its filename/contents.",
            "terse": "download poisson 'argements' file, report grid size + sample count",
            "verbose": "I need you to download the file referenced by the 'argements' field on the Poisson catalog record -- this should be the simulation-arguments file -- and then, based on either its filename or its actual contents, report back to me both the grid size used for the simulation and the total number of samples included in the dataset.",
            "casual": "can u dl the poisson args file and tell me the grid size + sample count from it",
        },
        expected_tools=["query_dsi_tool", "download_file_tool"],
        response_keywords=["32x32", "1000"],
        requires_network=True,
        success_criteria=(
            "Should query poisson.db for the 'argements' URL, download it, and correctly "
            "extract '32x32' as the grid size and '1000' (from the 1-1000 sample range in the "
            "filename/contents) as the sample count, without fabricating different numbers."
        ),
        reference_path=[
            "read data/oceans_11/poisson.db",
            'query "SELECT argements FROM poisson"',
            "pull_data url poisson_args https://oceans11.lanl.gov/electrostaticEquations/LA-UR-25-29606-electrostatic_dataset_1k/data/arguments_electrostatic_poisson_32x32_1-1000.json",
        ],
    ),
    #cross-referencing a static doc file against live schema, not just querying one source
    BenchmarkTask(
        task_id="L2_poisson_glossary_audit_1",
        level=2, category="Data Preparation",
        query=(
            "The Poisson dataset has a field glossary file at "
            "data/oceans_11/poisson_description.yaml describing its documented columns. "
            "Load poisson.db, then read the glossary file and compare the ACTUAL table "
            "schema against the fields documented in the glossary. Report any column that "
            "exists in the real data but is NOT documented in the glossary."
        ),
        prompt_variants={
            "novice": "There's a file at data/oceans_11/poisson_description.yaml that's supposed to describe all the columns in poisson.db. Can you load poisson.db, check that glossary file, and tell me if there's any column in the real data that's missing from the glossary?",
            "expert": "Load the poisson.db schema and cross-reference it against the field definitions documented in data/oceans_11/poisson_description.yaml; report any schema column absent from that glossary.",
            "terse": "load poisson.db, read data/oceans_11/poisson_description.yaml, diff schema vs glossary fields",
            "verbose": "I'd like you to perform a documentation-completeness check: first load the poisson.db database so you can see its actual table schema, then separately open and read the field glossary file located at data/oceans_11/poisson_description.yaml, which is supposed to document every column in that table -- once you've done both, compare the two lists and report any column that genuinely exists in the real database but isn't documented anywhere in that glossary file.",
            "casual": "can u load poisson.db, check the glossary at data/oceans_11/poisson_description.yaml, and tell me if any real column isn't in it",
        },
        expected_tools=["load_dsi_tool", "python_repl_tool"],
        response_keywords=["argements"],
        success_criteria=(
            "Should inspect poisson.db's real schema (name, LAUR, example, environment, "
            "argements, data, statistics), compare it field-by-field against the glossary's "
            "documented fields (name, LAUR, example, environment, data, statistics), and "
            "correctly identify 'argements' as the one column present in the data but "
            "undocumented in the glossary -- without inventing a different missing field or "
            "claiming the schema and glossary fully match."
        ),
        reference_path=[
            "read data/oceans_11/poisson.db",
            "schema",
            "read data/oceans_11/poisson_description.yaml",
            "diff schema fields vs glossary fields",
        ],
    ),

        #cross-referencing a static doc file against live schema, not just querying one source
    BenchmarkTask(
        task_id="L2_poisson_glossary_audit_2",
        level=2, category="Data Preparation",
        query=(
            "The Poisson dataset has a field glossary (poisson_description.yaml) describing its columns as: name, LAUR, example, environment, data, statistics. Load poisson.db, compare its ACTUAL table schema against that documented list of fields, and report any column that exists in the real data but is NOT documented in the glossary."
        ),
        prompt_variants={
            "novice": "There's a file at data/oceans_11/poisson_description.yaml that's supposed to describe all the columns in poisson.db. Can you load poisson.db, check that glossary file, and tell me if there's any column in the real data that's missing from the glossary?",
            "expert": "Load the poisson.db schema and cross-reference it against the field definitions documented in data/oceans_11/poisson_description.yaml; report any schema column absent from that glossary.",
            "terse": "load poisson.db, read data/oceans_11/poisson_description.yaml, diff schema vs glossary fields",
            "verbose": "I'd like you to perform a documentation-completeness check: first load the poisson.db database so you can see its actual table schema, then separately open and read the field glossary file located at data/oceans_11/poisson_description.yaml, which is supposed to document every column in that table -- once you've done both, compare the two lists and report any column that genuinely exists in the real database but isn't documented anywhere in that glossary file.",
            "casual": "can u load poisson.db, check the glossary at data/oceans_11/poisson_description.yaml, and tell me if any real column isn't in it",
        },
        expected_tools=["load_dsi_tool", "python_repl_tool"],
        response_keywords=["argements"],
        success_criteria=(
            "Should inspect poisson.db's real schema (name, LAUR, example, environment, "
            "argements, data, statistics), compare it field-by-field against the glossary's "
            "documented fields (name, LAUR, example, environment, data, statistics), and "
            "correctly identify 'argements' as the one column present in the data but "
            "undocumented in the glossary -- without inventing a different missing field or "
            "claiming the schema and glossary fully match."
        ),
        reference_path=[
            "read data/oceans_11/poisson.db",
            "schema",
            "read data/oceans_11/poisson_description.yaml",
            "diff schema fields vs glossary fields",
        ],
    ),

    
    # tests free-text/substring filtering on an unstructured tag field,
    BenchmarkTask(
        task_id="L2_simulation_tag_contacts",
        level=2, category="Metadata Generation",
        query=(
            "Find every dataset in the Ocean 11 catalog whose keywords/tags mention "
            "'simulation', list each one's point-of-contact owner, and tell me if any "
            "contact owns more than one of them."
        ),
        prompt_variants={
            "novice": "Can you find all the datasets in Ocean 11 that have 'simulation' somewhere in their tags, list who owns each one, and tell me if anyone owns more than one?",
            "expert": "Filter the Ocean 11 catalog for records whose Keywords/Tag field contains the substring 'simulation', list each record's Contact_Point_, and identify any contact appearing on more than one matching record.",
            "terse": "find datasets tagged 'simulation', list contacts, any contact own >1?",
            "verbose": "I'm trying to understand ownership concentration among simulation-related holdings, so I need you to search through the Ocean 11 catalog for every dataset whose keywords or tags mention the word 'simulation' in any form, list the point-of-contact owner for each one you find, and then let me know whether any single contact person is responsible for more than one of those datasets.",
            "casual": "can u find all the 'simulation' tagged datasets in oceans11, list their owners, and tell me if anyone's got more than one",
        },
        expected_tools=["query_dsi_tool"],
        expected_count=5,
        response_keywords=["Nathan"],
        success_criteria=(
            "Should find all 5 datasets with 'simulation' in their Keywords/Tag field via "
            "substring/fuzzy matching (Deep Water Impact Ensemble, Higrad Firetex Wildfire, "
            "Gray-Scott reaction-diffusion, Heat Equations, and the 3D FLASH/NIF dataset) -- "
            "not an exact match on the Theme/Category field -- list their contacts, and "
            "correctly report that Nathan DeBardeleben is the only contact appearing more "
            "than once (owns both Gray-Scott and Heat Equations)."
        ),
        reference_path=[
            "read data/oceans_11/ocean_11_datasets.db",
            "query \"SELECT Title, Contact_Point_ FROM genesis_datacard WHERE \\\"Keywords/Tag\\\" LIKE '%simulation%'\"",
        ],
    ),
    # requires genuine aggregation/extremum-finding across many rows
    BenchmarkTask(
        task_id="L2_nif_dens_max_extremum",
        level=2, category="Analysis & Summarization",
        query=(
            "Across all 20 timesteps of the NIF simulation, which timestep has the highest "
            "maximum density (dens_max), and what is that value?"
        ),
        prompt_variants={
            "novice": "Looking at all the NIF timesteps, which one has the highest density value, and what's that number?",
            "expert": "Across all nif_metadata rows, identify the timestep with the maximum value of dens_max and report that value.",
            "terse": "highest dens_max across NIF timesteps, which + value?",
            "verbose": "I'm trying to identify the peak density condition reached anywhere during the NIF simulation, so across all 20 recorded timesteps, could you determine which single timestep has the highest value of dens_max, and also tell me what that maximum density value actually is?",
            "casual": "out of all the nif timesteps which one's got the highest dens_max, and what's the number",
        },
        expected_tools=["load_dsi_tool", "query_dsi_tool"],
        response_keywords=["6", "7.9"],
        success_criteria=(
            "Should aggregate/compare dens_max across ALL 20 nif_metadata rows (e.g. via "
            "MAX()/ORDER BY, or by scanning and comparing every row) rather than reporting a "
            "single arbitrary row, and correctly identify timestep 6 with a dens_max of "
            "approximately 7.95 as the highest."
        ),
        reference_path=[
            "read data/oceans_11/nif.db",
            'query "SELECT timestep, dens_max FROM nif_metadata ORDER BY dens_max DESC LIMIT 1"',
        ],
    ),
    #tests whether the agent fabricates a confident-but-wrong distinction when the data actually shows a tie
    BenchmarkTask(
        task_id="L2_nif_num_arrays_tie",
        level=2, category="Analysis & Summarization",
        query=(
            "Which timestep in the NIF simulation has the most data arrays recorded (num_arrays), "
            "and by how many more than the next-highest timestep?"
        ),
        prompt_variants={
            "novice": "Which NIF timestep has the most arrays recorded, and how many more than the next one?",
            "expert": "Identify the timestep with the maximum num_arrays value in nif_metadata, and report the margin over the second-highest value.",
            "terse": "max num_arrays timestep + margin over 2nd?",
            "verbose": "I'm trying to understand whether the amount of data recorded varies across timesteps in the NIF simulation, so could you tell me which single timestep has the greatest number of data arrays recorded (num_arrays), and specifically by how many more arrays than whichever timestep has the next-highest count?",
            "casual": "which nif timestep has the most arrays recorded, and by how much more than the runner up",
        },
        expected_tools=["load_dsi_tool", "query_dsi_tool"],
        response_keywords=["9"],
        success_criteria=(
            "Should check num_arrays across ALL 20 nif_metadata rows and recognize they are "
            "ALL tied at exactly 9 -- there is no single highest timestep, so the correct "
            "answer explicitly states they're tied (difference of 0), rather than naming an "
            "arbitrary single timestep as 'the winner' or fabricating a nonzero difference."
        ),
        reference_path=[
            "read data/oceans_11/nif.db",
            'query "SELECT DISTINCT num_arrays FROM nif_metadata"',
        ],
    ),
    # a 2-step "find X, then branch on a property of X" workflow where the correct final action (stop, don't drill down) depends on data the agent only discovers mid-task.
    BenchmarkTask(
        task_id="L2_most_recent_drilldown",
        level=2, category="Data Discovery",
        query=(
            "Find the most recently updated dataset in the Ocean 11 catalog, then check "
            "whether it has a linked DSI sub-database (the dsi_database field) that you "
            "could load for more detail."
        ),
        prompt_variants={
            "novice": "Which dataset in Ocean 11 was updated most recently? And does it have a linked database you could open up for more details?",
            "expert": "Identify the catalog record with the most recent Update/Modification_Date, then determine whether its dsi_database field references a loadable sub-database.",
            "terse": "most recently updated oceans11 dataset? has linked dsi_database?",
            "verbose": "I'm trying to figure out which dataset in the Ocean 11 catalog has been updated most recently out of all of them, and once you've identified that dataset, I'd also like you to check whether it has a linked DSI sub-database listed in its dsi_database field that could actually be loaded for additional detail.",
            "casual": "which oceans11 dataset got updated most recently, and does it have a linked db you could dig into",
        },
        expected_tools=["query_dsi_tool"],
        response_keywords=["High Explosives"],
        success_criteria=(
            "Should correctly identify 'The High Explosives & Affected Targets (HEAT) "
            "Dataset' (updated 2025-10-14) as the most recently updated catalog entry, then "
            "correctly report that its dsi_database field is null/empty -- i.e. there is no "
            "linked sub-database to drill into -- rather than fabricating one or incorrectly "
            "naming NIF/Poisson (which DO have linked sub-databases, but are not the most "
            "recently updated)."
        ),
        reference_path=[
            "read data/oceans_11/ocean_11_datasets.db",
            'query "SELECT Title, dsi_database FROM genesis_datacard ORDER BY \\"Update/Modification_Date\\" DESC LIMIT 1"',
        ],
    ),
]
