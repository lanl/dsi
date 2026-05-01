"""
wwPDB Backend Function Tests

Tests wwPDB backend methods directly without Terminal integration.

Run from the repository root with something like:
python -m pytest -s dsi/backends/tests/test_wwpdb.py
"""

from dsi.backends.wwpdb import WWPDB


TEST_DOIS = [
    "10.2210/pdb1cbs/pdb",
    "10.2210/pdb4hhb/pdb",
]


def test_wwpdb_init_and_load():
    backend = WWPDB(identifiers=TEST_DOIS, auto_load=True)
    assert backend is not None
    assert backend.num_tables() >= 1
    backend.close()


def test_wwpdb_get_table_names():
    backend = WWPDB(identifiers=TEST_DOIS, auto_load=True)
    table_names = backend.get_table_names()

    assert isinstance(table_names, list)
    assert "datasets" in table_names
    assert "resources" in table_names
    backend.close()


def test_wwpdb_get_tables():
    backend = WWPDB(identifiers=TEST_DOIS, auto_load=True)
    tables = backend.get_tables()

    assert isinstance(tables, dict)
    assert "datasets" in tables
    assert "resources" in tables
    assert "errors" in tables
    backend.close()


def test_wwpdb_get_datasets_table():
    backend = WWPDB(identifiers=TEST_DOIS, auto_load=True)
    datasets = backend.get_table("datasets")

    assert isinstance(datasets, list)
    assert len(datasets) >= 1

    row = datasets[0]
    assert "dataset_id" in row
    assert "title" in row
    assert "source_repository" in row
    assert row["source_repository"] == "wwPDB"
    backend.close()


def test_wwpdb_get_resources_table():
    backend = WWPDB(identifiers=TEST_DOIS, auto_load=True)
    resources = backend.get_table("resources")

    assert isinstance(resources, list)
    assert len(resources) >= 1

    row = resources[0]
    assert "resource_id" in row
    assert "dataset_id" in row
    assert "download_url" in row
    assert row["format"] == "cif.gz"
    backend.close()


def test_wwpdb_summary_all():
    backend = WWPDB(identifiers=TEST_DOIS, auto_load=True)
    summary = backend.summary()

    assert isinstance(summary, dict)
    assert summary["backend"] == "WWPDB"
    assert "tables" in summary
    backend.close()


def test_wwpdb_summary_single_table():
    backend = WWPDB(identifiers=TEST_DOIS, auto_load=True)
    summary = backend.summary("datasets")

    assert isinstance(summary, dict)
    assert summary["table_name"] == "datasets"
    assert "row_count" in summary
    backend.close()


def test_wwpdb_display():
    backend = WWPDB(identifiers=TEST_DOIS, auto_load=True)
    datasets = backend.display("datasets")

    assert isinstance(datasets, list)
    assert len(datasets) >= 1
    backend.close()


def test_wwpdb_list():
    backend = WWPDB(identifiers=TEST_DOIS, auto_load=True)
    listed = backend.list()

    assert isinstance(listed, list)
    assert "datasets" in listed
    backend.close()


def test_wwpdb_find():
    backend = WWPDB(identifiers=TEST_DOIS, auto_load=True)
    matches = backend.find("hemoglobin")

    assert isinstance(matches, list)
    backend.close()


def test_wwpdb_find_table():
    backend = WWPDB(identifiers=TEST_DOIS, auto_load=True)
    matches = backend.find_table("datasets")

    assert isinstance(matches, list)
    assert len(matches) >= 1
    backend.close()


def test_wwpdb_find_column():
    backend = WWPDB(identifiers=TEST_DOIS, auto_load=True)
    matches = backend.find_column("title")

    assert isinstance(matches, list)
    assert len(matches) >= 1
    backend.close()


def test_wwpdb_find_cell():
    backend = WWPDB(identifiers=TEST_DOIS, auto_load=True)
    matches = backend.find_cell("1CBS")

    assert isinstance(matches, list)
    backend.close()


def test_wwpdb_find_relation():
    backend = WWPDB(identifiers=TEST_DOIS, auto_load=True)
    relations = backend.find_relation("dataset_id", "has_resources")

    assert isinstance(relations, list)
    assert len(relations) >= 1
    assert "dataset_id" in relations[0]
    assert "related_resource_count" in relations[0]
    backend.close()


def test_wwpdb_query_artifacts_single():
    backend = WWPDB(auto_load=False)
    tables = backend.query_artifacts("10.2210/pdb1cbs/pdb")

    assert isinstance(tables, dict)
    assert "datasets" in tables
    assert "resources" in tables
    backend.close()


def test_wwpdb_query_artifacts_multiple():
    backend = WWPDB(auto_load=False)
    tables = backend.query_artifacts(TEST_DOIS)

    assert isinstance(tables, dict)
    assert "datasets" in tables
    assert "resources" in tables
    assert len(tables["datasets"]) >= 1
    backend.close()


def test_wwpdb_process_artifacts():
    backend = WWPDB(identifiers=TEST_DOIS, auto_load=True)
    tables = backend.process_artifacts()

    assert isinstance(tables, dict)
    assert "datasets" in tables
    assert "resources" in tables
    backend.close()


def test_wwpdb_notebook():
    backend = WWPDB(identifiers=TEST_DOIS, auto_load=True)
    result = backend.notebook(table_name="datasets")

    assert isinstance(result, list)
    backend.close()


def test_wwpdb_get_schema():
    backend = WWPDB(identifiers=TEST_DOIS, auto_load=True)
    schema = backend.get_schema("datasets")

    assert isinstance(schema, list)
    assert "dataset_id" in schema
    backend.close()


def test_wwpdb_get_full_schema():
    backend = WWPDB(identifiers=TEST_DOIS, auto_load=True)
    schema = backend.get_schema()

    assert isinstance(schema, dict)
    assert "datasets" in schema
    assert "resources" in schema
    assert "errors" in schema
    backend.close()


def test_wwpdb_validate_urls():
    backend = WWPDB(identifiers=TEST_DOIS, auto_load=True)
    results = backend.validate_urls("resources")

    assert isinstance(results, list)
    assert len(results) >= 1
    assert "is_valid" in results[0]
    assert "method_used" in results[0]
    backend.close()


def test_wwpdb_overwrite_table():
    backend = WWPDB(auto_load=False)

    new_rows = [
        {
            "dataset_id": "TEST1",
            "source_repository": "wwPDB",
            "title": "Test Entry",
            "description": "Test Description",
        }
    ]

    backend.overwrite_table("datasets", new_rows)
    datasets = backend.get_table("datasets")

    assert len(datasets) == 1
    assert datasets[0]["dataset_id"] == "TEST1"
    backend.close()


def test_wwpdb_ingest_artifacts_read_only():
    backend = WWPDB(auto_load=False)

    try:
        backend.ingest_artifacts([])
        assert False, "Expected NotImplementedError"
    except NotImplementedError:
        assert True
    finally:
        backend.close()


def test_wwpdb_close():
    backend = WWPDB(auto_load=False)
    backend.close()
    assert True


def test_wwpdb_direct_pdb_id():
    backend = WWPDB(identifiers=["1CBS"], auto_load=True)
    datasets = backend.get_table("datasets")

    assert len(datasets) == 1
    assert datasets[0]["dataset_id"] == "1CBS"
    backend.close()


def test_wwpdb_errors_table_for_invalid_identifier():
    backend = WWPDB(identifiers=["not-a-pdb-id"], auto_load=True)

    errors = backend.get_table("errors")
    assert isinstance(errors, list)
    assert len(errors) == 1
    assert errors[0]["status"] == "skipped"
    backend.close()


def test_wwpdb_expected_table_names_include_errors():
    backend = WWPDB(identifiers=TEST_DOIS, auto_load=True)
    table_names = backend.get_table_names()

    assert "datasets" in table_names
    assert "resources" in table_names
    assert "errors" in table_names
    backend.close()


def test_wwpdb_resource_links_reference_existing_datasets():
    backend = WWPDB(identifiers=TEST_DOIS, auto_load=True)

    dataset_ids = {row["dataset_id"] for row in backend.get_table("datasets")}
    for resource in backend.get_table("resources"):
        assert resource["dataset_id"] in dataset_ids

    backend.close()


def test_wwpdb_context_manager():
    with WWPDB(identifiers=["1CBS"], auto_load=True) as backend:
        assert "datasets" in backend.get_table_names()


def test_wwpdb_query_params_keyword_search():
    backend = WWPDB(auto_load=False)
    tables = backend.query_artifacts({"keywords": "hemoglobin", "limit": 5})

    assert isinstance(tables, dict)
    assert "datasets" in tables
    assert "resources" in tables
    assert "errors" in tables
    assert len(tables["datasets"]) <= 5
    backend.close()


def test_wwpdb_init_with_params():
    backend = WWPDB(params={"keywords": "hemoglobin", "limit": 3}, auto_load=True)

    assert "datasets" in backend.get_table_names()
    assert "resources" in backend.get_table_names()
    assert len(backend.get_table("datasets")) <= 3
    backend.close()


def test_wwpdb_query_string_keyword_search():
    backend = WWPDB(auto_load=False)
    tables = backend.query_artifacts("hemoglobin", limit=3)

    assert isinstance(tables, dict)
    assert "datasets" in tables
    assert len(tables["datasets"]) <= 3
    backend.close()


def test_wwpdb_unsupported_params():
    backend = WWPDB(auto_load=False)

    try:
        backend.query_artifacts({"bad_param": "x"})
        assert False, "Expected ValueError for unsupported params"
    except ValueError:
        assert True
    finally:
        backend.close()


def test_wwpdb_validate_on_init_false():
    backend = WWPDB(auto_load=False, validate_on_init=False)

    assert backend is not None
    assert backend.get_table_names() == []
    backend.close()


def test_wwpdb_excel_style_batch_input_list():
    """
    This simulates how an Excel DOI column would be converted into a Python list.
    The backend itself does not depend on Excel.
    """
    excel_like_doi_column = [
        "10.2210/pdb1cbs/pdb",
        "10.2210/pdb4hhb/pdb",
    ]

    backend = WWPDB(identifiers=excel_like_doi_column, auto_load=True)
    datasets = backend.get_table("datasets")

    assert len(datasets) >= 1
    assert all(row["source_repository"] == "wwPDB" for row in datasets)
    backend.close()