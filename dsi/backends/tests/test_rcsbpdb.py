"""
RCSBPDB Backend Function Tests

Tests RCSBPDB backend methods directly without Terminal integration.

Run from the repository root with:
python -m pytest -s dsi/backends/tests/test_rcsbpdb.py

The RCSBPDB backend fixtures are shared across tests wherever possible to avoid
repeated backend initialization and repeated mocked RCSB API calls. Tests that
close, reload, or otherwise mutate a backend use dedicated fixtures.
"""

from collections import OrderedDict

import pandas as pd
import pytest
import requests

from dsi.backends.rcsbpdb import RCSBPDB, FileResource, RCSBPDBResolution

TEST_DOIS = [
    "10.2210/pdb1cbs/pdb",
    "10.2210/pdb4hhb/pdb",
]

TEST_PDB_IDS = ["1CBS", "4HHB"]


# =============================================================================
# Test fixtures and API mocks
# =============================================================================

def sample_entry_metadata(pdb_id="1CBS", title=None):
    """Small RCSB Data API-like entry payload."""
    title = title or f"Sample structure {pdb_id}"

    return {
        "struct": {
            "title": title,
        },
        "exptl": [
            {
                "method": "X-RAY DIFFRACTION",
            }
        ],
        "struct_keywords": {
            "pdbx_keywords": "DNA BINDING PROTEIN",
            "text": "sample keywords",
        },
        "rcsb_accession_info": {
            "initial_release_date": "1994-01-31T00:00:00+0000",
            "revision_date": "2020-07-29T00:00:00+0000",
        },
        "citation": [
            {
                "pdbx_database_id_DOI": f"10.2210/pdb{pdb_id.lower()}/pdb",
            }
        ],
        "rcsb_primary_citation": {
            "pdbx_database_id_DOI": f"10.2210/pdb{pdb_id.lower()}/pdb",
            "rcsb_authors": ["Example Author"],
        },
        "pdbx_struct_assembly": [
            {
                "id": "1",
            }
        ],
    }


def mock_request(self, endpoint, params=None):
    """Mock RCSB Data API GET helper."""
    pdb_id = endpoint.rstrip("/").split("/")[-1].upper()

    if pdb_id == "1CBS":
        return sample_entry_metadata("1CBS", "CRYSTAL STRUCTURE OF A DNA-BINDING PROTEIN")
    if pdb_id == "4HHB":
        return sample_entry_metadata("4HHB", "THE CRYSTAL STRUCTURE OF HUMAN DEOXYHAEMOGLOBIN")
    if pdb_id == "2XYZ":
        return sample_entry_metadata("2XYZ", "MOCK SEARCH RESULT STRUCTURE")

    response = requests.Response()
    response.status_code = 404
    raise requests.HTTPError(response=response)


def mock_post_json(self, endpoint, payload):
    """Mock RCSB Search API POST helper."""
    return {
        "result_set": [
            {"identifier": "1CBS"},
            {"identifier": "4HHB"},
        ]
    }


def mock_url_exists(self, url):
    """Pretend all generated resource URLs exist."""
    return True, 200, "application/octet-stream"


@pytest.fixture(scope="session", autouse=True)
def mocked_rcsb():
    """Patch network helpers once so tests are deterministic and offline."""
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(RCSBPDB, "_request", mock_request)
    monkeypatch.setattr(RCSBPDB, "_post_json", mock_post_json)
    monkeypatch.setattr(RCSBPDB, "_url_exists", mock_url_exists)

    yield

    monkeypatch.undo()


@pytest.fixture(scope="session")
def loaded_backend(mocked_rcsb):
    """Shared loaded backend for non-destructive tests."""
    instance = RCSBPDB(
        identifiers=TEST_DOIS,
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    yield instance

    if instance._loaded:
        instance.close()


@pytest.fixture(scope="session")
def keyword_backend(mocked_rcsb):
    """Shared backend loaded through keyword search."""
    instance = RCSBPDB(
        params={"keywords": "hemoglobin", "limit": 2},
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    yield instance

    if instance._loaded:
        instance.close()


@pytest.fixture(scope="session")
def invalid_backend(mocked_rcsb):
    """Shared backend with one invalid identifier."""
    instance = RCSBPDB(
        identifiers=["not-a-pdb-id"],
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    yield instance

    if instance._loaded:
        instance.close()


@pytest.fixture(scope="session")
def empty_backend(mocked_rcsb):
    """Shared unloaded backend for non-mutating helper tests."""
    instance = RCSBPDB(
        auto_load=False,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    yield instance

    if instance._loaded:
        instance.close()


@pytest.fixture
def mutable_backend(mocked_rcsb):
    """Fresh backend for tests that intentionally mutate loaded state."""
    instance = RCSBPDB(
        auto_load=False,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    yield instance

    if instance._loaded:
        instance.close()


@pytest.fixture
def api_backend(mocked_rcsb):
    """Fresh backend for API-backed find_relation tests that reload state."""
    instance = RCSBPDB(
        auto_load=False,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    yield instance

    if instance._loaded:
        instance.close()


# =============================================================================
# 1) Identifier normalization and classification
# =============================================================================

def test_rcsbpdb_normalize_doi():
    assert RCSBPDB.normalize_doi("https://doi.org/10.2210/pdb1cbs/pdb") == "10.2210/pdb1cbs/pdb"
    assert RCSBPDB.normalize_doi("doi:10.2210/pdb4hhb/pdb") == "10.2210/pdb4hhb/pdb"
    assert RCSBPDB.normalize_doi("no doi here") is None
    assert RCSBPDB.normalize_doi(None) is None


def test_rcsbpdb_normalize_pdb_id():
    assert RCSBPDB.normalize_pdb_id("1cbs") == "1CBS"
    assert RCSBPDB.normalize_pdb_id("4HHB") == "4HHB"
    assert RCSBPDB.normalize_pdb_id("not-a-pdb-id") is None
    assert RCSBPDB.normalize_pdb_id(None) is None


def test_rcsbpdb_classify_identifier():
    assert RCSBPDB.classify_identifier("10.2210/pdb1cbs/pdb") == "rcsbpdb_doi"
    assert RCSBPDB.classify_identifier("1CBS") == "pdb_id"
    assert RCSBPDB.classify_identifier("not-a-pdb-id") == "other"


def test_rcsbpdb_extract_pdb_id_from_doi():
    assert RCSBPDB.extract_pdb_id_from_doi("10.2210/pdb1cbs/pdb") == "1CBS"
    assert RCSBPDB.extract_pdb_id_from_doi("10.1234/example") is None


def test_rcsbpdb_get_file_ext():
    assert RCSBPDB.get_file_ext("file.csv") == "csv"
    assert RCSBPDB.get_file_ext("https://example.org/file.cif.gz?download=1") == "cif.gz"
    assert RCSBPDB.get_file_ext("no_extension") is None
    assert RCSBPDB.get_file_ext(None) is None


def test_rcsbpdb_classify_usability():
    assert RCSBPDB.classify_usability(["csv"]) == "tabular_or_easy_parse"
    assert RCSBPDB.classify_usability(["cif.gz"]) == "scientific_structured"
    assert RCSBPDB.classify_usability(["zip"]) == "archive_only"
    assert RCSBPDB.classify_usability(["abc"]) == "other_format"
    assert RCSBPDB.classify_usability([]) == "lookup_failed"


# =============================================================================
# 2) Basic Backend Initialization
# =============================================================================

def test_rcsbpdb_initialization_no_auto_load(empty_backend):
    assert empty_backend._loaded is True
    assert empty_backend.list(True) == []

    datasets_schema = empty_backend.get_schema("datasets")
    resources_schema = empty_backend.get_schema("resources")
    errors_schema = empty_backend.get_schema("errors")

    assert isinstance(datasets_schema, str)
    assert isinstance(resources_schema, str)
    assert isinstance(errors_schema, str)

    assert "CREATE TABLE datasets" in datasets_schema
    assert "CREATE TABLE resources" in resources_schema
    assert "CREATE TABLE errors" in errors_schema


def test_rcsbpdb_initialization_with_identifiers(loaded_backend):
    assert loaded_backend._loaded is True
    assert "datasets" in loaded_backend.list(True)
    assert "resources" in loaded_backend.list(True)

    datasets = loaded_backend.get_table("datasets")
    assert isinstance(datasets, pd.DataFrame)
    assert len(datasets) == 2
    assert set(datasets["dataset_id"]) == {"1CBS", "4HHB"}


def test_rcsbpdb_validate_connection(mocked_rcsb):
    class FakeResponse:
        def raise_for_status(self):
            return None

    class FakeSession:
        def get(self, url, timeout=60, verify=True):
            return FakeResponse()

        def close(self):
            pass

    backend = RCSBPDB(
        auto_load=False,
        validate_on_init=False,
        validate_resource_urls=False,
    )
    backend.session = FakeSession()

    assert backend.validate_connection() is True
    backend.close()


# =============================================================================
# 3) Data Loading and Structure
# =============================================================================

def test_rcsbpdb_load_initial_data(loaded_backend):
    assert loaded_backend._loaded is True
    assert isinstance(loaded_backend.tables, OrderedDict)
    assert "datasets" in loaded_backend.tables
    assert "resources" in loaded_backend.tables

    dataset_cols = list(loaded_backend.tables["datasets"].keys())
    assert dataset_cols == RCSBPDB.DATASET_SCHEMA

    resource_cols = list(loaded_backend.tables["resources"].keys())
    assert resource_cols == RCSBPDB.RESOURCE_SCHEMA


def test_rcsbpdb_process_artifacts(mutable_backend):
    mutable_backend.raw_results = [
        mutable_backend.lookup_identifier("10.2210/pdb1cbs/pdb"),
        mutable_backend.lookup_identifier("not-a-pdb-id"),
    ]

    tables = mutable_backend.process_artifacts()

    assert isinstance(tables, OrderedDict)
    assert "datasets" in tables
    assert "resources" in tables
    assert "errors" in tables

    datasets = mutable_backend.get_table("datasets")
    errors = mutable_backend.get_table("errors")

    assert len(datasets) == 1
    assert len(errors) == 1
    assert errors.iloc[0]["status"] == "skipped"


def test_rcsbpdb_extract_tables_success_and_error(empty_backend):
    results = [
        empty_backend.lookup_identifier("1CBS"),
        empty_backend.lookup_identifier("bad-id"),
    ]

    tables = empty_backend._extract_tables(results)

    assert set(tables.keys()) == {"datasets", "resources", "errors"}
    assert len(tables["datasets"]) == 1
    assert len(tables["resources"]) >= 1
    assert len(tables["errors"]) == 1

    assert tables["datasets"][0]["dataset_id"] == "1CBS"
    assert tables["errors"][0]["status"] == "skipped"


def test_rcsbpdb_build_file_resources(empty_backend):
    resources = empty_backend._build_file_resources("1CBS", sample_entry_metadata("1CBS"))

    assert isinstance(resources, list)
    assert len(resources) >= 8
    assert all(isinstance(resource, FileResource) for resource in resources)

    labels = {resource.label for resource in resources}
    assert "1cbs.cif" in labels
    assert "1cbs.cif.gz" in labels
    assert "1cbs.pdb" in labels
    assert "1cbs-assembly1.cif.gz" in labels


# =============================================================================
# 4) Table Accessors
# =============================================================================

def test_rcsbpdb_get_table_dataframe(loaded_backend):
    datasets = loaded_backend.get_table("datasets")
    assert isinstance(datasets, pd.DataFrame)
    assert not datasets.empty
    assert "dataset_id" in datasets.columns
    assert "title" in datasets.columns


def test_rcsbpdb_get_table_dict_return(loaded_backend):
    datasets = loaded_backend.get_table("datasets", dict_return=True)
    assert isinstance(datasets, OrderedDict)
    assert "dataset_id" in datasets
    assert datasets["dataset_id"] == ["1CBS", "4HHB"]


def test_rcsbpdb_get_empty_table_returns_dataframe_with_schema(empty_backend):
    datasets = empty_backend.get_table("datasets")

    assert isinstance(datasets, pd.DataFrame)
    assert datasets.empty
    assert list(datasets.columns) == RCSBPDB.DATASET_SCHEMA


def test_rcsbpdb_get_tables(loaded_backend):
    tables = loaded_backend.get_tables()

    assert isinstance(tables, OrderedDict)
    assert "datasets" in tables
    assert "resources" in tables


def test_rcsbpdb_get_schema(empty_backend):
    full_schema = empty_backend.get_schema()
    dataset_schema = empty_backend.get_schema("dataset")
    resource_schema = empty_backend.get_schema("resource")
    error_schema = empty_backend.get_schema("error")

    assert isinstance(full_schema, str)
    assert isinstance(dataset_schema, str)
    assert isinstance(resource_schema, str)
    assert isinstance(error_schema, str)

    assert "CREATE TABLE datasets" in full_schema
    assert "CREATE TABLE resources" in full_schema
    assert "CREATE TABLE errors" in full_schema

    assert "CREATE TABLE datasets" in dataset_schema
    assert "dataset_id" in dataset_schema
    assert "doi" in dataset_schema

    assert "CREATE TABLE resources" in resource_schema
    assert "resource_id" in resource_schema
    assert "download_url" in resource_schema

    assert "CREATE TABLE errors" in error_schema
    assert "identifier" in error_schema
    assert "status" in error_schema


def test_rcsbpdb_table_name_resolution(empty_backend):
    assert empty_backend._resolve_table_name("dataset") == "datasets"
    assert empty_backend._resolve_table_name("datasets") == "datasets"
    assert empty_backend._resolve_table_name("resource") == "resources"
    assert empty_backend._resolve_table_name("error") == "errors"
    assert empty_backend._resolve_table_name("custom") == "custom"
    assert empty_backend._resolve_table_name(None) is None


# =============================================================================
# 5) Search API / Param Loading
# =============================================================================

def test_rcsbpdb_build_search_query_keywords(empty_backend):
    query = empty_backend._build_search_query({"keywords": "hemoglobin"})

    assert query["type"] == "terminal"
    assert query["service"] == "full_text"
    assert query["parameters"]["value"] == "hemoglobin"


def test_rcsbpdb_build_search_query_multiple_nodes(empty_backend):
    query = empty_backend._build_search_query(
        {
            "keywords": "hemoglobin",
            "authors": "Perutz",
            "experimental_method": "X-RAY DIFFRACTION",
        }
    )

    assert query["type"] == "group"
    assert query["logical_operator"] == "and"
    assert len(query["nodes"]) == 3


def test_rcsbpdb_build_search_query_empty(empty_backend):
    assert empty_backend._build_search_query({}) is None


def test_rcsbpdb_validate_params_accepts_supported_keys(empty_backend):
    empty_backend._validate_params(
        {
            "keywords": "hemoglobin",
            "authors": "Perutz",
            "experimental_method": "X-RAY DIFFRACTION",
            "limit": 5,
            "start": 0,
        }
    )


def test_rcsbpdb_validate_params_rejects_unsupported_keys(empty_backend):
    with pytest.raises(ValueError):
        empty_backend._validate_params({"bad_param": "x"})


def test_rcsbpdb_extract_identifiers_from_params(empty_backend):
    identifiers = empty_backend._extract_identifiers_from_params(
        {
            "identifiers": ["1CBS"],
            "pdb_id": "4HHB",
            "doi": "10.2210/pdb2xyz/pdb",
        }
    )

    assert identifiers == ["1CBS", "4HHB", "10.2210/pdb2xyz/pdb"]


def test_rcsbpdb_search_rcsb(empty_backend):
    pdb_ids = empty_backend._search_rcsb({"keywords": "hemoglobin", "limit": 5})

    assert pdb_ids == ["1CBS", "4HHB"]
    assert empty_backend.last_search_response is not None


def test_rcsbpdb_load_from_params_keyword_search(mutable_backend):
    mutable_backend._load_from_params({"keywords": "hemoglobin", "limit": 5})

    assert mutable_backend.identifiers == ["1CBS", "4HHB"]
    assert "datasets" in mutable_backend.list(True)
    assert "resources" in mutable_backend.list(True)

    datasets = mutable_backend.get_table("datasets")
    assert len(datasets) == 2


def test_rcsbpdb_load_from_params_identifier_alias(mutable_backend):
    mutable_backend._load_from_params({"pdb_id": "1CBS"})

    datasets = mutable_backend.get_table("datasets")
    assert len(datasets) == 1
    assert datasets.iloc[0]["dataset_id"] == "1CBS"


def test_rcsbpdb_init_with_params(keyword_backend):
    assert keyword_backend._loaded is True
    assert "datasets" in keyword_backend.list(True)

    datasets = keyword_backend.get_table("datasets")
    assert len(datasets) == 2


# =============================================================================
# 6) Lookup Operations
# =============================================================================

def test_rcsbpdb_lookup_identifier_doi(empty_backend):
    result = empty_backend.lookup_identifier("10.2210/pdb1cbs/pdb")

    assert isinstance(result, RCSBPDBResolution)
    assert result.status == "ok"
    assert result.record_id == "1CBS"
    assert result.doi == "10.2210/pdb1cbs/pdb"
    assert result.repo == "rcsbpdb"
    assert result.metadata_url.endswith("/entry/1CBS")
    assert result.landing_page_url.endswith("/structure/1CBS")


def test_rcsbpdb_lookup_identifier_pdb_id(empty_backend):
    result = empty_backend.lookup_identifier("4hhb")

    assert result.status == "ok"
    assert result.record_id == "4HHB"
    assert result.doi == "10.2210/pdb4hhb/pdb"


def test_rcsbpdb_lookup_identifier_invalid(empty_backend):
    result = empty_backend.lookup_identifier("not-a-pdb-id")

    assert result.status == "skipped"
    assert result.repo == "other"
    assert result.endpoint_used is None
    assert result.notes


def test_rcsbpdb_lookup_rcsbpdb_http_error(empty_backend):
    result = empty_backend.lookup_rcsbpdb(
        pdb_id="9ZZZ",
        original_identifier="9ZZZ",
    )

    assert result.status == "http_error_404"
    assert "RCSB entry metadata request failed." in result.notes


def test_rcsbpdb_direct_pdb_id(loaded_backend):
    datasets = loaded_backend.get_table("datasets")

    one_cbs = datasets[datasets["dataset_id"] == "1CBS"]
    assert len(one_cbs) == 1
    assert one_cbs.iloc[0]["doi"] == "10.2210/pdb1cbs/pdb"


def test_rcsbpdb_errors_table_for_invalid_identifier(invalid_backend):
    errors = invalid_backend.get_table("errors")

    assert isinstance(errors, pd.DataFrame)
    assert len(errors) == 1
    assert errors.iloc[0]["status"] == "skipped"


# =============================================================================
# 7) Find Operations
# =============================================================================

def test_rcsbpdb_find_searches_table_column_and_cell_values(loaded_backend):
    matches = loaded_backend.find("1CBS")

    assert isinstance(matches, list)
    assert len(matches) >= 1
    assert any(match.type == "row" for match in matches)
    assert any(match.t_name == "datasets" for match in matches)


def test_rcsbpdb_find_missing_value_returns_empty_list(loaded_backend):
    matches = loaded_backend.find("definitely-not-present-value")

    assert isinstance(matches, list)
    assert matches == []


def test_rcsbpdb_find_table(loaded_backend):
    matches = loaded_backend.find_table("data")

    assert isinstance(matches, list)
    assert len(matches) >= 1
    assert any(match.t_name == "datasets" for match in matches)

    dataset_match = next(match for match in matches if match.t_name == "datasets")
    assert dataset_match.type == "table"
    assert "dataset_id" in dataset_match.c_name
    assert isinstance(dataset_match.value, OrderedDict)


def test_rcsbpdb_find_column(loaded_backend):
    matches = loaded_backend.find_column("title")

    assert isinstance(matches, list)
    assert len(matches) >= 1
    assert any("title" in match.c_name for match in matches)

    title_match = next(match for match in matches if "title" in match.c_name)
    assert title_match.type == "column"
    assert isinstance(title_match.value, list)
    assert len(title_match.value) == 2


def test_rcsbpdb_find_cell_matches_value(loaded_backend):
    matches = loaded_backend.find_cell("1CBS")

    assert isinstance(matches, list)
    assert len(matches) >= 1
    assert all(match.type == "row" for match in matches)

    dataset_matches = [
        match for match in matches
        if match.t_name == "datasets"
    ]

    assert len(dataset_matches) >= 1
    assert any(
        match.value["dataset_id"] == "1CBS"
        for match in dataset_matches
    )

def test_rcsbpdb_find_cell_cell_type_support(loaded_backend):
    matches = loaded_backend.find_cell("1CBS", row=False)

    assert isinstance(matches, list)
    assert len(matches) >= 1
    assert all(match.type == "cell" for match in matches)

    dataset_matches = [
        match for match in matches
        if match.t_name == "datasets"
    ]

    assert len(dataset_matches) >= 1
    assert any(
        match.c_name == ["dataset_id"] and match.value == "1CBS"
        for match in dataset_matches
    )

def test_rcsbpdb_find_relation_condition_string(loaded_backend):
    matches = loaded_backend.find_relation("dataset_id = 1CBS")

    assert isinstance(matches, list)
    assert len(matches) >= 1

    dataset_matches = [
        match for match in matches
        if match.t_name == "datasets"
    ]

    assert len(dataset_matches) == 1
    assert dataset_matches[0].type == "cell"

    dataset_idx = dataset_matches[0].c_name.index("dataset_id")
    assert dataset_matches[0].value[dataset_idx] == "1CBS"


def test_rcsbpdb_find_relation_condition_split_args(loaded_backend):
    matches = loaded_backend.find_relation("dataset_id", "= 1CBS")

    assert isinstance(matches, list)
    assert len(matches) >= 1

    dataset_matches = [
        match for match in matches
        if match.t_name == "datasets"
    ]

    assert len(dataset_matches) == 1
    assert dataset_matches[0].type == "cell"

    dataset_idx = dataset_matches[0].c_name.index("dataset_id")
    assert dataset_matches[0].value[dataset_idx] == "1CBS"


def test_rcsbpdb_find_relation_contains(loaded_backend):
    matches = loaded_backend.find_relation("title", "~~ structure")

    assert isinstance(matches, list)
    assert len(matches) >= 1
    assert all(match.t_name == "datasets" for match in matches)

    assert any(
        "STRUCTURE" in match.value[match.c_name.index("title")].upper()
        for match in matches
    )


def test_rcsbpdb_find_relation_numeric_condition(loaded_backend):
    matches = loaded_backend.find_relation("resource_count", "> 0")

    assert isinstance(matches, list)
    assert len(matches) == 2
    assert all(match.t_name == "datasets" for match in matches)


def test_rcsbpdb_find_relation_pdb_id(api_backend):
    tables = api_backend.find_relation("1CBS")

    assert isinstance(tables, OrderedDict)
    assert "datasets" in tables
    assert "resources" in tables

    datasets = api_backend.get_table("datasets")
    assert len(datasets) == 1
    assert datasets.iloc[0]["dataset_id"] == "1CBS"


def test_rcsbpdb_find_relation_doi(api_backend):
    tables = api_backend.find_relation("10.2210/pdb1cbs/pdb")

    assert isinstance(tables, OrderedDict)
    assert "datasets" in tables

    datasets = api_backend.get_table("datasets")
    assert len(datasets) == 1
    assert datasets.iloc[0]["dataset_id"] == "1CBS"


def test_rcsbpdb_find_relation_keyword_string(api_backend):
    tables = api_backend.find_relation("hemoglobin", limit=2)

    assert isinstance(tables, OrderedDict)
    assert "datasets" in tables

    datasets = api_backend.get_table("datasets")
    assert len(datasets) == 2


def test_rcsbpdb_find_relation_dict_query(api_backend):
    tables = api_backend.find_relation({"keywords": "hemoglobin", "limit": 2})

    assert isinstance(tables, OrderedDict)
    assert "datasets" in tables

    datasets = api_backend.get_table("datasets")
    assert len(datasets) == 2


def test_rcsbpdb_find_relation_list_input(api_backend):
    tables = api_backend.find_relation(["1CBS", "4HHB"])

    assert isinstance(tables, OrderedDict)
    assert "datasets" in tables

    datasets = api_backend.get_table("datasets")
    assert len(datasets) == 2
    assert set(datasets["dataset_id"]) == {"1CBS", "4HHB"}


def test_rcsbpdb_find_relation_none_returns_tables(api_backend):
    assert api_backend.find_relation(None) == api_backend.tables


def test_rcsbpdb_find_relation_invalid_type_raises(api_backend):
    with pytest.raises(TypeError):
        api_backend.find_relation(123)


# =============================================================================
# 8) URL Validation
# =============================================================================

class FakeResponse:
    def __init__(self, status_code=200, content_type="application/octet-stream"):
        self.status_code = status_code
        self.headers = {"content-type": content_type}

    def close(self):
        pass


class FakeSession:
    def head(self, url, allow_redirects=True, timeout=60, verify=True):
        return FakeResponse(200)

    def get(self, url, stream=True, allow_redirects=True, timeout=60, verify=True):
        return FakeResponse(200)

    def close(self):
        pass


def test_rcsbpdb_validate_urls(mutable_backend):
    mutable_backend.identifiers = ["1CBS"]
    mutable_backend._load_initial_data()
    mutable_backend.session = FakeSession()

    results = mutable_backend.validate_urls("resources")

    assert isinstance(results, list)
    assert len(results) >= 1
    assert all("is_valid" in row for row in results)
    assert all("method_used" in row for row in results)
    assert all(row["is_valid"] is True for row in results)


# =============================================================================
# 9) List, Summary, Display, and Counts
# =============================================================================

def test_rcsbpdb_list_collection_true(loaded_backend):
    table_names = loaded_backend.list(collection=True)

    assert isinstance(table_names, list)
    assert "datasets" in table_names
    assert "resources" in table_names


def test_rcsbpdb_list_print_mode_returns_none(loaded_backend):
    result = loaded_backend.list()

    assert result is None


def test_rcsbpdb_num_tables(loaded_backend):
    count = loaded_backend.num_tables()

    assert isinstance(count, int)
    assert count >= 2


def test_rcsbpdb_summary_all(loaded_backend):
    summary = loaded_backend.summary()

    assert isinstance(summary, list)
    assert len(summary) > 1
    assert isinstance(summary[0], list)

    for summary_df in summary[1:]:
        assert isinstance(summary_df, pd.DataFrame)
        assert "column" in summary_df.columns
        assert "type" in summary_df.columns
        assert "unique" in summary_df.columns


def test_rcsbpdb_summary_single_table(loaded_backend):
    summary = loaded_backend.summary("datasets")

    assert isinstance(summary, pd.DataFrame)
    assert "column" in summary.columns
    assert "type" in summary.columns
    assert "unique" in summary.columns
    assert "dataset_id" in set(summary["column"])


def test_rcsbpdb_display_returns_dataframe(loaded_backend):
    result = loaded_backend.display("datasets", num_rows=1)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1

def test_rcsbpdb_display_cols(loaded_backend):
    result = loaded_backend.display(
        "datasets",
        num_rows=2,
        display_cols=["dataset_id", "title"],
    )

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["dataset_id", "title"]

def test_rcsbpdb_display_requires_table_name(loaded_backend):
    with pytest.raises(TypeError):
        loaded_backend.display()

def test_rcsbpdb_display_missing_table_raises_value_error(empty_backend):
    with pytest.raises(ValueError):
        empty_backend.display("missing_table")


def test_rcsbpdb_display_missing_column_raises_value_error(loaded_backend):
    with pytest.raises(ValueError):
        loaded_backend.display("datasets", display_cols=["missing_column"])


# =============================================================================
# 10) Mutating Helpers and Lifecycle
# =============================================================================

def test_rcsbpdb_close(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=["1CBS"],
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    assert backend._loaded is True
    assert backend.list(True)

    backend.close()

    assert backend._loaded is False
    assert backend.tables == {}
    assert backend.raw_results == []
    assert backend.last_search_response is None
    assert backend.identifiers == []
    assert backend.params == {}


def test_rcsbpdb_context_manager(mocked_rcsb):
    with RCSBPDB(
        identifiers=["1CBS"],
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    ) as backend:
        assert "datasets" in backend.list(True)

    assert backend._loaded is False


# =============================================================================
# 11) Read-only / unsupported operations
# =============================================================================

def test_rcsbpdb_ingest_artifacts_read_only(empty_backend):
    with pytest.raises(NotImplementedError):
        empty_backend.ingest_artifacts({})


def test_rcsbpdb_query_artifacts_not_implemented(empty_backend):
    with pytest.raises(NotImplementedError):
        empty_backend.query_artifacts("10.2210/pdb1cbs/pdb")


def test_rcsbpdb_notebook_not_implemented(empty_backend):
    with pytest.raises(NotImplementedError):
        empty_backend.notebook()