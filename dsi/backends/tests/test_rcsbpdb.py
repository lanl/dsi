"""
RCSBPDB Backend Function Tests

Tests RCSBPDB backend methods directly without Terminal integration.

Run from the repository root with:
python -m pytest -s dsi/backends/tests/test_rcsbpdb.py
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


@pytest.fixture
def mocked_rcsb(monkeypatch):
    """Patch network helpers so tests are deterministic and offline."""
    monkeypatch.setattr(RCSBPDB, "_request", mock_request)
    monkeypatch.setattr(RCSBPDB, "_post_json", mock_post_json)
    monkeypatch.setattr(RCSBPDB, "_url_exists", mock_url_exists)


@pytest.fixture
def backend(mocked_rcsb):
    """Create an unloaded backend with network validation disabled."""
    b = RCSBPDB(
        auto_load=False,
        validate_on_init=False,
        validate_resource_urls=False,
    )
    yield b
    b.close()


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

def test_rcsbpdb_initialization_no_auto_load(mocked_rcsb):
    backend = RCSBPDB(
        auto_load=False,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    assert backend._loaded is True
    assert backend.get_table_names() == []
    assert backend.get_schema("datasets") == RCSBPDB.DATASET_SCHEMA
    assert backend.get_schema("resources") == RCSBPDB.RESOURCE_SCHEMA
    assert backend.get_schema("errors") == RCSBPDB.ERROR_SCHEMA

    backend.close()


def test_rcsbpdb_initialization_with_identifiers(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=TEST_DOIS,
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    assert backend._loaded is True
    assert "datasets" in backend.get_table_names()
    assert "resources" in backend.get_table_names()

    datasets = backend.get_table("datasets")
    assert isinstance(datasets, pd.DataFrame)
    assert len(datasets) == 2
    assert set(datasets["dataset_id"]) == {"1CBS", "4HHB"}

    backend.close()


def test_rcsbpdb_validate_connection(monkeypatch, mocked_rcsb):
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

def test_rcsbpdb_load_initial_data(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=["10.2210/pdb1cbs/pdb"],
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    assert backend._loaded is True
    assert isinstance(backend.tables, OrderedDict)
    assert "datasets" in backend.tables
    assert "resources" in backend.tables

    dataset_cols = list(backend.tables["datasets"].keys())
    assert dataset_cols == RCSBPDB.DATASET_SCHEMA

    resource_cols = list(backend.tables["resources"].keys())
    assert resource_cols == RCSBPDB.RESOURCE_SCHEMA

    backend.close()


def test_rcsbpdb_process_artifacts(mocked_rcsb):
    backend = RCSBPDB(
        auto_load=False,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    backend.raw_results = [
        backend.lookup_identifier("10.2210/pdb1cbs/pdb"),
        backend.lookup_identifier("not-a-pdb-id"),
    ]

    tables = backend.process_artifacts()

    assert isinstance(tables, OrderedDict)
    assert "datasets" in tables
    assert "resources" in tables
    assert "errors" in tables

    datasets = backend.get_table("datasets")
    errors = backend.get_table("errors")

    assert len(datasets) == 1
    assert len(errors) == 1
    assert errors.iloc[0]["status"] == "skipped"

    backend.close()


def test_rcsbpdb_extract_tables_success_and_error(backend):
    results = [
        backend.lookup_identifier("1CBS"),
        backend.lookup_identifier("bad-id"),
    ]

    tables = backend._extract_tables(results)

    assert set(tables.keys()) == {"datasets", "resources", "errors"}
    assert len(tables["datasets"]) == 1
    assert len(tables["resources"]) >= 1
    assert len(tables["errors"]) == 1

    assert tables["datasets"][0]["dataset_id"] == "1CBS"
    assert tables["errors"][0]["status"] == "skipped"


def test_rcsbpdb_build_file_resources(backend):
    resources = backend._build_file_resources("1CBS", sample_entry_metadata("1CBS"))

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

def test_rcsbpdb_get_table_dataframe(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=TEST_DOIS,
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    datasets = backend.get_table("datasets")
    assert isinstance(datasets, pd.DataFrame)
    assert not datasets.empty
    assert "dataset_id" in datasets.columns
    assert "title" in datasets.columns

    backend.close()


def test_rcsbpdb_get_table_dict_return(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=TEST_DOIS,
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    datasets = backend.get_table("datasets", dict_return=True)
    assert isinstance(datasets, OrderedDict)
    assert "dataset_id" in datasets
    assert datasets["dataset_id"] == ["1CBS", "4HHB"]

    backend.close()


def test_rcsbpdb_get_empty_table_returns_dataframe_with_schema(backend):
    datasets = backend.get_table("datasets")

    assert isinstance(datasets, pd.DataFrame)
    assert datasets.empty
    assert list(datasets.columns) == RCSBPDB.DATASET_SCHEMA


def test_rcsbpdb_get_tables(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=TEST_DOIS,
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    tables = backend.get_tables()

    assert isinstance(tables, OrderedDict)
    assert "datasets" in tables
    assert "resources" in tables

    backend.close()


def test_rcsbpdb_get_schema(backend):
    full_schema = backend.get_schema()
    dataset_schema = backend.get_schema("dataset")
    resource_schema = backend.get_schema("resource")
    error_schema = backend.get_schema("error")

    assert isinstance(full_schema, dict)
    assert dataset_schema == RCSBPDB.DATASET_SCHEMA
    assert resource_schema == RCSBPDB.RESOURCE_SCHEMA
    assert error_schema == RCSBPDB.ERROR_SCHEMA


def test_rcsbpdb_get_table_names(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=TEST_DOIS,
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    names = backend.get_table_names()

    assert isinstance(names, list)
    assert "datasets" in names
    assert "resources" in names

    backend.close()


def test_rcsbpdb_table_name_resolution(backend):
    assert backend._resolve_table_name("dataset") == "datasets"
    assert backend._resolve_table_name("datasets") == "datasets"
    assert backend._resolve_table_name("resource") == "resources"
    assert backend._resolve_table_name("error") == "errors"
    assert backend._resolve_table_name("custom") == "custom"
    assert backend._resolve_table_name(None) is None


# =============================================================================
# 5) Search API / Param Loading
# =============================================================================

def test_rcsbpdb_build_search_query_keywords(backend):
    query = backend._build_search_query({"keywords": "hemoglobin"})

    assert query["type"] == "terminal"
    assert query["service"] == "full_text"
    assert query["parameters"]["value"] == "hemoglobin"


def test_rcsbpdb_build_search_query_multiple_nodes(backend):
    query = backend._build_search_query(
        {
            "keywords": "hemoglobin",
            "authors": "Perutz",
            "experimental_method": "X-RAY DIFFRACTION",
        }
    )

    assert query["type"] == "group"
    assert query["logical_operator"] == "and"
    assert len(query["nodes"]) == 3


def test_rcsbpdb_build_search_query_empty(backend):
    assert backend._build_search_query({}) is None


def test_rcsbpdb_validate_params_accepts_supported_keys(backend):
    backend._validate_params(
        {
            "keywords": "hemoglobin",
            "authors": "Perutz",
            "experimental_method": "X-RAY DIFFRACTION",
            "limit": 5,
            "start": 0,
        }
    )


def test_rcsbpdb_validate_params_rejects_unsupported_keys(backend):
    with pytest.raises(ValueError):
        backend._validate_params({"bad_param": "x"})


def test_rcsbpdb_extract_identifiers_from_params(backend):
    identifiers = backend._extract_identifiers_from_params(
        {
            "identifiers": ["1CBS"],
            "pdb_id": "4HHB",
            "doi": "10.2210/pdb2xyz/pdb",
        }
    )

    assert identifiers == ["1CBS", "4HHB", "10.2210/pdb2xyz/pdb"]


def test_rcsbpdb_search_rcsb(backend):
    pdb_ids = backend._search_rcsb({"keywords": "hemoglobin", "limit": 5})

    assert pdb_ids == ["1CBS", "4HHB"]
    assert backend.last_search_response is not None


def test_rcsbpdb_load_from_params_keyword_search(backend):
    backend._load_from_params({"keywords": "hemoglobin", "limit": 5})

    assert backend.identifiers == ["1CBS", "4HHB"]
    assert "datasets" in backend.get_table_names()
    assert "resources" in backend.get_table_names()

    datasets = backend.get_table("datasets")
    assert len(datasets) == 2


def test_rcsbpdb_load_from_params_identifier_alias(backend):
    backend._load_from_params({"pdb_id": "1CBS"})

    datasets = backend.get_table("datasets")
    assert len(datasets) == 1
    assert datasets.iloc[0]["dataset_id"] == "1CBS"


def test_rcsbpdb_init_with_params(mocked_rcsb):
    backend = RCSBPDB(
        params={"keywords": "hemoglobin", "limit": 2},
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    assert backend._loaded is True
    assert "datasets" in backend.get_table_names()

    datasets = backend.get_table("datasets")
    assert len(datasets) == 2

    backend.close()


# =============================================================================
# 6) Lookup Operations
# =============================================================================

def test_rcsbpdb_lookup_identifier_doi(backend):
    result = backend.lookup_identifier("10.2210/pdb1cbs/pdb")

    assert isinstance(result, RCSBPDBResolution)
    assert result.status == "ok"
    assert result.record_id == "1CBS"
    assert result.doi == "10.2210/pdb1cbs/pdb"
    assert result.repo == "rcsbpdb"
    assert result.metadata_url.endswith("/entry/1CBS")
    assert result.landing_page_url.endswith("/structure/1CBS")


def test_rcsbpdb_lookup_identifier_pdb_id(backend):
    result = backend.lookup_identifier("4hhb")

    assert result.status == "ok"
    assert result.record_id == "4HHB"
    assert result.doi == "10.2210/pdb4hhb/pdb"


def test_rcsbpdb_lookup_identifier_invalid(backend):
    result = backend.lookup_identifier("not-a-pdb-id")

    assert result.status == "skipped"
    assert result.repo == "other"
    assert result.endpoint_used is None
    assert result.notes


def test_rcsbpdb_lookup_rcsbpdb_http_error(backend):
    result = backend.lookup_rcsbpdb(
        pdb_id="9ZZZ",
        original_identifier="9ZZZ",
    )

    assert result.status == "http_error_404"
    assert "RCSB entry metadata request failed." in result.notes


def test_rcsbpdb_direct_pdb_id(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=["1CBS"],
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    datasets = backend.get_table("datasets")

    assert len(datasets) == 1
    assert datasets.iloc[0]["dataset_id"] == "1CBS"
    assert datasets.iloc[0]["doi"] == "10.2210/pdb1cbs/pdb"

    backend.close()


def test_rcsbpdb_errors_table_for_invalid_identifier(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=["not-a-pdb-id"],
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    errors = backend.get_table("errors")

    assert isinstance(errors, pd.DataFrame)
    assert len(errors) == 1
    assert errors.iloc[0]["status"] == "skipped"

    backend.close()


# =============================================================================
# 7) Find Operations
# =============================================================================

def test_rcsbpdb_find_condition(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=TEST_DOIS,
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    matches = backend.find("dataset_id = 1CBS")

    assert isinstance(matches, list)
    assert len(matches) == 1
    assert matches[0].t_name == "datasets"
    assert matches[0].type == "row"
    assert matches[0].value["dataset_id"] == "1CBS"

    backend.close()


def test_rcsbpdb_find_contains(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=TEST_DOIS,
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    matches = backend.find("title ~ hemoglobin")

    assert isinstance(matches, list)
    assert len(matches) >= 1
    assert any(match.t_name == "datasets" for match in matches)

    backend.close()


def test_rcsbpdb_find_numeric_condition(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=TEST_DOIS,
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    matches = backend.find("resource_count > 0")

    assert isinstance(matches, list)
    assert len(matches) == 2

    backend.close()


def test_rcsbpdb_find_invalid_query_returns_empty_list(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=TEST_DOIS,
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    matches = backend.find("hemoglobin")

    assert isinstance(matches, list)
    assert matches == []

    backend.close()


def test_rcsbpdb_find_table(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=TEST_DOIS,
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    matches = backend.find_table("data")

    assert isinstance(matches, list)
    assert len(matches) >= 1
    assert any(match.t_name == "datasets" for match in matches)

    backend.close()


def test_rcsbpdb_find_column(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=TEST_DOIS,
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    matches = backend.find_column("title")

    assert isinstance(matches, list)
    assert len(matches) >= 1
    assert any("title" in match.c_name for match in matches)

    backend.close()


def test_rcsbpdb_find_cell_aliases_find(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=TEST_DOIS,
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    matches = backend.find_cell("dataset_id = 1CBS")

    assert isinstance(matches, list)
    assert len(matches) == 1
    assert matches[0].value["dataset_id"] == "1CBS"

    backend.close()


def test_rcsbpdb_find_relation_condition_string(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=TEST_DOIS,
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    matches = backend.find_relation("dataset_id = 1CBS")

    assert isinstance(matches, list)
    assert len(matches) == 1
    assert matches[0].value["dataset_id"] == "1CBS"

    backend.close()


def test_rcsbpdb_find_relation_condition_split_args(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=TEST_DOIS,
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    matches = backend.find_relation("dataset_id", "= 1CBS")

    assert isinstance(matches, list)
    assert len(matches) == 1
    assert matches[0].value["dataset_id"] == "1CBS"

    backend.close()


def test_rcsbpdb_find_relation_pdb_id(backend):
    tables = backend.find_relation("1CBS")

    assert isinstance(tables, OrderedDict)
    assert "datasets" in tables
    assert "resources" in tables

    datasets = backend.get_table("datasets")
    assert len(datasets) == 1
    assert datasets.iloc[0]["dataset_id"] == "1CBS"


def test_rcsbpdb_find_relation_doi(backend):
    tables = backend.find_relation("10.2210/pdb1cbs/pdb")

    assert isinstance(tables, OrderedDict)
    assert "datasets" in tables

    datasets = backend.get_table("datasets")
    assert len(datasets) == 1
    assert datasets.iloc[0]["dataset_id"] == "1CBS"


def test_rcsbpdb_find_relation_keyword_string(backend):
    tables = backend.find_relation("hemoglobin", limit=2)

    assert isinstance(tables, OrderedDict)
    assert "datasets" in tables

    datasets = backend.get_table("datasets")
    assert len(datasets) == 2


def test_rcsbpdb_find_relation_dict_query(backend):
    tables = backend.find_relation({"keywords": "hemoglobin", "limit": 2})

    assert isinstance(tables, OrderedDict)
    assert "datasets" in tables

    datasets = backend.get_table("datasets")
    assert len(datasets) == 2


def test_rcsbpdb_find_relation_list_input(backend):
    tables = backend.find_relation(["1CBS", "4HHB"])

    assert isinstance(tables, OrderedDict)
    assert "datasets" in tables

    datasets = backend.get_table("datasets")
    assert len(datasets) == 2
    assert set(datasets["dataset_id"]) == {"1CBS", "4HHB"}


def test_rcsbpdb_find_relation_none_returns_tables(backend):
    assert backend.find_relation(None) == backend.tables


def test_rcsbpdb_find_relation_invalid_type_raises(backend):
    with pytest.raises(TypeError):
        backend.find_relation(123)


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


def test_rcsbpdb_validate_urls(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=["1CBS"],
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )
    backend.session = FakeSession()

    results = backend.validate_urls("resources")

    assert isinstance(results, list)
    assert len(results) >= 1
    assert all("is_valid" in row for row in results)
    assert all("method_used" in row for row in results)
    assert all(row["is_valid"] is True for row in results)

    backend.close()


# =============================================================================
# 9) List, Summary, Display, and Counts
# =============================================================================

def test_rcsbpdb_list_collection_true(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=TEST_DOIS,
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    table_names = backend.list(collection=True)

    assert isinstance(table_names, list)
    assert "datasets" in table_names
    assert "resources" in table_names

    backend.close()


def test_rcsbpdb_list_print_mode_returns_none(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=TEST_DOIS,
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    result = backend.list()

    assert result is None

    backend.close()


def test_rcsbpdb_num_tables(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=TEST_DOIS,
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    count = backend.num_tables()

    assert isinstance(count, int)
    assert count >= 2

    backend.close()


def test_rcsbpdb_summary_all(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=TEST_DOIS,
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    summary = backend.summary()

    assert isinstance(summary, list)
    assert len(summary) > 1
    assert isinstance(summary[0], list)

    for summary_df in summary[1:]:
        assert isinstance(summary_df, pd.DataFrame)
        assert "column" in summary_df.columns
        assert "type" in summary_df.columns
        assert "unique" in summary_df.columns

    backend.close()


def test_rcsbpdb_summary_single_table(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=TEST_DOIS,
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    summary = backend.summary("datasets")

    assert isinstance(summary, pd.DataFrame)
    assert "column" in summary.columns
    assert "type" in summary.columns
    assert "unique" in summary.columns
    assert "dataset_id" in set(summary["column"])

    backend.close()


def test_rcsbpdb_display_returns_none(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=TEST_DOIS,
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    result = backend.display("datasets", num_rows=1)

    assert result is None

    backend.close()


def test_rcsbpdb_display_missing_table_returns_none(backend):
    result = backend.display("missing_table")

    assert result is None


def test_rcsbpdb_display_missing_column_returns_none(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=["1CBS"],
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    result = backend.display("datasets", display_cols=["missing_column"])

    assert result is None

    backend.close()


# =============================================================================
# 10) Mutating Helpers and Lifecycle
# =============================================================================

def test_rcsbpdb_overwrite_table(backend):
    new_rows = [
        {
            "dataset_id": "TEST1",
            "doi": "10.2210/pdbtest/pdb",
            "title": "Test Entry",
            "description": "Test Description",
            "landing_page": "https://example.org",
            "metadata_url": "https://example.org/meta",
            "experimental_method": "X-RAY DIFFRACTION",
            "release_date": "2020-01-01",
            "revision_date": "2021-01-01",
            "resource_count": 0,
            "raw_metadata": {},
            "notes": None,
        }
    ]

    backend.overwrite_table("datasets", new_rows)

    datasets = backend.get_table("datasets")

    assert isinstance(datasets, pd.DataFrame)
    assert len(datasets) == 1
    assert datasets.iloc[0]["dataset_id"] == "TEST1"
    assert list(datasets.columns) == RCSBPDB.DATASET_SCHEMA


def test_rcsbpdb_close(mocked_rcsb):
    backend = RCSBPDB(
        identifiers=["1CBS"],
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    assert backend._loaded is True
    assert backend.get_table_names()

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
        assert "datasets" in backend.get_table_names()

    assert backend._loaded is False


# =============================================================================
# 11) Read-only / unsupported operations
# =============================================================================

def test_rcsbpdb_ingest_artifacts_read_only(backend):
    with pytest.raises(NotImplementedError):
        backend.ingest_artifacts({})


def test_rcsbpdb_write_read_only(backend):
    with pytest.raises(NotImplementedError):
        backend.write({})


def test_rcsbpdb_update_read_only(backend):
    with pytest.raises(NotImplementedError):
        backend.update({})


def test_rcsbpdb_query_artifacts_not_implemented(backend):
    with pytest.raises(NotImplementedError):
        backend.query_artifacts("10.2210/pdb1cbs/pdb")


def test_rcsbpdb_query_not_implemented(backend):
    with pytest.raises(NotImplementedError):
        backend.query("SELECT * FROM datasets")


def test_rcsbpdb_notebook_not_implemented(backend):
    with pytest.raises(NotImplementedError):
        backend.notebook()