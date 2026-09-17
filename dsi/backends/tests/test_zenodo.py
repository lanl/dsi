"""
Zenodo Backend Function Tests

Tests Zenodo backend methods directly without Terminal integration.

Run from the repository root with:
python -m pytest -s dsi/backends/tests/test_zenodo.py

The Zenodo backend fixtures are shared across tests wherever possible to avoid
repeated backend initialization and repeated API calls. Network helpers are
mocked so these tests are deterministic and do not depend on live Zenodo
availability, SSL certificates, or rate limits.
"""

from collections import OrderedDict

import pandas as pd
import pytest
import requests

from dsi.backends.zenodo import Zenodo


pytestmark = pytest.mark.filterwarnings(
    "ignore::urllib3.exceptions.InsecureRequestWarning"
)

TEST_RECORD_IDS = ["16537543", "16537544"]
TEST_DOI = "10.5281/zenodo.16537543"


# =============================================================================
# Test fixtures and API mocks
# =============================================================================

def sample_zenodo_record(
    record_id="16537543",
    title=None,
    doi=None,
    file_count=2,
):
    """Small Zenodo Records API-like payload."""
    title = title or f"Sample Zenodo Dataset {record_id}"
    doi = doi or f"10.5281/zenodo.{record_id}"

    files = []

    if file_count >= 1:
        files.append(
            {
                "key": f"sample_{record_id}.csv",
                "size": 1234,
                "checksum": f"md5:{record_id}csvchecksum",
                "mimetype": "text/csv",
                "links": {
                    "self": (
                        "https://zenodo.org/api/records/"
                        f"{record_id}/files/sample_{record_id}.csv/content"
                    )
                },
            }
        )

    if file_count >= 2:
        files.append(
            {
                "key": f"metadata_{record_id}.json",
                "size": 567,
                "checksum": f"md5:{record_id}jsonchecksum",
                "mimetype": "application/json",
                "links": {
                    "self": (
                        "https://zenodo.org/api/records/"
                        f"{record_id}/files/metadata_{record_id}.json/content"
                    )
                },
            }
        )

    return {
        "id": int(record_id),
        "conceptrecid": int(record_id) - 1,
        "metadata": {
            "doi": doi,
            "conceptdoi": f"10.5281/zenodo.{int(record_id) - 1}",
            "title": title,
            "description": f"Description for {title}",
            "publication_date": "2024-01-01",
            "resource_type": {
                "type": "dataset",
                "title": "Dataset",
            },
            "access_right": "open",
            "license": {
                "id": "cc-by-4.0",
                "title": "Creative Commons Attribution 4.0 International",
            },
            "creators": [
                {
                    "name": "Example Author",
                    "affiliation": "Example Lab",
                }
            ],
            "keywords": [
                "climate",
                "test",
                "zenodo",
            ],
            "version": "1.0",
            "communities": [
                {
                    "id": "example-community",
                    "title": "Example Community",
                }
            ],
        },
        "links": {
            "self": f"https://zenodo.org/api/records/{record_id}",
            "self_html": f"https://zenodo.org/records/{record_id}",
            "html": f"https://zenodo.org/records/{record_id}",
        },
        "files": files,
    }


def mock_request(self, url, params=None):
    """Mock Zenodo GET helper used by _request()."""
    params = params or {}

    if url.rstrip("/").endswith("/16537543"):
        return sample_zenodo_record(
            record_id="16537543",
            title="Climate Dataset for Zenodo Backend Tests",
            doi="10.5281/zenodo.16537543",
            file_count=2,
        )

    if url.rstrip("/").endswith("/16537544"):
        return sample_zenodo_record(
            record_id="16537544",
            title="Battery Materials Dataset for Zenodo Backend Tests",
            doi="10.5281/zenodo.16537544",
            file_count=1,
        )

    if "/api/records/" in url and url.rstrip("/").split("/")[-1].isdigit():
        response = requests.Response()
        response.status_code = 404
        raise requests.HTTPError(response=response)

    q = str(params.get("q", "")).lower()
    size = int(params.get("size", 25))

    records = [
        sample_zenodo_record(
            record_id="16537543",
            title="Climate Dataset for Zenodo Backend Tests",
            doi="10.5281/zenodo.16537543",
            file_count=2,
        ),
        sample_zenodo_record(
            record_id="16537544",
            title="Battery Materials Dataset for Zenodo Backend Tests",
            doi="10.5281/zenodo.16537544",
            file_count=1,
        ),
    ]

    if 'doi:"10.5281/zenodo.16537543"' in q:
        records = [records[0]]
    elif 'doi:"10.5281/zenodo.16537544"' in q:
        records = [records[1]]
    elif "zzzzznonexistentkeywordzzzzz" in q:
        records = []
    elif "battery" in q:
        records = [records[1]]
    elif "climate" in q:
        records = [records[0]]

    return {
        "hits": {
            "hits": records[:size],
            "total": len(records[:size]),
        }
    }


def mock_url_exists(self, url):
    """Pretend all generated resource URLs exist."""
    return True, 200, "application/octet-stream"


@pytest.fixture(scope="module", autouse=True)
def mocked_zenodo():
    """Patch network helpers once so tests are deterministic and offline."""
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(Zenodo, "_request", mock_request)
    monkeypatch.setattr(Zenodo, "_url_exists", mock_url_exists)

    yield

    monkeypatch.undo()


@pytest.fixture(scope="module")
def loaded_backend(mocked_zenodo):
    """
    Shared loaded backend for non-destructive tests.

    Do not call close() on this fixture from an individual test. Tests that
    intentionally unload or invalidate backend state should use a dedicated
    fixture instead.
    """
    instance = Zenodo(
        params={
            "record_id": TEST_RECORD_IDS,
        },
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    yield instance

    if instance._loaded:
        instance.close()


@pytest.fixture(scope="module")
def keyword_backend(mocked_zenodo):
    """Shared backend loaded through keyword search."""
    instance = Zenodo(
        params={
            "keywords": "climate",
            "limit": 2,
        },
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    yield instance

    if instance._loaded:
        instance.close()


@pytest.fixture(scope="module")
def empty_result_backend(mocked_zenodo):
    """Shared backend for an empty search result."""
    instance = Zenodo(
        params={
            "keywords": "zzzzznonexistentkeywordzzzzz",
            "limit": 2,
        },
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    yield instance

    if instance._loaded:
        instance.close()


@pytest.fixture(scope="module")
def empty_backend(mocked_zenodo):
    """Shared backend with empty initialized tables for helper tests."""
    instance = Zenodo(
        auto_load=False,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    yield instance

    if instance._loaded:
        instance.close()


@pytest.fixture(scope="module")
def mutable_backend(mocked_zenodo):
    """Shared backend for tests that do not need loaded table state."""
    instance = Zenodo(
        auto_load=False,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    yield instance

    if instance._loaded:
        instance.close()


@pytest.fixture(scope="module")
def api_backend(mocked_zenodo):
    """
    Shared backend for API-backed find_relation tests.

    These tests intentionally reload this backend with different parameters,
    but do not close it or leave it invalidated.
    """
    instance = Zenodo(
        auto_load=False,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    yield instance

    if instance._loaded:
        instance.close()


@pytest.fixture
def error_backend(mocked_zenodo):
    """
    Dedicated backend for tests that intentionally raise from methods.

    This avoids invalidating shared fixtures such as loaded_backend.
    """
    instance = Zenodo(
        params={
            "record_id": TEST_RECORD_IDS,
        },
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    yield instance

    if instance._loaded:
        instance.close()


# =============================================================================
# 1) Normalization and utility helpers
# =============================================================================

def test_zenodo_normalize_doi():
    assert Zenodo.normalize_doi("https://doi.org/10.5281/zenodo.16537543") == TEST_DOI
    assert Zenodo.normalize_doi("doi:10.5281/zenodo.16537543") == TEST_DOI
    assert Zenodo.normalize_doi("DOI:10.5281/zenodo.16537543") == TEST_DOI
    assert Zenodo.normalize_doi("no doi here") is None
    assert Zenodo.normalize_doi(None) is None


def test_zenodo_normalize_record_id():
    assert Zenodo.normalize_record_id("16537543") == "16537543"
    assert Zenodo.normalize_record_id(16537543) == "16537543"
    assert Zenodo.normalize_record_id("not-a-record-id") is None
    assert Zenodo.normalize_record_id(None) is None


def test_zenodo_extract_record_id_from_doi():
    assert Zenodo.extract_record_id_from_doi(TEST_DOI) == "16537543"
    assert Zenodo.extract_record_id_from_doi("10.1234/example") is None
    assert Zenodo.extract_record_id_from_doi(None) is None


def test_zenodo_get_file_ext():
    assert Zenodo.get_file_ext("file.csv") == "csv"
    assert Zenodo.get_file_ext("file.csv.gz") == "csv.gz"
    assert Zenodo.get_file_ext("archive.tar.gz") == "tar.gz"
    assert Zenodo.get_file_ext("https://example.org/file.json?download=1") == "json"
    assert Zenodo.get_file_ext("no_extension") is None
    assert Zenodo.get_file_ext(None) is None


def test_zenodo_classify_usability():
    assert Zenodo.classify_usability(["csv"]) == "tabular_or_easy_parse"
    assert Zenodo.classify_usability(["nc"]) == "scientific_structured"
    assert Zenodo.classify_usability(["zip"]) == "archive_only"
    assert Zenodo.classify_usability(["pdf"]) == "document_only"
    assert Zenodo.classify_usability(["abc"]) == "other_format"
    assert Zenodo.classify_usability([]) == "unknown_format"


def test_zenodo_json_or_none(empty_backend):
    assert empty_backend._json_or_none(None) is None
    assert empty_backend._json_or_none({"b": 2, "a": 1}) == '{"a": 1, "b": 2}'


# =============================================================================
# 2) Basic Backend Initialization
# =============================================================================

def test_zenodo_initialization_no_auto_load(empty_backend):
    assert empty_backend._loaded is True
    assert isinstance(empty_backend._cache, OrderedDict)
    assert isinstance(empty_backend.tables, OrderedDict)

    table_names = empty_backend.list(collection=True)

    assert table_names == ["datasets", "resources"]

    datasets = empty_backend.get_table("datasets")
    resources = empty_backend.get_table("resources")

    assert isinstance(datasets, pd.DataFrame)
    assert isinstance(resources, pd.DataFrame)

    assert datasets.empty
    assert resources.empty

    assert list(datasets.columns) == Zenodo.DATASET_COLUMNS
    assert list(resources.columns) == Zenodo.RESOURCE_COLUMNS


def test_zenodo_only_validate_initializes_without_loading(mocked_zenodo):
    backend = Zenodo(
        only_validate=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    assert backend._loaded is False
    assert backend.list(collection=True) == ["datasets", "resources"]
    assert backend.validate_error_msg is None

    backend.close()


def test_zenodo_initialization_with_record_ids(loaded_backend):
    assert loaded_backend._loaded is True
    assert "datasets" in loaded_backend.list(collection=True)
    assert "resources" in loaded_backend.list(collection=True)

    datasets = loaded_backend.get_table("datasets")

    assert isinstance(datasets, pd.DataFrame)
    assert len(datasets) == 2
    assert set(datasets["dataset_id"]) == {"16537543", "16537544"}


def test_zenodo_initialization_with_keyword_search(keyword_backend):
    assert keyword_backend._loaded is True
    assert "datasets" in keyword_backend.list(collection=True)

    datasets = keyword_backend.get_table("datasets")

    assert isinstance(datasets, pd.DataFrame)
    assert len(datasets) >= 1
    assert any("Climate" in title for title in datasets["title"])


def test_zenodo_invalid_base_url():
    with pytest.raises(ValueError):
        Zenodo(
            url="not-a-valid-url",
            auto_load=False,
            validate_on_init=False,
        )


def test_zenodo_validate_connection(mocked_zenodo):
    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "hits": {
                    "hits": [],
                    "total": 0,
                }
            }

    class FakeSession:
        def get(self, url, params=None, timeout=60, verify=True):
            return FakeResponse()

        def close(self):
            pass

    backend = Zenodo(
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

def test_zenodo_load_initial_data(loaded_backend):
    assert loaded_backend._loaded is True
    assert isinstance(loaded_backend.tables, OrderedDict)

    assert "datasets" in loaded_backend.tables
    assert "resources" in loaded_backend.tables

    dataset_cols = list(loaded_backend.tables["datasets"].keys())
    resource_cols = list(loaded_backend.tables["resources"].keys())

    assert dataset_cols == Zenodo.DATASET_COLUMNS
    assert resource_cols == Zenodo.RESOURCE_COLUMNS


def test_zenodo_datasets_resources_relationship(loaded_backend):
    datasets = loaded_backend.get_table("datasets")
    resources = loaded_backend.get_table("resources")

    assert not datasets.empty
    assert not resources.empty

    dataset_ids = set(datasets["dataset_id"])
    resource_dataset_ids = set(resources["dataset_id"])

    assert resource_dataset_ids.issubset(dataset_ids)


def test_zenodo_process_artifacts(loaded_backend):
    tables = loaded_backend.process_artifacts()

    assert isinstance(tables, OrderedDict)
    assert "datasets" in tables
    assert "resources" in tables
    assert isinstance(tables["datasets"], OrderedDict)
    assert isinstance(tables["resources"], OrderedDict)


def test_zenodo_extract_tables(empty_backend):
    records = [
        sample_zenodo_record(
            record_id="16537543",
            title="Climate Dataset for Zenodo Backend Tests",
            doi=TEST_DOI,
            file_count=2,
        )
    ]

    dataset_rows, resource_rows = empty_backend._extract_tables(records)

    assert isinstance(dataset_rows, list)
    assert isinstance(resource_rows, list)

    assert len(dataset_rows) == 1
    assert len(resource_rows) == 2

    assert dataset_rows[0]["dataset_id"] == "16537543"
    assert dataset_rows[0]["source_repository"] == "Zenodo"
    assert dataset_rows[0]["resource_count"] == 2
    assert dataset_rows[0]["raw_metadata"] is not None

    assert resource_rows[0]["dataset_id"] == "16537543"
    assert resource_rows[0]["source_repository"] == "Zenodo"
    assert resource_rows[0]["format"] == "csv"


def test_zenodo_extract_resource_rows(empty_backend):
    record = sample_zenodo_record(
        record_id="16537543",
        title="Climate Dataset for Zenodo Backend Tests",
        doi=TEST_DOI,
        file_count=2,
    )

    rows, exts = empty_backend._extract_resource_rows(
        rec=record,
        record_id="16537543",
        title="Climate Dataset for Zenodo Backend Tests",
        md=record["metadata"],
    )

    assert isinstance(rows, list)
    assert isinstance(exts, list)

    assert len(rows) == 2
    assert set(exts) == {"csv", "json"}

    assert rows[0]["resource_id"] == "16537543:1"
    assert rows[0]["dataset_id"] == "16537543"
    assert rows[0]["download_url"] is not None


# =============================================================================
# 4) Table Accessors
# =============================================================================

def test_zenodo_get_table_dataframe(loaded_backend):
    datasets = loaded_backend.get_table("datasets")

    assert isinstance(datasets, pd.DataFrame)
    assert not datasets.empty
    assert "dataset_id" in datasets.columns
    assert "title" in datasets.columns


def test_zenodo_get_table_dict_return(loaded_backend):
    datasets = loaded_backend.get_table("datasets", dict_return=True)

    assert isinstance(datasets, OrderedDict)
    assert "dataset_id" in datasets
    assert datasets["dataset_id"] == ["16537543", "16537544"]


def test_zenodo_get_empty_table_returns_dataframe_with_schema(empty_backend):
    datasets = empty_backend.get_table("datasets")
    resources = empty_backend.get_table("resources")

    assert isinstance(datasets, pd.DataFrame)
    assert isinstance(resources, pd.DataFrame)

    assert datasets.empty
    assert resources.empty

    assert list(datasets.columns) == Zenodo.DATASET_COLUMNS
    assert list(resources.columns) == Zenodo.RESOURCE_COLUMNS


def test_zenodo_get_table_invalid_name_raises(loaded_backend):
    with pytest.raises(ValueError):
        loaded_backend.get_table("missing_table")


def test_zenodo_get_tables(loaded_backend):
    tables = loaded_backend.get_tables()

    assert isinstance(tables, OrderedDict)
    assert "datasets" in tables
    assert "resources" in tables


def test_zenodo_get_tables_as_dataframes(loaded_backend):
    tables = loaded_backend.get_tables_as_dataframes()

    assert isinstance(tables, dict)
    assert "datasets" in tables
    assert "resources" in tables

    assert isinstance(tables["datasets"], pd.DataFrame)
    assert isinstance(tables["resources"], pd.DataFrame)


def test_zenodo_get_schema(loaded_backend):
    schema = loaded_backend.get_schema()

    assert isinstance(schema, str)
    assert "CREATE TABLE datasets" in schema
    assert "CREATE TABLE resources" in schema
    assert "dataset_id" in schema
    assert "resource_id" in schema


def test_zenodo_table_name_resolution(loaded_backend):
    assert loaded_backend._resolve_table_name("datasets") == "datasets"
    assert loaded_backend._resolve_table_name("resources") == "resources"

    with pytest.raises(ValueError):
        loaded_backend._resolve_table_name("missing_table")


# =============================================================================
# 5) Search API / Param Loading
# =============================================================================

def test_zenodo_build_search_query_keywords(empty_backend):
    query = empty_backend._build_search_query({"keywords": "climate"})

    assert query == "climate"


def test_zenodo_build_search_query_combined(empty_backend):
    query = empty_backend._build_search_query(
        {
            "keywords": "climate",
            "communities": "example-community",
            "resource_type": "dataset",
            "access_right": "open",
        }
    )

    assert "climate" in query
    assert "communities:example-community" in query
    assert "resource_type.type:dataset" in query
    assert "access_right:open" in query
    assert " AND " in query


def test_zenodo_build_search_query_empty(empty_backend):
    assert empty_backend._build_search_query({}) is None


def test_zenodo_validate_params_accepts_supported_keys(empty_backend):
    empty_backend._validate_params(
        {
            "keywords": "climate",
            "q": "climate",
            "doi": TEST_DOI,
            "record_id": "16537543",
            "limit": 5,
            "size": 5,
            "page": 1,
            "sort": "mostrecent",
            "communities": "example-community",
            "resource_type": "dataset",
            "access_right": "open",
        }
    )


def test_zenodo_validate_params_rejects_unsupported_keys(empty_backend):
    with pytest.raises(ValueError):
        empty_backend._validate_params({"bad_param": "x"})


def test_zenodo_search_records(empty_backend):
    records = empty_backend._search_records(
        {
            "keywords": "climate",
            "limit": 2,
        }
    )

    assert isinstance(records, list)
    assert len(records) == 1
    assert records[0]["id"] == 16537543
    assert empty_backend.last_search_response is not None


def test_zenodo_search_records_rejects_bad_limit(empty_backend):
    with pytest.raises(ValueError):
        empty_backend._search_records(
            {
                "keywords": "climate",
                "limit": "not-an-int",
            }
        )


def test_zenodo_search_records_rejects_zero_limit(empty_backend):
    with pytest.raises(ValueError):
        empty_backend._search_records(
            {
                "keywords": "climate",
                "limit": 0,
            }
        )


def test_zenodo_search_records_rejects_zero_page(empty_backend):
    with pytest.raises(ValueError):
        empty_backend._search_records(
            {
                "keywords": "climate",
                "page": 0,
            }
        )


# =============================================================================
# 6) Lookup Operations
# =============================================================================

def test_zenodo_lookup_record(empty_backend):
    record = empty_backend._lookup_record("16537543")

    assert isinstance(record, dict)
    assert record["id"] == 16537543
    assert record["metadata"]["doi"] == TEST_DOI


def test_zenodo_lookup_record_missing_returns_none(empty_backend):
    record = empty_backend._lookup_record("999999999999")

    assert record is None


def test_zenodo_records_from_params_record_id(empty_backend):
    records = empty_backend._records_from_params(
        {
            "record_id": "16537543",
        }
    )

    assert isinstance(records, list)
    assert len(records) == 1
    assert records[0]["id"] == 16537543


def test_zenodo_records_from_params_record_id_list(empty_backend):
    records = empty_backend._records_from_params(
        {
            "record_id": ["16537543", "16537544"],
        }
    )

    assert isinstance(records, list)
    assert len(records) == 2
    assert {record["id"] for record in records} == {16537543, 16537544}


def test_zenodo_records_from_params_invalid_record_id(empty_backend):
    with pytest.raises(ValueError):
        empty_backend._records_from_params(
            {
                "record_id": "not-a-record-id",
            }
        )


def test_zenodo_records_from_params_doi(empty_backend):
    records = empty_backend._records_from_params(
        {
            "doi": TEST_DOI,
        }
    )

    assert isinstance(records, list)
    assert len(records) == 1
    assert records[0]["id"] == 16537543


def test_zenodo_records_from_params_invalid_doi(empty_backend):
    with pytest.raises(ValueError):
        empty_backend._records_from_params(
            {
                "doi": "bad-doi",
            }
        )


def test_zenodo_search_by_exact_doi(empty_backend):
    records = empty_backend._search_by_exact_doi(TEST_DOI)

    assert isinstance(records, list)
    assert len(records) == 1
    assert records[0]["id"] == 16537543


# =============================================================================
# 7) Query Artifacts
# =============================================================================

def test_zenodo_query_artifacts_not_implemented(loaded_backend):
    with pytest.raises(NotImplementedError):
        loaded_backend.query_artifacts("resource_count >= 1")


def test_zenodo_query_artifacts_none_not_implemented(loaded_backend):
    with pytest.raises(NotImplementedError):
        loaded_backend.query_artifacts(None)


def test_zenodo_query_artifacts_dict_not_implemented(mutable_backend):
    with pytest.raises(NotImplementedError):
        mutable_backend.query_artifacts(
            {
                "record_id": "16537543",
            }
        )


# =============================================================================
# 8) Find Operations
# =============================================================================

def test_zenodo_find_searches_table_column_and_cell_values(loaded_backend):
    matches = loaded_backend.find("16537543")

    assert isinstance(matches, list)
    assert len(matches) >= 1
    assert any(match.t_name == "datasets" for match in matches)


def test_zenodo_find_missing_value_returns_empty_list(loaded_backend):
    matches = loaded_backend.find("definitely-not-present-value")

    assert isinstance(matches, list)
    assert matches == []


def test_zenodo_find_table(loaded_backend):
    matches = loaded_backend.find_table("data")

    assert isinstance(matches, list)
    assert len(matches) >= 1
    assert any(match.t_name == "datasets" for match in matches)

    dataset_match = next(match for match in matches if match.t_name == "datasets")
    assert dataset_match.type == "table"
    assert "dataset_id" in dataset_match.c_name
    assert isinstance(dataset_match.value, OrderedDict)


def test_zenodo_find_column(loaded_backend):
    matches = loaded_backend.find_column("title")

    assert isinstance(matches, list)
    assert len(matches) >= 1
    assert any("title" in match.c_name for match in matches)

    title_match = next(match for match in matches if "title" in match.c_name)
    assert title_match.type == "column"
    assert isinstance(title_match.value, list)


def test_zenodo_find_column_with_range(loaded_backend):
    matches = loaded_backend.find_column("resource_count", range=True)

    assert isinstance(matches, list)
    assert len(matches) >= 1

    match = next(item for item in matches if "resource_count" in item.c_name)

    assert match.type == "column"
    assert isinstance(match.value, dict)
    assert "min" in match.value
    assert "max" in match.value


def test_zenodo_find_cell_matches_value(loaded_backend):
    matches = loaded_backend.find_cell("16537543")

    assert isinstance(matches, list)
    assert len(matches) >= 1
    assert all(match.type == "row" for match in matches)

    dataset_matches = [
        match for match in matches
        if match.t_name == "datasets"
    ]

    assert len(dataset_matches) >= 1
    assert any(match.value["dataset_id"] == "16537543" for match in dataset_matches)

def test_zenodo_find_cell_cell_type_support(loaded_backend):
    matches = loaded_backend.find_cell("16537543", row=False)

    assert isinstance(matches, list)
    assert len(matches) >= 1
    assert all(match.type == "cell" for match in matches)

    dataset_matches = [
        match for match in matches
        if match.t_name == "datasets"
    ]

    assert len(dataset_matches) >= 1
    assert any(
        match.c_name == ["dataset_id"] and match.value == "16537543"
        for match in dataset_matches
    )

def test_zenodo_find_cell_rows_are_unique(loaded_backend):
    matches = loaded_backend.find_cell("Zenodo")

    assert isinstance(matches, list)

    row_keys = [
        (match.t_name, match.row_num)
        for match in matches
    ]

    assert len(row_keys) == len(set(row_keys))
    assert all(match.type == "row" for match in matches)


def test_zenodo_find_relation_condition_string_local(loaded_backend):
    matches = loaded_backend.find_relation("resource_count >= 1")

    assert isinstance(matches, list)
    assert len(matches) >= 1
    assert all(match.t_name == "datasets" for match in matches)
    assert all(match.type == "cell" for match in matches)
    assert all(isinstance(match.value, list) for match in matches)


def test_zenodo_find_relation_condition_split_args_local(loaded_backend):
    matches = loaded_backend.find_relation("resource_count", ">= 1")

    assert isinstance(matches, list)
    assert len(matches) >= 1
    assert all(match.t_name == "datasets" for match in matches)
    assert all(match.type == "cell" for match in matches)


def test_zenodo_find_relation_contains(loaded_backend):
    matches = loaded_backend.find_relation("title", "~~ climate")

    assert isinstance(matches, list)
    assert len(matches) >= 1
    assert all(match.t_name == "datasets" for match in matches)
    assert all(match.type == "cell" for match in matches)

    title_index = Zenodo.DATASET_COLUMNS.index("title")

    assert any(
        "CLIMATE" in str(match.value[title_index]).upper()
        for match in matches
    )


def test_zenodo_find_relation_numeric_resource_size(loaded_backend):
    matches = loaded_backend.find_relation("size", "> 0")

    assert isinstance(matches, list)
    assert len(matches) >= 1
    assert all(match.t_name == "resources" for match in matches)
    assert all(match.type == "cell" for match in matches)


def test_zenodo_find_relation_format_exact(loaded_backend):
    matches = loaded_backend.find_relation("format", "= csv")

    assert isinstance(matches, list)
    assert len(matches) >= 1
    assert all(match.t_name == "resources" for match in matches)
    assert all(match.type == "cell" for match in matches)

    format_index = Zenodo.RESOURCE_COLUMNS.index("format")

    assert all(match.value[format_index] == "csv" for match in matches)


def test_zenodo_find_relation_api_record_id(api_backend):
    tables = api_backend.find_relation("16537543")

    assert isinstance(tables, OrderedDict)
    assert "datasets" in tables
    assert "resources" in tables

    datasets = api_backend.get_table("datasets")
    assert len(datasets) == 1
    assert datasets.iloc[0]["dataset_id"] == "16537543"


def test_zenodo_find_relation_api_doi(api_backend):
    tables = api_backend.find_relation(TEST_DOI)

    assert isinstance(tables, OrderedDict)
    assert "datasets" in tables

    datasets = api_backend.get_table("datasets")
    assert len(datasets) == 1
    assert datasets.iloc[0]["doi"] == TEST_DOI


def test_zenodo_find_relation_api_keywords(api_backend):
    tables = api_backend.find_relation(
        {
            "keywords": "climate",
            "limit": 2,
        }
    )

    assert isinstance(tables, OrderedDict)
    assert "datasets" in tables

    datasets = api_backend.get_table("datasets")
    assert len(datasets) >= 1
    assert any("Climate" in title for title in datasets["title"])


def test_zenodo_find_relation_api_q(api_backend):
    tables = api_backend.find_relation(
        {
            "q": "battery",
            "limit": 2,
        }
    )

    assert isinstance(tables, OrderedDict)
    assert "datasets" in tables

    datasets = api_backend.get_table("datasets")
    assert len(datasets) == 1
    assert datasets.iloc[0]["dataset_id"] == "16537544"


def test_zenodo_find_relation_plain_string_searches_api(api_backend):
    tables = api_backend.find_relation("climate", limit=2)

    assert isinstance(tables, OrderedDict)
    assert "datasets" in tables

    datasets = api_backend.get_table("datasets")
    assert len(datasets) >= 1
    assert any("Climate" in title for title in datasets["title"])


def test_zenodo_find_relation_api_record_id_list(api_backend):
    tables = api_backend.find_relation(["16537543", "16537544"])

    assert isinstance(tables, OrderedDict)
    assert "datasets" in tables
    assert "resources" in tables

    datasets = api_backend.get_table("datasets")
    assert len(datasets) == 2
    assert set(datasets["dataset_id"]) == {"16537543", "16537544"}


def test_zenodo_find_relation_invalid_relation_raises(error_backend):
    with pytest.raises(ValueError):
        error_backend.find_relation("resource_count", "bad relation")


# =============================================================================
# 9) URL Validation
# =============================================================================

class FakeResponse:
    def __init__(self, status_code=200, content_type="application/octet-stream"):
        self.status_code = status_code
        self.headers = {
            "content-type": content_type,
        }

    def close(self):
        pass


class FakeSession:
    def head(self, url, allow_redirects=True, timeout=60, verify=True):
        return FakeResponse(200)

    def get(self, url, stream=True, allow_redirects=True, timeout=60, verify=True):
        return FakeResponse(200)

    def close(self):
        pass


def test_zenodo_url_exists(loaded_backend):
    original_session = loaded_backend.session
    loaded_backend.session = FakeSession()

    try:
        exists, status_code, content_type = loaded_backend._url_exists(
            "https://zenodo.org/example-file.csv"
        )
    finally:
        loaded_backend.session = original_session

    assert exists is True
    assert status_code == 200
    assert content_type == "application/octet-stream"


def test_zenodo_validate_urls(loaded_backend):
    original_session = loaded_backend.session
    loaded_backend.session = FakeSession()

    try:
        results = loaded_backend.validate_urls()
    finally:
        loaded_backend.session = original_session

    assert isinstance(results, list)
    assert len(results) >= 1
    assert all(isinstance(value, dict) for value in results)
    assert all("is_valid" in value for value in results)
    assert all("status_code" in value for value in results)
    assert all(value["is_valid"] is True for value in results)

    resources = loaded_backend.get_table("resources")

    assert "url_valid" in resources.columns
    assert all(resources["url_valid"] == True)


# =============================================================================
# 10) List, Summary, Display, and Counts
# =============================================================================

def test_zenodo_list_collection_true(loaded_backend):
    table_names = loaded_backend.list(collection=True)

    assert isinstance(table_names, list)
    assert table_names == ["datasets", "resources"]


def test_zenodo_list_print_mode_returns_none(loaded_backend):
    result = loaded_backend.list()

    assert result is None


def test_zenodo_num_tables(loaded_backend):
    count = loaded_backend.num_tables()

    assert isinstance(count, int)
    assert count == 2


def test_zenodo_summary_all(loaded_backend):
    summary = loaded_backend.summary()

    assert isinstance(summary, list)
    assert len(summary) == 3
    assert isinstance(summary[0], list)
    assert summary[0] == ["datasets", "resources"]

    for summary_df in summary[1:]:
        assert isinstance(summary_df, pd.DataFrame)
        assert "table_name" in summary_df.columns
        assert "num_rows" in summary_df.columns
        assert "num_columns" in summary_df.columns
        assert "columns" in summary_df.columns


def test_zenodo_summary_single_table(loaded_backend):
    summary = loaded_backend.summary("datasets")

    assert isinstance(summary, pd.DataFrame)
    assert "table_name" in summary.columns
    assert "num_rows" in summary.columns
    assert "num_columns" in summary.columns
    assert "columns" in summary.columns

    assert summary.iloc[0]["table_name"] == "datasets"


def test_zenodo_summary_invalid_table_raises(loaded_backend):
    with pytest.raises(ValueError):
        loaded_backend.summary("missing_table")


def test_zenodo_display_returns_dataframe(loaded_backend):
    result = loaded_backend.display("datasets", num_rows=1)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1


def test_zenodo_display_cols(loaded_backend):
    result = loaded_backend.display(
        "datasets",
        num_rows=2,
        display_cols=["dataset_id", "title"],
    )

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["dataset_id", "title"]


def test_zenodo_display_missing_table_raises_value_error(loaded_backend):
    with pytest.raises(ValueError):
        loaded_backend.display("missing_table")


def test_zenodo_display_missing_column_raises_value_error(loaded_backend):
    with pytest.raises(ValueError):
        loaded_backend.display("datasets", display_cols=["missing_column"])


# =============================================================================
# 11) Mutating Helpers and Lifecycle
# =============================================================================

def test_zenodo_close(mocked_zenodo):
    backend = Zenodo(
        params={
            "record_id": "16537543",
        },
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    )

    assert backend._loaded is True
    assert backend.list(collection=True)

    backend.close()

    assert backend._loaded is False
    assert backend._cache == OrderedDict()
    assert backend.tables == OrderedDict()
    assert backend.raw_records == []
    assert backend.last_search_response is None
    assert backend.last_request_params is None
    assert backend.params == {}


def test_zenodo_context_manager(mocked_zenodo):
    with Zenodo(
        params={
            "record_id": "16537543",
        },
        auto_load=True,
        validate_on_init=False,
        validate_resource_urls=False,
    ) as backend:
        assert "datasets" in backend.list(collection=True)

    assert backend._loaded is False


# =============================================================================
# 12) Read-only / unsupported operations
# =============================================================================

def test_zenodo_ingest_artifacts_read_only(empty_backend):
    with pytest.raises(NotImplementedError):
        empty_backend.ingest_artifacts({})


def test_zenodo_notebook_not_implemented(empty_backend):
    with pytest.raises(NotImplementedError):
        empty_backend.notebook()


# =============================================================================
# 13) Empty Results
# =============================================================================

def test_zenodo_empty_results(empty_result_backend):
    datasets = empty_result_backend.get_table("datasets")
    resources = empty_result_backend.get_table("resources")

    assert isinstance(datasets, pd.DataFrame)
    assert isinstance(resources, pd.DataFrame)

    assert datasets.empty
    assert resources.empty

    assert list(datasets.columns) == Zenodo.DATASET_COLUMNS
    assert list(resources.columns) == Zenodo.RESOURCE_COLUMNS