"""Tests for the RCSBPDB query_artifacts webserver user API."""

from collections import OrderedDict

import pandas as pd
import pytest
import requests

from dsi.backends.rcsbpdb import FileResource, RCSBPDB, RCSBPDBResolution


TEST_DOIS = ["10.2210/pdb1cbs/pdb", "10.2210/pdb4hhb/pdb"]


def sample_entry_metadata(pdb_id="1CBS", title=None):
    """Small RCSB Data API-like entry payload."""
    title = title or f"Sample structure {pdb_id}"
    return {
        "struct": {"title": title},
        "exptl": [{"method": "X-RAY DIFFRACTION"}],
        "struct_keywords": {
            "pdbx_keywords": "DNA BINDING PROTEIN",
            "text": "sample keywords",
        },
        "rcsb_accession_info": {
            "initial_release_date": "1994-01-31T00:00:00+0000",
            "revision_date": "2020-07-29T00:00:00+0000",
        },
        "citation": [{"pdbx_database_id_DOI": f"10.2210/pdb{pdb_id.lower()}/pdb"}],
        "rcsb_primary_citation": {
            "pdbx_database_id_DOI": f"10.2210/pdb{pdb_id.lower()}/pdb",
            "rcsb_authors": ["Example Author"],
        },
        "pdbx_struct_assembly": [{"id": "1"}],
    }


def mock_request(self, endpoint, params=None):
    """Mock RCSB Data API GET helper."""
    pdb_id = endpoint.rstrip("/").split("/")[-1].upper()
    entries = {
        "1CBS": sample_entry_metadata(
            "1CBS", "CRYSTAL STRUCTURE OF A DNA-BINDING PROTEIN"
        ),
        "4HHB": sample_entry_metadata(
            "4HHB", "THE CRYSTAL STRUCTURE OF HUMAN DEOXYHAEMOGLOBIN"
        ),
        "2XYZ": sample_entry_metadata("2XYZ", "MOCK SEARCH RESULT STRUCTURE"),
    }
    if pdb_id in entries:
        return entries[pdb_id]

    response = requests.Response()
    response.status_code = 404
    raise requests.HTTPError(response=response)


def mock_post_json(self, endpoint, payload):
    """Mock RCSB Search API POST helper."""
    return {"result_set": [{"identifier": "1CBS"}, {"identifier": "4HHB"}]}


def mock_url_exists(self, url):
    return True, 200, "application/octet-stream"


@pytest.fixture(autouse=True)
def mocked_rcsb(monkeypatch):
    """Keep all tests deterministic and offline."""
    monkeypatch.setattr(RCSBPDB, "_request", mock_request)
    monkeypatch.setattr(RCSBPDB, "_post_json", mock_post_json)
    monkeypatch.setattr(RCSBPDB, "_url_exists", mock_url_exists)


@pytest.fixture
def backend():
    """Fresh, connected-but-empty RCSBPDB backend."""
    instance = RCSBPDB(
        validate_on_init=False,
        validate_resource_urls=False,
    )
    yield instance
    instance.close()


# ---------------------------------------------------------------------------
# Initialization and helper behavior
# ---------------------------------------------------------------------------

def test_initialization_creates_empty_ordered_cache(backend):
    assert backend._loaded is True
    assert isinstance(backend._cache, OrderedDict)
    assert backend._cache == OrderedDict()
    assert backend.tables is backend._cache
    assert backend.list(collection=True) == []


def test_initialization_rejects_load_time_params():
    with pytest.raises(ValueError, match="does not retrieve artifacts"):
        RCSBPDB(
            params={"keywords": "hemoglobin"},
            validate_on_init=False,
        )


def test_initialization_rejects_load_time_identifiers():
    with pytest.raises(ValueError, match="does not retrieve artifacts"):
        RCSBPDB(
            identifiers=["1CBS"],
            validate_on_init=False,
        )


def test_only_validate_still_creates_session_and_cache():
    instance = RCSBPDB(only_validate=True)
    try:
        assert hasattr(instance, "session")
        assert isinstance(instance._cache, OrderedDict)
        assert instance._cache == OrderedDict()
    finally:
        instance.close()


def test_normalization_and_identifier_classification():
    assert RCSBPDB.normalize_doi("doi:10.2210/pdb1cbs/pdb") == "10.2210/pdb1cbs/pdb"
    assert RCSBPDB.normalize_pdb_id("4hhb") == "4HHB"
    assert RCSBPDB.classify_identifier("1CBS") == "pdb_id"
    assert RCSBPDB.classify_identifier("10.2210/pdb1cbs/pdb") == "rcsbpdb_doi"
    assert RCSBPDB.classify_identifier("not-a-pdb-id") == "other"


def test_validate_connection(backend):
    class FakeResponse:
        def raise_for_status(self):
            return None

    class FakeSession:
        def get(self, url, timeout=60, verify=True):
            return FakeResponse()

        def close(self):
            return None

    backend.session = FakeSession()
    assert backend.validate_connection() is True


def test_get_schema_describes_webserver_queries(backend):
    schema = backend.get_schema()
    assert "Tier 1" in schema
    assert "Tier 2" in schema
    assert "pandas DataFrame" in schema
    assert "OrderedDict" in schema
    assert "pdb_id" in schema


# ---------------------------------------------------------------------------
# Tiered remote retrieval through query_artifacts
# ---------------------------------------------------------------------------

def test_query_artifacts_discovery_returns_one_dataframe_and_caches_it(backend):
    result = backend.query_artifacts({"keywords": "hemoglobin", "limit": 2})

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == RCSBPDB.DATASET_SCHEMA
    assert set(result["dataset_id"]) == {"1CBS", "4HHB"}
    assert result.attrs["table_name"] == "pdb_datasets_hemoglobin"
    assert list(backend._cache) == ["pdb_datasets_hemoglobin"]
    assert isinstance(backend._cache["pdb_datasets_hemoglobin"], OrderedDict)
    assert backend.get_table("pdb_datasets_hemoglobin").equals(result)
    assert result["resource_count"].isna().all()


def test_query_artifacts_selected_entry_returns_resources_and_retains_discovery(backend):
    discovery = backend.query_artifacts({"keywords": "hemoglobin", "limit": 2})
    resources = backend.query_artifacts({"pdb_id": "4HHB"})

    assert isinstance(resources, pd.DataFrame)
    assert list(resources.columns) == RCSBPDB.RESOURCE_SCHEMA
    assert not resources.empty
    assert set(resources["dataset_id"]) == {"4HHB"}
    assert resources.attrs["table_name"] == "pdb_resources_4hhb"
    assert list(backend._cache) == [
        discovery.attrs["table_name"],
        resources.attrs["table_name"],
    ]
    assert backend.get_table(discovery.attrs["table_name"]).equals(discovery)
    assert backend.get_table(resources.attrs["table_name"]).equals(resources)


def test_query_artifacts_accepts_doi_and_identifier_list(backend):
    one_entry = backend.query_artifacts({"doi": "10.2210/pdb1cbs/pdb"})
    many_entries = backend.query_artifacts({"identifiers": TEST_DOIS})

    assert set(one_entry["dataset_id"]) == {"1CBS"}
    assert set(many_entries["dataset_id"]) == {"1CBS", "4HHB"}
    assert one_entry.attrs["table_name"] == "pdb_resources_1cbs"
    assert many_entries.attrs["table_name"] == "pdb_resources_1cbs_4hhb"


def test_query_artifacts_invalid_identifier_returns_one_errors_dataframe(backend):
    result = backend.query_artifacts({"pdb_id": "not-a-pdb-id"})

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == RCSBPDB.ERROR_SCHEMA
    assert len(result) == 1
    assert result.iloc[0]["status"] == "skipped"
    assert result.attrs["table_name"].endswith("_errors")
    assert result.attrs["table_name"] in backend._cache


def test_query_artifacts_rejects_invalid_dictionary_input(backend):
    with pytest.raises(ValueError, match="Unsupported rcsbpdb search params"):
        backend.query_artifacts({"not_supported": "value"})


def test_query_artifacts_rejects_empty_or_non_query_input(backend):
    with pytest.raises(ValueError, match="dictionary of parameters"):
        backend.query_artifacts("")

    with pytest.raises(ValueError, match="dictionary of parameters"):
        backend.query_artifacts(["1CBS"])


# ---------------------------------------------------------------------------
# Cached-data query behavior and exploration helpers
# ---------------------------------------------------------------------------

def test_query_artifacts_simple_string_uses_find_cell(backend):
    backend.query_artifacts({"keywords": "hemoglobin", "limit": 2})
    matches = backend.query_artifacts("1CBS")

    assert isinstance(matches, list)
    assert matches
    assert all(match.type == "row" for match in matches)
    assert any(match.t_name == "pdb_datasets_hemoglobin" for match in matches)


def test_query_artifacts_relational_string_uses_find_relation(backend):
    backend.query_artifacts({"keywords": "hemoglobin", "limit": 2})
    matches = backend.query_artifacts("dataset_id = 1CBS")

    assert isinstance(matches, list)
    assert len(matches) == 1
    assert matches[0].type == "relation"
    assert matches[0].t_name == "pdb_datasets_hemoglobin"


def test_find_relation_only_filters_cache_and_returns_valueobjects(backend, monkeypatch):
    backend.query_artifacts({"keywords": "hemoglobin", "limit": 2})

    def fail_if_called(*args, **kwargs):
        raise AssertionError("find_relation() must not perform remote API retrieval")

    monkeypatch.setattr(backend, "_post_json", fail_if_called)
    monkeypatch.setattr(backend, "_request", fail_if_called)

    matches = backend.find_relation("dataset_id", "= 1CBS")

    assert isinstance(matches, list)
    assert len(matches) == 1
    assert matches[0].type == "relation"
    assert matches[0].value[matches[0].c_name.index("dataset_id")] == "1CBS"


def test_find_relation_returns_empty_list_when_no_cached_rows_match(backend):
    backend.query_artifacts({"keywords": "hemoglobin", "limit": 2})
    assert backend.find_relation("dataset_id", "= DOES_NOT_EXIST") == []


def test_find_relation_rejects_old_api_retrieval_inputs(backend):
    with pytest.raises(ValueError, match="Could not parse relational query"):
        backend.find_relation("1CBS")

    with pytest.raises(TypeError, match="expects a relational query"):
        backend.find_relation({"keywords": "hemoglobin"})


def test_display_list_and_summary_use_cached_tables(backend):
    datasets = backend.query_artifacts({"keywords": "hemoglobin", "limit": 2})
    resources = backend.query_artifacts({"pdb_id": "1CBS"})

    assert backend.list(collection=True) == [
        datasets.attrs["table_name"],
        resources.attrs["table_name"],
    ]

    displayed = backend.display(
        datasets.attrs["table_name"],
        display_cols=["dataset_id", "title"],
    )

    assert list(displayed.columns) == ["dataset_id", "title"]
    assert displayed.attrs["max_rows"] == len(datasets)

    summary = backend.summary(datasets.attrs["table_name"])

    assert isinstance(summary, pd.DataFrame)
    assert "dataset_id" in set(summary["column"])

def test_process_artifacts_without_arguments_returns_cache(backend):
    result = backend.query_artifacts({"pdb_id": "1CBS"})

    cached_tables = backend.process_artifacts()

    assert cached_tables is backend._cache
    assert list(cached_tables) == [result.attrs["table_name"]]


def test_validate_urls_uses_selected_resource_table(backend, monkeypatch):
    resources = backend.query_artifacts({"pdb_id": "1CBS"})

    class FakeResponse:
        status_code = 200
        headers = {"content-type": "application/octet-stream"}

        def close(self):
            return None

    class FakeSession:
        def head(self, *args, **kwargs):
            return FakeResponse()

        def get(self, *args, **kwargs):
            return FakeResponse()

        def close(self):
            return None

    backend.session = FakeSession()
    checks = backend.validate_urls(resources.attrs["table_name"])
    assert len(checks) == len(resources)
    assert all(check["is_valid"] for check in checks)


# ---------------------------------------------------------------------------
# Existing lookup, resource, and lifecycle behavior
# ---------------------------------------------------------------------------

def test_lookup_identifier_can_skip_or_include_resources(backend):
    discovery_result = backend.lookup_identifier("1CBS", include_resources=False)
    selected_result = backend.lookup_identifier("1CBS", include_resources=True)

    assert isinstance(discovery_result, RCSBPDBResolution)
    assert discovery_result.status == "ok"
    assert discovery_result.resources_loaded is False
    assert discovery_result.files == []
    assert selected_result.resources_loaded is True
    assert len(selected_result.files) >= 8


def test_build_file_resources(backend):
    resources = backend._build_file_resources("1CBS", sample_entry_metadata("1CBS"))
    assert resources
    assert all(isinstance(resource, FileResource) for resource in resources)
    assert {resource.label for resource in resources} >= {"1cbs.cif", "1cbs.pdb"}


def test_close_resets_ordered_cache(backend):
    backend.query_artifacts({"keywords": "hemoglobin", "limit": 2})
    backend.close()

    assert backend._loaded is False
    assert backend._cache == OrderedDict()
    assert backend.tables is backend._cache
    assert backend.raw_results == []
    assert backend.params == {}
    assert backend.identifiers == []


def test_context_manager_closes_backend():
    with RCSBPDB(validate_on_init=False, validate_resource_urls=False) as instance:
        instance.query_artifacts({"keywords": "hemoglobin", "limit": 2})
        assert instance.list(collection=True)

    assert instance._loaded is False


def test_read_only_and_unsupported_operations(backend):
    with pytest.raises(NotImplementedError):
        backend.ingest_artifacts({})

    with pytest.raises(NotImplementedError):
        backend.notebook()

    with pytest.raises(NotImplementedError):
        backend.search({"pdb_id": "1CBS"})
