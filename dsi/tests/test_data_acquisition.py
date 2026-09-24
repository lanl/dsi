import os
import shutil

import pytest

from dsi.utils.data_acquisition import pull_data
from dsi.utils.acquisition.web_utils import get_url_file_size, download_web_file

TEST_URL = "https://oceans11.lanl.gov/dataCatalog/oceans11.db"
TEST_FILENAME = "oceans11.db"


def _url_reachable(url: str) -> bool:
    try:
        return get_url_file_size(url) > 0
    except Exception:
        return False


requires_network = pytest.mark.skipif(
    not _url_reachable(TEST_URL),
    reason=f"{TEST_URL} is not reachable from this environment",
)


# ---------------------------------------------------------------------------
# Live tests against a real, public, password-free URL endpoint
# ---------------------------------------------------------------------------

@requires_network
def test_get_url_file_size_live():
    filesize = get_url_file_size(TEST_URL)
    assert filesize > 0


@requires_network
def test_download_web_file_live(tmp_path):
    output_file = download_web_file(url=TEST_URL, output_dir=str(tmp_path))

    assert output_file.exists()
    assert output_file.name == TEST_FILENAME
    assert output_file.stat().st_size > 0


@requires_network
def test_pull_data_url_live(tmp_path):
    result_path = pull_data(
        location_type="url",
        remote_location="https://oceans11.lanl.gov/dataCatalog",
        remote_path=TEST_URL,
        download_location=str(tmp_path),
        username="",
    )

    assert result_path == str(tmp_path / TEST_FILENAME)
    assert os.path.exists(result_path)
    assert os.path.getsize(result_path) > 0


@requires_network
def test_pull_data_url_redownload_live(tmp_path):
    # Downloading twice to the same location should succeed both times
    # (no stale lock/temp file left behind by the first download).
    first_path = pull_data(
        location_type="url",
        remote_location="https://oceans11.lanl.gov/dataCatalog",
        remote_path=TEST_URL,
        download_location=str(tmp_path),
        username="",
    )
    second_path = pull_data(
        location_type="url",
        remote_location="https://oceans11.lanl.gov/dataCatalog",
        remote_path=TEST_URL,
        download_location=str(tmp_path),
        username="",
    )

    assert first_path == second_path
    assert os.path.exists(second_path)


# ---------------------------------------------------------------------------
# Network-independent tests: bad/unreachable URLs should fail predictably
# ---------------------------------------------------------------------------

def test_get_url_file_size_unreachable_host_returns_zero():
    filesize = get_url_file_size("https://this-host-does-not-exist.invalid/file.db", timeout=3, retries=1)
    assert filesize == 0


def test_pull_data_url_missing_file_raises(tmp_path):
    with pytest.raises(Exception):
        pull_data(
            location_type="url",
            remote_location="https://oceans11.lanl.gov/dataCatalog",
            remote_path="https://oceans11.lanl.gov/dataCatalog/this_file_does_not_exist.db",
            download_location=str(tmp_path),
            username="",
        )


def test_pull_data_empty_remote_path_raises(tmp_path):
    # A remote_path with no path component at all leaves get_last_part()
    # unable to extract a filename, which pull_data() should reject.
    with pytest.raises(ValueError):
        pull_data(
            location_type="url",
            remote_location="https://oceans11.lanl.gov",
            remote_path="https://oceans11.lanl.gov",
            download_location=str(tmp_path),
            username="",
        )
