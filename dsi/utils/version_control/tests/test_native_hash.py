"""Check the native SHA-256 interface against published digest vectors."""

import mmap

import pytest

from dsi.utils.version_control.storage_backend import sha256


def test_sha256_known_vectors_and_incremental_digest():
    assert sha256().hexdigest() == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    digest = sha256(b"a")
    digest.update(b"b")
    digest.update(b"c")
    expected = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    assert digest.hexdigest() == expected
    assert digest.digest() == bytes.fromhex(expected)
    assert digest.hexdigest() == expected
    digest.update(b"d")
    assert digest.hexdigest() == "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589"


def test_sha256_buffer_inputs_and_invalid_input(tmp_path):
    expected = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    assert sha256(bytearray(b"abc")).hexdigest() == expected
    assert sha256(memoryview(b"abc")).hexdigest() == expected
    path = tmp_path / "data"
    path.write_bytes(b"abc")
    with path.open("rb") as source, mmap.mmap(source.fileno(), 0, access=mmap.ACCESS_READ) as data:
        assert sha256(data).hexdigest() == expected
    with pytest.raises(TypeError):
        sha256("abc")
    with pytest.raises(BufferError):
        sha256(memoryview(b"abcdef")[::2])


def test_federation_folder_name_uses_same_digest(tmp_path):
    from dsi.utils.federation_utils import create_folder_from_path

    name, path = create_folder_from_path("https://example.org/abc", str(tmp_path))
    assert name == "ba7816bf8f01cfea"
    assert path == str(tmp_path / name)
