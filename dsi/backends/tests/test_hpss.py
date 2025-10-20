import git
from collections import OrderedDict

from dsi.backends.hpss import HPSS
import os

def test_hpss():
    files = { 'wildfiredata.csv': {'local_path': '/home/user/dsi/examples/data/wildfiredata.csv', 'local_sha1': '', 'hpss_hash': ''} }
    store = HPSS(files, './tmp')
    store.close()
    assert True
    # No error implies success

def test_hpss_put():
    files = { 'wildfiredata.csv': {'local_path': '/home/user/dsi/examples/data/wildfiredata.csv', 'local_sha1': '', 'hpss_hash': ''} }
    store = HPSS(files, './tmp')
    store.put(files['wildfiredata.csv']['local_path'], '/hpss/user')
    store.close()
    assert True

def test_create_tar():
    files = { 'wildfiredata.csv': {'local_path': '/home/user/dsi/examples/data/wildfiredata.csv', 'local_sha1': '', 'hpss_hash': ''} }
    local_files_to_tar = ['/home/user/dsi/examples/data/wildfiredata.csv']
    store = HPSS(files, './tmp')
    tar_file = "wildfire.tar.gz"
    store.create_tar(tar_file, local_files_to_tar)
    store.close()
    # No error implies success
    assert True
