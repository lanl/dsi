import git
from collections import OrderedDict

from dsi.backends.aws import AWS
import os

def test_aws():
    files = { 'wildfiredata.csv': {'local_path': '/path_to/dsi/examples/wildfire/wildfiredata.csv'} }
     #aws-saml-cmd -r us-gov-west-1 -id your_aws_id
    store = AWS('us-gov-west-1', 'your_aws_id', files)
    store.close()
    assert True
    # No error implies success

def test_aws_put():
    files = { 'wildfiredata.csv': {'local_path': '/path_to/dsi/examples/wildfire/wildfiredata.csv'} }
    store = AWS('us-gov-west-1', 'your_aws_id', files)
    store.put(files['wildfiredata.csv']['local_path'], 's3://your_bucket_name/')
    store.close()
    assert True

def test_aws_get():
    files = { 'wildfiredata.csv': {'local_path': '/path_to/dsi/examples/data/wildfiredata_copied.csv'} }
    store = AWS('us-gov-west-1', 'your_aws_id', files)
    store.get('s3://your_bucket_name/wildfiredata.csv', files['wildfiredata.csv']['local_path'])
    store.close()
    assert True

def test_aws_sync_remote_to_local():
    files = { 'tmp': {'local_path': '/path_to/dsi/dsi/backends/tests'} }
    store = AWS('us-gov-west-1', 'your_aws_id', files)
    store.aws_sync('s3://your_bucket_name/tmp', files['tmp']['local_path'])
    store.close()
    assert True

def test_aws_sync_local_to_remote():
    files = { 'tmp': {'local_path': '/path_to/dsi/dsi/backends/tests/tmp'} }
    store = AWS('us-gov-west-1', 'your_aws_id', files)
    store.aws_sync(files['tmp']['local_path'], 's3://your_bucket_name/tmp')
    store.close()
    assert True
