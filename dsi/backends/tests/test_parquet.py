import git
from collections import OrderedDict

from dsi.backends.parquet import Parquet

isVerbose = True


def get_git_root(path):
    git_repo = git.Repo(path, search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")
    return (git_root)

def test_query_artifacts():
    a = Parquet(filename='/'.join([get_git_root('.'), 'examples/data/wildfiredata.pq']))
    b = a.query_artifacts()
    cnt = 0
    for key in b:
        cnt = cnt + 1
        assert 4 == len(b[key])
    assert 11 == cnt

def test_notebook():
    a = Parquet(filename='/'.join([get_git_root('.'), 'examples/data/wildfiredata.pq']))
    b = a.query_artifacts()
    a.notebook(b)
    # No error on notebook return implies success
    assert True

