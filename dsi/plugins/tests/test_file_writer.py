from dsi.core import Terminal
from collections import OrderedDict
import git

import dsi.plugins.file_writer as wCSV
import dsi.plugins.file_reader as rCSV

def get_git_root(path):
    git_repo = git.Repo(path, search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")
    return (git_root)

def test_csv_plugin_type():
    path = '/'.join([get_git_root('.'), 'examples/data', 'wildfiredata.csv'])
    plug = rCSV.Csv(filenames=path)
    plug.add_rows()

    #assert type(plug.output_collector) == OrderedDict