from dsi.core import Terminal
from collections import OrderedDict
import git

# import dsi.plugins.file_writer as wCSV
from dsi.backends.sqlite import Sqlite
import cv2
import numpy as np
import os

def get_git_root(path):
    git_repo = git.Repo(path, search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")
    return (git_root)

def test_csv_plugin_type():
    path = '/'.join([get_git_root('.'), 'examples/data', 'wildfiredata.sqlite_db'])
    back = Sqlite(filename=path)
    
    #assert type(plug.output_collector) == OrderedDict

def test_export_db_erd():
    a=Terminal(debug_flag=False)
    a.load_module('plugin', 'Schema', 'reader', filename="examples/data/example_schema.json" , target_table_prefix = "student")
    a.load_module('plugin', 'YAML1', 'reader', filenames=["examples/data/student_test1.yml", "examples/data/student_test2.yml"], target_table_prefix = "student")
    a.load_module('plugin', 'TOML1', 'reader', filenames=["examples/data/results.toml"], target_table_prefix = "results")
    a.load_module('plugin', 'ER_Diagram', 'writer', filename = 'erd_test_output.png')
    a.transload()
    
    er_image = cv2.imread("erd_test_output.png")
    assert er_image is not None #check if image generated at all
    
    pixel_mean = np.mean(er_image)
    os.remove("erd_test_output.png")
    assert pixel_mean != 255 #check if image is all white pixels (no diagram generated)
