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
    path = '/'.join([get_git_root('.'), 'examples/test', 'wildfiredata.sqlite_db'])
    back = Sqlite(filename=path)
    
    #assert type(plug.output_collector) == OrderedDict

def test_er_diagram():
    a=Terminal()
    a.load_module('plugin', 'Schema', 'reader', filename="examples/test/yaml1_circular_schema.json" , target_table_prefix = "student")
    a.load_module('plugin', 'YAML1', 'reader', filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], target_table_prefix = "student")
    a.load_module('plugin', 'TOML1', 'reader', filenames=["examples/test/results.toml"], target_table_prefix = "results")
    a.load_module('plugin', 'ER_Diagram', 'writer', filename = 'erd_test_output.png')
    a.transload()
    
    er_image = cv2.imread("erd_test_output.png")
    assert er_image is not None #check if image generated at all
    
    pixel_mean = np.mean(er_image)
    os.remove("erd_test_output.png")
    assert pixel_mean != 255 #check if image is all white pixels (no diagram generated)

def test_table_plot():
    a=Terminal()
    a.load_module('plugin', 'YAML1', 'reader', filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], target_table_prefix = "student")
    a.load_module('plugin', "Table_Plot", "writer", table_name = "student__physics", filename = "student_physics_plot")
    a.transload()

    plot_image = cv2.imread("student_physics_plot.png")
    assert plot_image is not None #check if image generated at all
    
    pixel_mean = np.mean(plot_image)
    os.remove("student_physics_plot.png")
    assert pixel_mean != 255 #check if image is all white pixels (no diagram generated)

def test_parquet_writer():
    a=Terminal()
    a.load_module('plugin', 'YAML1', 'reader', filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], target_table_prefix = "student")
    a.load_module('plugin', "Parquet_Writer", "writer", table_name = "student__physics", filename = "student_physics_parquet")
    a.transload()

    assert os.path.exists("student_physics_parquet.pq")
    
    a.load_module('plugin', 'Parquet', 'reader', filenames="student_physics_parquet.pq")
    assert "Parquet" in a.active_metadata.keys()
    assert a.active_metadata["Parquet"]["specification"] == ["!amy", "!amy1"]

    os.remove("student_physics_parquet.pq")