from dsi.plugins import file_writer as fw
from dsi.core import Terminal

import cv2
import numpy as np
import subprocess
import os

def test_export_db_erd():
    a=Terminal(debug_flag=False)
    a.load_module('plugin', 'Schema', 'reader', filename="examples/data/example_schema.json" , target_table_prefix = "student")
    a.load_module('plugin', 'YAML', 'reader', filenames=["examples/data/student_test1.yml", "examples/data/student_test2.yml"], target_table_prefix = "student")
    a.load_module('plugin', 'TOML', 'reader', filenames=["examples/data/results.toml"], target_table_prefix = "results")
    a.load_module('plugin', 'ER_Diagram', 'writer', filename = 'erd_test_output.png')
    a.transload()
    
    er_image = cv2.imread("erd_test_output.png")
    assert er_image is not None #check if image generated at all
    
    pixel_mean = np.mean(er_image)
    os.remove("erd_test_output.png")
    assert pixel_mean != 255 #check if image is all white pixels (no diagram generated)
