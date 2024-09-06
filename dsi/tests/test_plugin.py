from dsi.plugins import file_writer as fw
import cv2
import subprocess
import os

def test_export_db_erd():
    subprocess.run(["sqlite3", "erd_test.db"], stdin= open("examples/data/erd_test.sql", "r"))
    fw.ER_Diagram("erd_test.db").export_erd("erd_test.db", "erd_test_output")
    os.remove("erd_test.db")

    assert cv2.imread("erd_test_output.png") is not None #check if image generated at all
    
    assert open("examples/data/er-diagram.png","rb").read() == open("erd_test_output.png","rb").read() #check if er diagram matches reference image
    os.remove("erd_test_output.png")

test_export_db_erd()