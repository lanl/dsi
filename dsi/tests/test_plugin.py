from dsi.plugins import file_writer as fw
import cv2
import subprocess
import os

def test_export_db_erd():
    subprocess.run(["sqlite3", "erd_test.db"], stdin= open("examples/data/erd_test.sql", "r"))
    fw.ER_Diagram("erd_test.db").export_erd("erd_test.db", "erd_test_output")
    os.remove("erd_test.db")
    output = cv2.imread("erd_test_output.png")
    assert output is not None #check if image generated at all

    reference = cv2.imread("examples/data/er-diagram.png")

    assert output.shape == reference.shape
    assert cv2.countNonZero(cv2.cvtColor(cv2.absdiff(output, reference), cv2.COLOR_BGR2GRAY)) == 0
    os.remove("erd_test_output.png")
