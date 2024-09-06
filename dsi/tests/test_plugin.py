from dsi.plugins import file_writer as fw
import cv2
import subprocess
import os

def test_export_db_erd():
    subprocess.run(["sqlite3", "erd_test.db"], stdin= open("examples/data/erd_test.sql", "r"))
    fw.ER_Diagram("erd_test.db").export_erd("erd_test.db", "erd_test_output")
    os.remove("erd_test.db")
    
    er_image = cv2.imread("erd_test_output.png")
    assert er_image is not None #check if image generated at all
    
    pixel_mean = np.mean(er_image)
    os.remove("erd_test_output.png")
    assert pixel_mean != 255 #check if image is all white pixels (no diagram generated)
