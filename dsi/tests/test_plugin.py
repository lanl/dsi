from dsi.plugins import file_writer as fw
import cv2
import sqlite3
import numpy as np

def test_export_db_erd():

    connection = sqlite3.connect("test.db")
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS example (id INTEGER, name TEXT, age INTEGER)")
    cursor.execute("INSERT INTO example VALUES (1, 'alice', 20)")
    cursor.execute("INSERT INTO example VALUES (2, 'bob', 30)")
    cursor.execute("INSERT INTO example VALUES (3, 'eve', 40)")
    connection.commit()
    connection.close()

    erd = fw.ER_Diagram("test.db")
    erd.export_erd("test.db", "test1")
    
    er_image = cv2.imread("test1.png") 

    assert er_image is not None #check if image generated at all
    assert np.mean(er_image) != 255 #check if image is all white pixels (empty diagram)
