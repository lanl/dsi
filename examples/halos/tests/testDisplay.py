import hiplot as hip
import sqlite3
import pandas as pd

conn = sqlite3.connect('/home/pascalgrosset/projects/dsi/hacc_halo_test.db')
cursor = conn.cursor()
cursor.execute('SELECT * FROM hacc_halo_test')
results = cursor.fetchall()
df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])

experiment = hip.Experiment.from_dataframe(df)
experiment.display()

