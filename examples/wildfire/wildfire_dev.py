import os
import pandas as pd
import urllib.request 

from dsi.core import Terminal

def downloadImages(path_to_csv, imageFolder):
    """
    Read and download the images from the SDSC server
    """
    if not os.path.exists(imageFolder):
        os.makedirs(imageFolder)

    df = pd.read_csv(path_to_csv)
    for url in df["FILE"]:
        filename = url.rsplit('/', 1)[1]
        
        dst = imageFolder + filename
        if not os.path.exists(dst):
            urllib.request.urlretrieve(url, dst)

if __name__ == "__main__":
    input_csv = "wildfiredata.csv"
    db_name = 'wildfire.db'
    cinema_db_name = "wildfire.cdb/"
    path_to_cinema_images = cinema_db_name + "images/"
    datacard = "wildfire_oceans11.yml"
    output_csv = cinema_db_name + "wildfire_output.csv"
    table_name = "wfdata"
    columns_to_keep = ["wind_speed", "wdir", "smois", "burned_area", "LOCAL_PATH"]

    # downloads wildfire images and moves them to the Cinema Database folder - external to DSI
    if not os.path.exists(cinema_db_name):
        os.makedirs(cinema_db_name)
    downloadImages(input_csv, path_to_cinema_images)

    core = Terminal()

    #creating manual simulation table where each row of wildfire is its own simulation
    core.load_module('plugin', "Ensemble", "reader", filenames = input_csv, table_name = table_name)

    #ingest metadata information via data card
    core.load_module('plugin', "Oceans11Datacard", "reader", filenames = datacard)

    # update DSI abstraction directly
    updatedFilePaths = []
    wildfire_table = core.get_current_abstraction(table_name)
    for url_image in wildfire_table['FILE']:
        image_name = url_image.rsplit('/', 1)[1]
        filePath = path_to_cinema_images + image_name
        updatedFilePaths.append(filePath)
    wildfire_table['LOCAL_PATH'] = updatedFilePaths
    core.update_abstraction(table_name, wildfire_table)

    # export data with revised filepaths to CSV
    core.load_module('plugin', "Csv_Writer", "writer", filename = output_csv, table_name = table_name, export_cols = columns_to_keep)
    core.transload()

    if os.path.exists(db_name):
        os.remove(db_name)

    #load data to a sqlite database
    core.load_module('backend','Sqlite','back-write', filename=db_name)
    core.artifact_handler(interaction_type='ingest')


