import os
import pandas as pd
import urllib.request 

from dsi.dsi import DSI

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
    image_path = "images/"
    input_csv = "wildfiredataSmall.csv"
    db_name = 'wildfire.db'
    output_csv = "wildfire_output.csv"
    datacard = "wildfire_oceans11.yml"
    table_name = "wfdata"
    columns_to_keep = ["wind_speed", "wdir", "smois", "burned_area", "LOCAL_PATH"]

    #downloads wildfire images to a local path -- external to DSI
    if not os.path.exists(image_path):
        os.makedirs(image_path)
    downloadImages(input_csv, image_path)

    if os.path.exists(db_name):
        os.remove(db_name)

    dsi = DSI(db_name)

    dsi.read(input_csv, "Ensemble", table_name=table_name)
    dsi.read(datacard, "Oceans11Datacard")

    #get wildfire table's data
    wildfire_data = dsi.get_table(table_name, collection=True)
    
    updatedFilePaths = []
    for url_image in wildfire_data['FILE']:
        image_name = url_image.rsplit('/', 1)[1]
        filePath = image_path + image_name
        updatedFilePaths.append(filePath)
    wildfire_data['LOCAL_PATH'] = updatedFilePaths

    #update wildfire table with new column of local paths to the downloaded wildfire images
    dsi.update(wildfire_data)
    
    dsi.write(output_csv, "Csv_Writer", table_name=table_name)

    dsi.display(table_name, display_cols=columns_to_keep)