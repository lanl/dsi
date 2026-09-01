import os
import pandas as pd
import urllib.request

def downloadImages(path_to_csv, imageFolder):
    """
    Read and download the images from the SDSC server
    """
    if not os.path.exists(imageFolder):
        os.makedirs(imageFolder)

    df = pd.read_csv(path_to_csv)
    counter = 0
    for url in df["FILE"]:
        if counter == 10:
            return
        filename = url.rsplit('/', 1)[1]
        
        dst = imageFolder + filename
        if not os.path.exists(dst):
            urllib.request.urlretrieve(url, dst)
        counter += 1
        
        
def count_burned_pixels(df):
    return df