# DSI capabilities for Generative Adversarial Networks

The examples in this folder highlight the use of the DSI framework for generative adversarial networks (GANs). We present DSI capability for GANs with the QUIC-Fire wildfire dataset. The labels and paths for the wildfire data are stored in wildfire.db, a Cinema database. The image information is stored in wildfire_64.cdb/images/ and pulled when necessary. This *cdb* folder is called a Cinema Database (https://github.com/cinemascience). A cinema database is comprised of a *csv* file where each row of the table is a data element (a run or ensemble member of a simulation or experiment, for example) and each column is a property of a data element. Any column name that starts with 'FILE' is a path to a file corresponding to the data element.

## Downloading wildfire data
Due to size limits on GitHub, the wildfire data is stored in a remote location. 
Before downloading data, ensure a SSH key has been set up on LANL Gitlab.

To download the wildfire images and model files, run `python download_wildfire.py`. This will require user input in the terminal.

## Installing dependencies
Run `pip install -r requirements.txt`

## How to run
Open `gan_example.ipynb` and execute the cells to train, predict and evaluate the data.

### Train
Currently, the number of epochs is set to 2 for easier testing, but 400-500 is better for more accurate predictions. The trained models are saved in `wildfire/trained_models/`

### Predict
Currently, new images are predicted based on the `pretrained_models/`. Change path to `trained_models/` to make predictions using the trained models from above.

## Evaluate
There are currently three evaluation metrics explored in this GAN workflow: mean squared error, inception score and frechet inception distance. Results are printed to the console, and plots are generated to visualize the results.