# DSI capabilities for Generative Adversarial Networks

The examples in this folder highlight the use of the DSI Framework for generative adversarial networks (GANs). We present DSI capability for GANs for 3 datasets: MNIST handwritten digits, FMNIST clothing pictures and QUIC-Fire wildfire images. 60,000 images each for the MNIST and FMNIST examples are stored (with associated labels) in the mnist.db and fmnist.db files, respectively. The labels and paths for the wildfire data are stored in wildfire.db, a Cinema database. The image information is stored in wildfire_64.cdb/images/ and pulled when necessary. This *cdb* folder is called a Cinema Database (https://github.com/cinemascience). A cinema database is comprised of a *csv* file where each row of the table is a data element (a run or ensemble member of a simulation or experiment, for example) and each column is a property of a data element. Any column name that starts with 'FILE' is a path to a file corresponding to the data element.

Each subfolder, FMNIST/, MNIST/ and wildfire/ contain three scripts for training, prediction and evaluation (*_train.py, *_predict.py, *_evaluate.py). Each subfolder also contain untrained models in the format compatible for training (in *_untrained_modles/), and pretrained models (in *_pretrained_models/) to use for prediction and evaluation scripts. 

The process to run the scripts in each subfolder is the same. Here we give examples using the wildfire data:

First load dsi. 

## Train 
To run the wildfire_train.py script, from the wildfire/ folder, run

    python3 wildfire_train.py

Currently, the number of epochs is set to 2 for easier testing. In our experience, a reasonable number of epochs for more accurate predictions is about 400-500. The trained models after training is completed are saved in wildfire_trained_models/.

## Predict
To save the results of GAN predictions, we use matplotlib. Install matplotlib:

    python3 -m pip install matplotlib

Currently, wildfire_predict.py will predict new images based on the model 'wildfire_pretrained_models/gen_64x64_499.keras'. This is to ensure users can make predictions even if they are unable to train a new model. If you want to make predictions based on a trained model, update the path defined in "path_to_generator".

To run wildfire_predict.py, from the wildfire/ folder, run 
    
    python3 wildfire_predict.py

## Evaluate
There are currently three evaluation metrics defined in the GAN workflow functionality: mean squared error, inception score and frechet inception distance. In this example, we explore the inception score and frechet inception distance. 

To run wildfire_evaluate.py, from the wildfire/ folder, run 
    
    python3 wildfire_evaluate.py

Results are printed to the console. Additional tools like matplotlib can be used to visualize the results through plots. 