import os
from contextlib import redirect_stdout
from pathlib import Path
import numpy as np
from enum import Enum
from PIL import Image
from scipy.linalg import sqrtm
import matplotlib.pyplot as plt

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "2" # Ignore TensorFlow log messages
import tensorflow as tf

from dsi.dsi import DSI

class GAN:
    class ModelType(Enum):
        generator = 0
        discriminator = 1

    def __init__(self, dataset_name: str, image_col: str = None):
        '''
        GAN Constructor that loads and formats the image data from a database.
        Image data is stored in self.imageArray - a numpy array of shape (N, HEIGHT, WIDTH, CHANNEL)

        dataset_name:
            Name of the dataset and the directory where the database and models are stored.

        image_col: 
            Name of column in database that contains the image paths. 
            If None, assumes all columns except the first one are pixel values of the images.        
        '''
        self.dataset_name = dataset_name

        if not (os.path.exists(dataset_name) and os.path.isdir(dataset_name)):
            raise ValueError("Dataset directory does not exist:", dataset_name)
        
        db_path = self.check_directory(dataset_name)
        if not db_path:
            raise ValueError("Dataset directory must have a database file, untrained_models/ and pretrained_models/ directories:")

        self.generatorModel = None
        self.discriminatorModel = None
        self.epochsTrained = 0
        self.dLosses = []
        self.gLosses = []
        self.dAccuracies = []

        fnull = open(os.devnull, 'w')
        with redirect_stdout(fnull): # suppress print statements from DSI
            store = DSI(db_path, backend_name="SQLite")
            tableNames = store.list(True)
            dbName = tableNames[0]

            try:
                if image_col is not None:
                    if not isinstance(image_col, str):
                        raise ValueError("Image Column input must be a string")
                    
                    all_paths = store.query(f"SELECT {image_col} FROM {dbName}", True)

                    images = []
                    for path in all_paths[image_col]:
                        image_path = str(Path(self.dataset_name) / path)
                        if os.path.exists(image_path):
                            images.append(Image.open(image_path).convert('L'))
                        else:
                            print("Warning: Image path does not exist:", image_path)
                    
                    self.imageArray = np.array(images)
                else:
                    table_df = store.get_table(dbName, True)
                    # column 0 corresponds to the labels so drop it
                    imageDF = table_df.drop(table_df.columns[0], axis = 1)
                    self.imageArray = imageDF.values # extract array of values from df
            except Exception as e:
                raise ValueError("Error loading database:", str(e))

    def has_required_keras_files(self, folder: Path):
        has_dis = any(folder.glob("dis*.keras"))
        has_gen = any(folder.glob("gen*.keras"))
        return has_dis and has_gen

    def check_directory(self, path_str):
        path = Path(path_str)
        if not path.is_dir():
            return None

        untrained = path / "untrained_models"
        pretrained = path / "pretrained_models"

        if not untrained.is_dir() or not pretrained.is_dir():
            return None
        
        if not self.has_required_keras_files(untrained):
            return None
        if not self.has_required_keras_files(pretrained):
            return None
        
        for f in path.iterdir():
            if f.is_file() and f.suffix == ".db":
                return str(f)

        return None

    def load_model(self, model_dir, modelType):
        '''
        Load GAN model. 
        model_dir is the directory within the dataset directory where the model is stored. ex: untrained_models/gen_0.keras
        modelType is either ModelType.generator or ModelType.discriminator
        '''
        if not model_dir: #if the directory is empty
            raise ValueError(f"Directory input empty")
        if not (Path(self.dataset_name) / model_dir).is_dir():
            raise ValueError(f"Model path is not a directory within {self.dataset_name}:", model_dir)
        if modelType not in self.ModelType:
            raise ValueError(f"Invalid model type. Must be either ModelType.generator or ModelType.discriminator'")
        
        model_dir_path = Path(self.dataset_name) / model_dir
        modelPath = None
        if modelType == self.ModelType.generator:
            match = next(model_dir_path.glob("gen*.keras"), None)
            if match:
                modelPath = str(match)
        elif modelType == self.ModelType.discriminator:
            match = next(model_dir_path.glob("dis*.keras"), None)
            modelPath = None
            if match:
                modelPath = str(match)
        
        if modelPath is None:
            raise ValueError(f"No model found in '{model_dir}' for the '{modelType.name}' type.")
        
        try:
            model = tf.keras.models.load_model(modelPath)
        except:
            raise ValueError(f"Unable to open ML model: {modelPath}")

        if modelType == self.ModelType.generator:
            self.generatorModel = model
        if modelType == self.ModelType.discriminator:
            self.discriminatorModel = model
    
        
    def shape_images(self, data, width, height):
        '''
        From a numpy array, shape images from pixel columns
        columns numbered as pixel0, pixel1, pixel2....
        '''
        #data = imageDF.values
        data = data.reshape(data.shape[0], width, height, 1)
        return data      
    
    def format_images(self, images):
        '''
        Normalize the image values to [-1,1] 
        for ML processing
        '''        
        images = images.astype('float32')
        images = (images - 127.5) / 127.5
        # standardize 
        # images = images / 255
        # images = images * 2 - 1.
        return images

    def train(self, trainData: np.ndarray, epochs: int, batchSize: int, noiseDim: int):
        # format input data
        trainDataset = tf.data.Dataset.from_tensor_slices(trainData).shuffle(trainData.shape[0]).batch(batchSize)
        # define crossEntropy
        crossEntropy = tf.keras.losses.BinaryCrossentropy(from_logits=True)

        # define generator loss
        def generator_loss(fake_output):
            return crossEntropy(tf.ones_like(fake_output), fake_output)
        
        # define discriminator loss
        def discriminator_loss(real_output, fake_output):
            realLoss = crossEntropy(tf.ones_like(real_output), real_output)
            fakeLoss = crossEntropy(tf.zeros_like(fake_output), fake_output)
            totalLoss = realLoss + fakeLoss
            return totalLoss

        # define optimizers
        generatorOptimizer = tf.keras.optimizers.Adam(1e-4)
        discriminatorOptimizer = tf.keras.optimizers.Adam(1e-4)

        # Notice the use of `tf.function`
        # This annotation causes the function to be "compiled".
        @tf.function
        def train_step(images):
            noise = tf.random.normal([batchSize, noiseDim])

            with tf.GradientTape() as gen_tape, tf.GradientTape() as disc_tape:
                generatedImages = self.generatorModel(noise, training=True)

                realOutput = self.discriminatorModel(images, training=True)
                fakeOutput = self.discriminatorModel(generatedImages, training=True)

                genLoss = generator_loss(fakeOutput)
                discLoss = discriminator_loss(realOutput, fakeOutput)

            gradientsOfGenerator = gen_tape.gradient(genLoss, self.generatorModel.trainable_variables)
            gradientsOfDiscriminator = disc_tape.gradient(discLoss, self.discriminatorModel.trainable_variables)

            generatorOptimizer.apply_gradients(zip(gradientsOfGenerator, self.generatorModel.trainable_variables))
            discriminatorOptimizer.apply_gradients(zip(gradientsOfDiscriminator, self.discriminatorModel.trainable_variables))

            return discLoss, genLoss, tf.reduce_mean(realOutput), tf.reduce_mean(fakeOutput)
        
        # train the model
        for epoch in range(epochs):
            print("Epoch ", epoch)

            # iter = 0
            for batch_idx, imageBatch in enumerate(trainDataset):
                print("batch", batch_idx)
                dLoss, gLoss, realOutput, fakeOutput = train_step(imageBatch)
                self.dLosses.append(dLoss)
                self.gLosses.append(gLoss)
                dAccuracy = (realOutput + (1.0 - fakeOutput) / 2.0)
                self.dAccuracies.append(dAccuracy)
                # iter = iter + 1

            self.epochsTrained = epoch

    def save_models(self):
        '''
        Save GAN models to file.
        Models are of type '.keras' and are subscripted by the number of epochs trained
        '''
        folder = str(Path(self.dataset_name) / "trained_models")
        if not os.path.exists(folder):
            os.makedirs(folder)
        
        if self.generatorModel:
            self.generatorModel.save(folder + "/gen_"+str(self.epochsTrained)+".keras")
            print("Saved generator model to:", folder + "/gen_"+str(self.epochsTrained)+".keras")
        else:
            print("Generator model not defined")
        if self.discriminatorModel:
            self.discriminatorModel.save(folder + "/dis_"+str(self.epochsTrained)+".keras")
            print("Saved discriminator model to:", folder + "/dis_"+str(self.epochsTrained)+".keras")
        else:
            print("Discriminator model not defined")
    
    def plot_training_metrics(self, save_path):
        '''
        Plot the training metrics of the GAN model:
            - discriminator losses
            - generator losses
            - discriminator accuracies
        '''
        if any(len(metric) == 0 for metric in [self.dLosses, self.gLosses, self.dAccuracies]):
            print("Error: No training metrics to plot")
            return

        plt.plot(self.dLosses, label = "D Loss")
        plt.plot(self.gLosses, label = "G Loss")
        plt.plot(self.dAccuracies, label = "D Accuracies")

        plt.title("GAN Training Metrics")
        plt.xlabel("All batches for all epochs")
        plt.ylabel("Value")
        plt.legend()
        plt.savefig(save_path)


    def predict(self, numImages, noiseDim, prediction_path = None):
        """
        numImages: 
            number of images to generate

        noiseDim: 
            dimension of noise vector used for the generator model

        prediction_path: 
            If provided, saves generated images in a grid to the path. 
            If None, returns the generated images as a numpy array of shape (numImages, HEIGHT, WIDTH, CHANNEL)
        """
        try:
            seed = tf.random.normal([numImages, noiseDim])
            predictions = self.generatorModel(seed, training=False)
        except Exception:
            raise ValueError("Error generating images. Check if generator model is defined and trained.")
        
        if prediction_path:
            fig = plt.figure(figsize=(4, 4))
            for i in range(predictions.shape[0]):
                plt.subplot(4, 4, i+1)
                plt.imshow(predictions[i, :, :, 0] * 127.5 + 127.5, cmap='gray')
                plt.axis('off')

            plt.savefig(prediction_path)
        else:
            return predictions
    

    def calculate_mse(self, imageA, imageB):
        '''
        Mean Squared Error measure the difference between pixel intensisties of two images. 
        In this implementation MSE measure the difference between the synthetic vs 
        generated images using the GAN
        Lower the error, better the MSE score.
        '''
        if isinstance(imageA, tf.Tensor): # if images are tensor value, convert to numpy
            imageA = imageA.numpy()
        if isinstance(imageB, tf.Tensor): # if images are tensor value, convert to numpy
            imageB = imageB.numpy()
        err = np.sum((imageA.astype("float") - imageB.astype("float")) ** 2) #Images to be of same dimension
        err /= float(imageA.shape[0] * imageA.shape[1])
        return err
    
    def calculate_inception_score(self, images):
        '''
        Inception Score measures the image quality of the generated images 
        incorporating the difference of probabilities using KL Divergence
        Images must be numpy aray of shape (N, HEIGHT, WIDTH, CHANNEL) where N is number of samples
        '''
        images = images.reshape(images.shape[0], -1) # Flatten images
        images = images.astype('float32') / 255.0 # Normalize between [0,1]

        pYx = np.exp(images) / np.exp(images).sum(axis=1, keepdims=True) # Conditional porbability of class for generated_images
        pY = np.mean(pYx, axis=0, keepdims=True) # Marginal distribution

        entropyConditional = -np.sum(pYx * np.log(pYx + 1e-8), axis=1)  
        entropyMarginal = -np.sum(pY * np.log(pY + 1e-8)) 

        klDivergence = entropyConditional - entropyMarginal # KL divergence between condtional and marginal distribution
        avgKLDivergence = np.mean(klDivergence)
    
        score = np.exp(avgKLDivergence) # Inception Score is exponential calculation of mean KL divergence
        return score
    
    def calculate_fid(self, realImages, generatedImages):
        '''
        Frechet Inception Distance measures the feature distance between the real and generated images
        realImages and generatedImages must be numpy array of shape (N, HEIGHT, WIDTH, CHANNEL) where N is number of samples of images
        '''
        if isinstance(realImages, tf.Tensor): # if images are tensor value, convert to numpy
             realImages = realImages.numpy()
        if isinstance(generatedImages, tf.Tensor): # if images are tensor value, convert to numpy
             generatedImages = generatedImages.numpy()
        realImages = realImages.reshape((realImages.shape[0], -1))  # Flatten images
        generatedImages = generatedImages.reshape((generatedImages.shape[0], -1))  # Flatten images

        mu1, sigma1 = realImages.mean(axis=0),np.cov(realImages, rowvar=False) # Calculate mean and covariance statistics
        mu2, sigma2 = generatedImages.mean(axis=0), np.cov(generatedImages, rowvar=False) # Calculate mean and covariance statistics

        ssdiff = np.sum((mu1 - mu2)**2.0) # Calculate sum squared difference between means

        covmean = sqrtm(sigma1.dot(sigma2)) # Calculate sqrt of product between covariances

        if np.iscomplexobj(covmean):
           covmean = covmean.real # Check and correct imaginary numbers from sqrt

        fid = ssdiff + np.trace(sigma1 + sigma2 - 2.0 * covmean) # Calculate score
        return fid
    
    def generate_scatterplot(self, metrics, titles, num_images, save_path):
        '''
        Generate scatter plot for the given metrics
        '''
        x_labels = [f'Image {i}' for i in range(num_images)]
        x = np.arange(num_images)

        fig, axes = plt.subplots(len(metrics), 1, figsize=(12, 8), squeeze=False)
        axes = axes.flatten()
        for ax, y_values, title in zip(axes, metrics, titles):
            ax.scatter(x, y_values, marker="o")
            ax.set_title(f"{title} Scatter Plot")
            ax.set_xlabel('Image')
            ax.set_ylabel(f'{title} Value')
            ax.set_xticks(x)
            ax.set_xticklabels(x_labels, rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(save_path)
        plt.close()
    

    def generate_heatmaps(self, realImages, generatedImages, num_samples, save_path):
        '''
        Generate heatmap visualization for real vs generated images by generator model
        '''
        realImages = realImages[:num_samples]
        generatedImages = generatedImages[:num_samples]
        num_real = len(realImages)
        num_gen = len(generatedImages)

        fig, axes = plt.subplots(num_real, num_gen, figsize=(num_gen * 4, num_real * 4), squeeze=False)

        for i in range(num_real):
            for j in range(num_gen):
                    diff = np.abs(realImages[i] - generatedImages[j])
                    diff_min = np.min(diff)
                    diff_max = np.max(diff)

                    if diff_max > diff_min:
                        norm_diff = (diff - diff_min) / (diff_max - diff_min)
                    else:
                        norm_diff = np.zeros_like(diff)
                    
                    if norm_diff.ndim == 3:
                        norm_diff = np.mean(norm_diff, axis=2)

                    ax = axes[i, j]
                    im = ax.imshow(norm_diff, cmap="viridis", aspect="auto")
                    # ax.imshow(norm_diff, cmap='viridis', aspect="auto")
                    ax.set_title(f"Real {i+1} vs Generated {j+1}")
                    ax.set_xticks([])
                    ax.set_yticks([])
                    # ax.axis('off')
                    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04, label='Pixel Difference')

        plt.tight_layout()
        plt.savefig(save_path)
        plt.close()