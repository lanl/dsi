import os
import pandas as pd 
import numpy as np

from enum import Enum
from PIL import Image
from scipy.linalg import sqrtm

from tensorflow.keras.optimizers import *
from tensorflow.keras.losses import *

from dsi.backends.sqlite import Sqlite, DataType

class GAN:

    generatorModel = None
    discriminatorModel = None
    database = None
    epochsTrained = 0
    dLosses = []
    gLosses = []
    dAccuracies = []

    class ModelType(Enum):
        generator = 0
        discriminator = 1

    def __init__(self):
        '''
        Constructor to create object for GAN functinality
        '''
        pass

    def load_model(self, modelPath, modelType):
        '''
        Load GAN model
        Model must be of type '.keras'
        modelType must be ModelType
        '''
        if not modelPath: #if the path is empty
            print("Error: Path to model empty")
            return 0
        if not os.path.exists(modelPath): # if the path is incorrect
            print("Error: Path does not exist:", modelPath)
            return 0
        if not modelPath.endswith('.keras'): # if not a keras file
            print("Error: not of type Keras")
            return 0
        try:
            model = tf.keras.models.load_model(modelPath)
        except:
            print("Unable to open ML model")
            return 0

        if modelType == self.ModelType.generator:
            self.generatorModel = model
        if modelType == self.ModelType.discriminator:
            self.discriminatorModel = model
            
        return 1
        
    # # check if there's a need for this    
    # def set_model(self, model):
    #     check if model type if correct
    #     then self.model = model

    def get_models(self):
        '''
        Return GAN models as 
        a list: [generator, discriminator]
        Models are of type '.keras' or None
        '''        
        return [self.generatorModel, self.discriminatorModel]
        
    def save_models_to_file(self, folder):
        '''
        Save GAN models to file
        Models are of type '.keras'
        Model names are subscripted by 
        the number of epochs trained
        '''
        # check folder path
        if not folder: #if the path is empty
            print("Error: Path to folder empty/not defined")
            return 0
        if not os.path.exists(folder): # if the path is incorrect
            print("Folder does not exist, generated.", folder)
            os.makedirs(folder)

        # save models to file if they are defined
        if self.generatorModel:
            self.generatorModel.save(folder + "/gen_"+str(self.epochsTrained)+".keras")
        else:
            print("Generator model not defined")
        if self.discriminatorModel:
            self.discriminatorModel.save(folder + "/dis_"+str(self.epochsTrained)+".keras")
        else:
            print("Discriminator model not defined")            

    def load_database(self, dbPath):
        '''
        Load DSI Sqlite database
        Must be of type DSI Sqlite (for now)
        '''
        if not dbPath: #if the path is empty
            print("Error: Path to database empty")
            return 0
        if not os.path.exists(dbPath): # if the path is incorrect
            print("Error: Path does not exist:", dbPath)
            return 0
        if not dbPath.endswith('.db'): # if not a db file
            print("Error: not of type DSI Sqlite DB")
            return 0
        try:
            database = Sqlite(dbPath)
        except:
            print("Unable to open DSI Sqlite Database")
            return 0
        
        self.database = database
        return 1
        
    def get_database(self):
        '''
        Return DSI Sqlite database
        Must be of type DSI Sqlite (for now)
        '''        
        if self.database:
            return self.database

    def close_database(self):
        self.database.close()

    def get_DB_names(self):
        '''
        Return DSI Sqlite database table names
        Must be of type DSI Sqlite (for now)
        '''
        result = self.database.get_artifact_list()
        dbNames =[name[0] for name in result]
        # dbName = dbName[0]
        return dbNames
    
    def extract_DB_columns(self, columnIndices, dbName):
        '''
        Return columns of interest (by column index)
        as a Pandas dataframe
        '''        
        dataType = DataType()
        dataType.name = dbName

        # get all headers and then select those needed by index
        query = "SELECT name FROM PRAGMA_TABLE_INFO('" + str(dataType.name) + "')"
        headerResult = self.database.sqlquery(query)
        columnNames = list(map(headerResult.__getitem__, columnIndices))

        names = ""
        for name in columnNames:
            name = name[0]
            names += name + ","
        names = names[:-1]
        # get the data from the needed columns and combine into a pandas dataframe
        query = "SELECT " + names  + " FROM " + str(dataType.name) + ";"
        result = self.database.sqlquery(query)
        columnHeaders = [column[0] for column in columnNames]
        dataframe = pd.DataFrame(result, columns=columnHeaders)
       
        return dataframe

    def extract_images_from_path(self, imgColumnId, dbName, pathPrefix='./'):
        '''
        Given an integer that corresponds to the column with image path
        Return np array of images following that path to image location
        Optional path prefix allows to add relative path to those in path
        '''
        if not isinstance(imgColumnId, int):
            print("Error: Column Id is not of type integer")
            return 0
        if not os.path.exists(pathPrefix):
            print("Error: Path Prefix does not exist:", pathPrefix)
            return 0
        dataType = DataType()
        dataType.name = dbName

        # get all headers and then select those needed by index
        query = "SELECT name FROM PRAGMA_TABLE_INFO('" + str(dataType.name) + "')"
        headerResult = self.database.sqlquery(query)
        columnNames = list(map(headerResult.__getitem__, [imgColumnId]))

        name = columnNames[0][0]
        # names = ""
        # for name in columnNames:
        #     name = name[0]
        #     names += name + ","
        # names = names[:-1]

        # get the path data from the needed column
        query = "SELECT " + name  + " FROM " + str(dataType.name) + ";"
        result = self.database.sqlquery(query)

        # extract the image data from the path
        images = []
        for pth in result:
            path = pathPrefix + pth[0]
            img = Image.open(path).convert('L') #future work: color img
            images.append(img)

        return np.array(images)


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
    
    def get_epochs_trained(self):
        return self.epochsTrained

    def get_discriminator_losses(self):
        return self.dLosses
    
    def get_generator_losses(self):
        return self.gLosses
    
    def get_discriminator_accuracies(self):
        return self.dAccuracies

    def train(self, trainData, epochs, batchSize, noiseDim):
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

            iter = 0
            for imageBatch in trainDataset:
                print("batch", iter)
                dLoss, gLoss, realOutput, fakeOutput = train_step(imageBatch)
                self.dLosses.append(dLoss)
                self.gLosses.append(gLoss)
                dAccuracy = (realOutput + (1.0 - fakeOutput) / 2.0)
                self.dAccuracies.append(dAccuracy)
                iter = iter + 1

            self.epochsTrained = epoch

    def predict(self, numImages, noiseDim):
        if self.generatorModel:
            seed = tf.random.normal([numImages, noiseDim])
            predictions = self.generatorModel(seed, training=False)
            return predictions
        else:
            print("Generator model is not set")
            return None
    
    def calculate_mse(self, imageA, imageB):
         '''
    #     Mean Squared Error measure the difference between pixel intensisties of two images. 
    #     In this implementation MSE measure the difference between the synthetic vs 
    #     generated images using the GAN
    #     Lower the error, better the MSE score.
    #     '''
         if isinstance(imageA, tf.Tensor): # if images are tensor value, convert to numpy
             imageA = imageA.numpy()
         if isinstance(imageB, tf.Tensor): # if images are tensor value, convert to numpy
             imageB = imageB.numpy()
         err = np.sum((imageA.astype("float") - imageB.astype("float")) ** 2) #Images to be of same dimension
         err /= float(imageA.shape[0] * imageA.shape[1])
         return err
    
    def calculate_inception_score(self, images):
         '''
    #     Inception Score measures the image quality of the generated images 
    #     incorporating the difference of probabilities using KL Divergence
    #     Images must be numpy aray of shape (N, HEIGHT, WIDTH, CHANNEL) where N is number of samples
    #     '''
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
    #    Frechet Inception Distance measures the feature distance between the real and generated images
    #    realImages and generatedImages must be numpy array of shape (N, HEIGHT, WIDTH, CHANNEL) where N is number of samples of images
    #    '''
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