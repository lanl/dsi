import os
from dsi.ml_workflows.gan import GAN

def FMNIST_train(pathToGenerator, pathToDiscriminator, pathToDB, columnsofInterest, epochs, batchSize, noise, trainedModelsFolder):
    gWorkflow = GAN()
    genModelLoadSuccessful = gWorkflow.load_model(pathToGenerator, gWorkflow.ModelType.generator)
    disModelLoadSuccessful = gWorkflow.load_model(pathToDiscriminator, gWorkflow.ModelType.discriminator)
    dbLoadSuccessful = gWorkflow.load_database(pathToDB)
    if genModelLoadSuccessful and disModelLoadSuccessful and dbLoadSuccessful:
        tableNames = gWorkflow.get_DB_names()
        dbName = tableNames[0]
        dataframe = gWorkflow.extract_DB_columns(columnsofInterest, dbName) 

        # column 0 corresponds to the labels
        imageDF = dataframe.drop(dataframe.columns[0], axis = 1)
        imgData = imageDF.values # extract array of values from df
        width = height = 28
        images = gWorkflow.shape_images(imgData, width, height)
        images = gWorkflow.format_images(images)

        # run the training process
        if not os.path.exists(trainedModelsFolder):
            os.makedirs(trainedModelsFolder)        
        gWorkflow.train(images, epochs, batchSize, noise)
        gWorkflow.save_models_to_file(trainedModelsFolder)
        gWorkflow.close_database()

        # # plot the losses and accuracies of the results 
        # dLosses = gWorkflow.get_discriminator_losses()
        # gLosses = gWorkflow.get_generator_losses()
        # dAccuracies = gWorkflow.get_discriminator_accuracies()

        # plt.plot(dLosses, label = "D Loss")
        # plt.plot(gLosses, label = "G Loss")
        # plt.plot(dAccuracies, label = "D Accuracies")

        # plt.title ("GAN Training Metrics")
        # plt.xlabel("All batches for all epochs")
        # plt.ylabel("Value")
        # plt.legend()
        # plt.savefig("metrics.png")
                
if __name__ == "__main__":
    path_to_generator = 'fmnist_untrained_models/gen.keras'
    path_to_discriminator = 'fmnist_untrained_models/dis.keras'
    path_to_db = 'fmnist.db'
    trained_models_folder = 'fmnist_trained_models/'
    columns_of_interest = range(785)
    epochs = 2
    batch_size = 256
    noise = 100
    FMNIST_train(path_to_generator, path_to_discriminator, path_to_db, columns_of_interest, epochs, batch_size, noise, trained_models_folder)
        