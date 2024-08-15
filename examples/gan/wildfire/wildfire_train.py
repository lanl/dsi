from dsi.ml_workflows.gan import GAN

def wildfire_train(pathToGenerator, pathToDiscriminator, pathToDB, dataColumnsofInterest, imagesColumnOfInterest, epochs, batchSize, noise, trainedModelsFolder, pathPrefix):
        gWorkflow = GAN()
        genModelLoadSuccessful = gWorkflow.load_model(pathToGenerator, gWorkflow.ModelType.generator)
        disModelLoadSuccessful = gWorkflow.load_model(pathToDiscriminator, gWorkflow.ModelType.discriminator)
        dbLoadSuccessful = gWorkflow.load_database(pathToDB)
        if genModelLoadSuccessful and disModelLoadSuccessful and dbLoadSuccessful:

                tableNames = gWorkflow.get_DB_names()
                dbName = tableNames[0]

                # # extract label columns. not used here, can be used with conditional GANS
                # label_dataframe = gWorkflow.extract_DB_columns(dataColumnsofInterest, dbName) 
                
                # extract images from column of urls
                imageArray = gWorkflow.extract_images_from_url(imagesColumnOfInterest, dbName, pathPrefix)
                width = height = 64
                images = gWorkflow.shape_images(imageArray, width, height)
                images = gWorkflow.format_images(images)

                # run the training process
                gWorkflow.train(images, epochs, batchSize, noise)
                gWorkflow.save_models_to_file(trainedModelsFolder)
                gWorkflow.close_database()

        #         # plot the losses and accuracies of the results 
        #         dLosses = gWorkflow.get_discriminator_losses()
        #         gLosses = gWorkflow.get_generator_losses()
        #         dAccuracies = gWorkflow.get_discriminator_accuracies()

        #         plt.plot(dLosses, label = "D Loss")
        #         plt.plot(gLosses, label = "G Loss")
        #         plt.plot(dAccuracies, label = "D Accuracies")

        #         plt.title ("GAN Training Metrics")
        #         plt.xlabel("All batches for all epochs")
        #         plt.ylabel("Value")
        #         plt.legend()
        #         plt.savefig("metrics.png")
                
if __name__ == "__main__":
        path_to_generator = 'wildfire_untrained_models/gen_64x64.keras'
        path_to_discriminator = 'wildfire_untrained_models/dis_64x64.keras'
        path_to_db = 'wildfire.db'
        path_prefix = "wildfire_64.cdb/"
        trained_models_folder = 'wildfire_trained_models/'
        data_columns_of_interest = [4]
        images_columns_of_interest = 8
        epochs = 2
        batch_size = 256
        noise = 100
        wildfire_train(path_to_generator, path_to_discriminator, path_to_db, data_columns_of_interest, images_columns_of_interest, epochs, batch_size, noise, trained_models_folder, path_prefix)
        