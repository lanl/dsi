import os
from dsi.ml_workflows.gan import GAN

if __name__ == "__main__":
    db_path = 'wildfire.db'
    image_column = "FILE"

    gWorkflow = GAN(db_path, image_column)

    # # RUN THE TRAINING PROCESS

    # path_to_generator = 'wildfire_untrained_models/gen_64x64.keras'
    # path_to_discriminator = 'wildfire_untrained_models/dis_64x64.keras'
    # trained_models_folder = 'wildfire_trained_models/'
    # epochs = 2
    # batch_size = 256
    # noise = 100
    # size = (64, 64) # width, height

    # gWorkflow.load_model(path_to_generator, gWorkflow.ModelType.generator)
    # gWorkflow.load_model(path_to_discriminator, gWorkflow.ModelType.discriminator)

    # width, height = size
    # images = gWorkflow.shape_images(gWorkflow.imageArray, width, height)
    # images = gWorkflow.format_images(images)
    
    # gWorkflow.train(images, epochs, batch_size, noise)

    # if not os.path.exists(trained_models_folder):
    #     os.makedirs(trained_models_folder)
    # gWorkflow.save_models_to_file(trained_models_folder)

    # gWorkflow.plot_training_metrics("metrics.png")




    # # RUN THE PREDICTION PROCESS

    # path_to_generator = 'wildfire_pretrained_models/gen_64x64_499.keras'
    # num_images = 16
    # noise_dim = 100
    # prediction_path = 'prediction.png'

    # gWorkflow.load_model(path_to_generator, gWorkflow.ModelType.generator)
    # generatedImages = gWorkflow.predict(num_images, noise_dim, prediction_path)




    # # RUN THE EVALUATION PROCESS

    # path_to_generator = 'wildfire_pretrained_models/gen_64x64_499.keras'
    # size = (64, 64) # width, height
    # num_images = 25
    # noise_dim = 100
    # num_samples = 4

    # gWorkflow.load_model(path_to_generator, gWorkflow.ModelType.generator)
    # width, height = size
    # images = gWorkflow.shape_images(gWorkflow.imageArray, width, height)
    # images = gWorkflow.format_images(images)

    # generatedImages = gWorkflow.predict(num_images, noise_dim)
    # generatedImages = generatedImages.numpy()
    # IS = gWorkflow.calculate_inception_score(generatedImages)
    # print("Inception Score: ", IS)

    # FID, MSE = [], []
    # for i in range(num_images):
    #     MSE.append(gWorkflow.calculate_mse(images[i], generatedImages[i]))
    #     print(f"Mean Squared Error (MSE): {MSE[-1]}")
    #     FID.append(gWorkflow.calculate_fid(images[i], generatedImages[i]))
    #     print('Frechet Inception Distance: %.3f' % FID[-1])

    # metrics = [FID, MSE]
    # titles = ['FID', 'MSE']

    # gWorkflow.generate_scatterplot(metrics, titles, num_images, 'metrics_scatter_plots.png')

    # gWorkflow.generate_heatmaps(images, generatedImages, num_samples, 'generated_heatmaps.png')