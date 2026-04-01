import os
from dsi.ml_workflows.gan import GAN

import urllib.request
from dsi.dsi import DSI
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

def downloadImages(path_to_csv, path_to_db, imageFolder):
    """
    Read and download the images from the SDSC server
    """
    if not os.path.exists(imageFolder):
        os.makedirs(imageFolder)

    store = DSI(path_to_db)
    store.read(path_to_csv, "CSV", table_name="wfdata")
    df = pd.read_csv(path_to_csv)
    for url in df["FILE"]:
        filename = url.rsplit('/', 1)[1]
        
        dst = imageFolder + filename
        if not os.path.exists(dst):
            urllib.request.urlretrieve(url, dst)


if __name__ == "__main__":
    db_path = 'wildfire.db'
    images_columns_of_interest = "FILE"
    path_prefix = "wildfire_64.cdb/"
    image_path = "wildfire_images/"


    input_csv = "wildfiredata.csv"
    downloadImages(input_csv, db_path, image_path)

    gWorkflow = GAN(db_path, images_columns_of_interest, path_prefix)

    # # RUN THE TRAINING PROCESS

    path_to_generator = 'wildfire_untrained_models/gen_64x64.keras'
    path_to_discriminator = 'wildfire_untrained_models/dis_64x64.keras'
    trained_models_folder = 'wildfire_trained_models/'
    epochs = 2
    batch_size = 256
    noise = 100
    size = (64, 64)

    gWorkflow.load_model(path_to_generator, gWorkflow.ModelType.generator)
    gWorkflow.load_model(path_to_discriminator, gWorkflow.ModelType.discriminator)
    width, height = size
    images = gWorkflow.shape_images(gWorkflow.imageArray, width, height)
    images = gWorkflow.format_images(images)

    if not os.path.exists(trained_models_folder):
        os.makedirs(trained_models_folder)
    gWorkflow.train(images, epochs, batch_size, noise)
    gWorkflow.save_models_to_file(trained_models_folder)


    # # # plot the losses and accuracies of the results 
    # # dLosses = gWorkflow.get_discriminator_losses()
    # # gLosses = gWorkflow.get_generator_losses()
    # # dAccuracies = gWorkflow.get_discriminator_accuracies()

    # # plt.plot(dLosses, label = "D Loss")
    # # plt.plot(gLosses, label = "G Loss")
    # # plt.plot(dAccuracies, label = "D Accuracies")

    # # plt.title ("GAN Training Metrics")
    # # plt.xlabel("All batches for all epochs")
    # # plt.ylabel("Value")
    # # plt.legend()
    # # plt.savefig("metrics.png")




    # # RUN THE PREDICTION PROCESS

    # path_to_generator = 'wildfire_pretrained_models/gen_64x64_499.keras'
    # num_images = 16
    # noise_dim = 100
    # prediction_path = 'prediction.png'

    # gWorkflow.load_model(path_to_generator, gWorkflow.ModelType.generator)
    # generatedImages = gWorkflow.predict(num_images, noise_dim)

    # fig = plt.figure(figsize=(4, 4))
    # for i in range(generatedImages.shape[0]):
    #     plt.subplot(4, 4, i+1)
    #     plt.imshow(generatedImages[i, :, :, 0] * 127.5 + 127.5, cmap='gray')
    #     plt.axis('off')

    # plt.savefig(prediction_path)




    # # RUN THE EVALUATION PROCESS

    # path_to_generator = 'wildfire_pretrained_models/gen_64x64_499.keras'
    # size = (64, 64)
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
    # x_labels = [f'Image {i}' for i in range(num_images)]
    # x = np.arange(num_images)

    # fig, axes = plt.subplots(len(metrics), 1, figsize=(12, 8), squeeze=False)
    # axes = axes.flatten()
    # for ax, y_values, title in zip(axes, metrics, titles):
    #     ax.scatter(x, y_values, marker="o")
    #     ax.set_title(f"{title} Scatter Plot")
    #     ax.set_xlabel('Image')
    #     ax.set_ylabel(f'{title} Value')
    #     ax.set_xticks(x)
    #     ax.set_xticklabels(x_labels, rotation=45, ha='right')
    # plt.tight_layout()
    # plt.savefig('metrics_scatter_plots.png')
    # plt.close()

    # '''
    # Heatmap visualization for real vs generated images by generator model
    # '''
    # images = images[:num_samples]
    # generatedImages = generatedImages[:num_samples]
    # num_real = len(images)
    # num_gen = len(generatedImages)

    # fig, axes = plt.subplots(num_real, num_gen, figsize=(num_gen * 4, num_real * 4), squeeze=False)

    # for i in range(num_real):
    #     for j in range(num_gen):
    #             diff = np.abs(images[i] - generatedImages[j])
    #             diff_min = np.min(diff)
    #             diff_max = np.max(diff)

    #             if diff_max > diff_min:
    #                 norm_diff = (diff - diff_min) / (diff_max - diff_min)
    #             else:
    #                 norm_diff = np.zeros_like(diff)
                
    #             if norm_diff.ndim == 3:
    #                 norm_diff = np.mean(norm_diff, axis=2)

    #             ax = axes[i, j]
    #             im = ax.imshow(norm_diff, cmap="viridis", aspect="auto")
    #             ax.set_title(f"Real {i+1} vs Gen {j+1}")
    #             ax.set_xticks([])
    #             ax.set_yticks([])
    #             fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04, label='Pixel Difference')
        
    # plt.tight_layout()
    # plt.savefig('generated_heatmaps.png')
    # plt.close()