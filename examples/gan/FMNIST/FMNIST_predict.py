from dsi.ml_workflows.gan import GAN
import matplotlib.pyplot as plt

def FMNIST_predict(pathToGenerator, numImages, noiseDim, prediction_path):
    gWorkflow = GAN()
    genModelLoadSuccessful = gWorkflow.load_model(pathToGenerator, gWorkflow.ModelType.generator)
    if genModelLoadSuccessful:

        generatedImages = gWorkflow.predict(numImages, noiseDim)
        if generatedImages is not None: #images generated correctly

            fig = plt.figure(figsize=(4, 4))
            for i in range(generatedImages.shape[0]):
                plt.subplot(4, 4, i+1)
                plt.imshow(generatedImages[i, :, :, 0] * 127.5 + 127.5, cmap='gray')
                plt.axis('off')

            plt.savefig(prediction_path)
        


if __name__ == '__main__':
    path_to_generator = 'fmnist_pretrained_models/gen_42.keras'
    num_of_images = 16
    noise_dim = 100
    prediction_path = 'prediction.png'
    FMNIST_predict(path_to_generator, num_of_images, noise_dim, prediction_path)
   
    