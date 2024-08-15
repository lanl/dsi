# import numpy as np
# import seaborn as sns
# import matplotlib.pyplot as plt
# import pyqtgraph as pg

from dsi.ml_workflows.gan import GAN

def MNIST_evaluate(pathToGenerator, pathToDB, columnsofInterest, numImages, noiseDim, numberofSamples):
    gWorkflow = GAN()
    genModelLoadSuccessful = gWorkflow.load_model(pathToGenerator, gWorkflow.ModelType.generator)
    dbLoadSuccessful = gWorkflow.load_database(pathToDB)
    if genModelLoadSuccessful and dbLoadSuccessful:
        tableNames = gWorkflow.get_DB_names()
        dbName = tableNames[0]
        dataframe = gWorkflow.extract_DB_columns(columnsofInterest, dbName) 

        # column 0 corresponds to the labels
        imageDF = dataframe.drop(dataframe.columns[0], axis = 1)
        imgData = imageDF.values # extract array of values from df
        width = height = 28
        images = gWorkflow.shape_images(imgData, width, height)
        images = gWorkflow.format_images(images)

        # predicted images
        generatedImages = gWorkflow.predict(numImages, noiseDim)
        generatedImages = generatedImages.numpy()
        IS = gWorkflow.calculate_inception_score(generatedImages)
        print("Inception Score: ", IS)       

        for i in range(num_of_images):
          FID = gWorkflow.calculate_fid(images[i], generatedImages[i])
          print('Frechet Inception Distance: %.3f' % FID)
    #     '''
    #     PyQtGraph Visualization for all three metrics namely Mean Squared Error, Frechet Inception Distance, Inception Score
    #     '''
    #     metrics = [FID, IS] #MSE
    #     titles = ['FID', 'IS'] #'MSE'
    #     xLabels = ['Image {}'.format(i) for i in range(numImages)]
    #     for window in range(2): #3 
    #             window = pg.GraphicsLayoutWidget()
    #             window.setWindowTitle('Metrics for Generated Images')
    #             for metric_index, (y_values, title) in enumerate(zip(metrics, titles)):
    #                     plot = window.addPlot(title=f"{title} Scatter Plot")
    #                     plot.setLabel('left', f'{title} Value')
    #                     plot.setLabel('bottom', 'Image')
    #                     plot.getAxis('bottom').setTicks([[(i, label) for i, label in enumerate(xLabels)]])

    #             # Create ScatterPlotItem for the metric
    #                     x = np.arange(numImages)  # x positions for each image
    #                     scatter = pg.ScatterPlotItem()
    #                     scatter.setData(x=x, y=y_values, pen=None, symbol='o', brush='r')

    #                     # Add ScatterPlotItem to the plot
    #                     plot.addItem(scatter)

    #                     # Show the window
    #                     window.show()

    #     '''
    #     Heatmap visualization for real vs generated images by generator model
    #     '''
    #     images = images[:numberofSamples]
    #     generatedImages = images[:numberofSamples]
    #     num_real = len(images)
    #     num_gen = len(generatedImages)

    #     fig, axes = plt.subplots(num_real, num_gen, figsize=(num_gen * 5, num_real * 5), squeeze=False)

    #     for i in range(num_real):
    #         for j in range(num_gen):
    #                 diff = np.abs(images[i] - generatedImages[j])
    #                 norm_diff = (diff - np.min(diff)) / (np.max(diff) - np.min(diff))
    
    #                 # Convert from [-1, 1] to [0, 1] if needed
    #                 if norm_diff.min() < 0 or norm_diff.max() > 1:
    #                     norm_diff = (norm_diff + 1) / 2  # Adjust if your images are between [-1, 1]
    
    # # If images have multiple channels, convert to grayscale
    #                 if norm_diff.ndim == 3:
    #                     norm_diff = np.mean(norm_diff, axis=2)  # Convert to grayscale
    
    # # Plot heatmap
    #                 sns.heatmap(norm_diff, cmap='viridis', cbar_kws={'label': 'Differences in Pixels'}, ax=axes[i, j])
    #                 axes[i, j].set_title(f'Real {i+1} vs Gen {j+1}')
    

    #     plt.tight_layout()
    #     plt.savefig('generated_heatmaps/')
    #     plt.show()
        
                
if __name__ == '__main__':
      path_to_generator = 'mnist_pretrained_models/gen_49.keras'
      path_to_db = 'mnist.db'
      columns_of_interest = range(785)
      num_of_images = 25
      noise_dim = 100
      number_of_samples = 4
      MNIST_evaluate(path_to_generator, path_to_db, columns_of_interest, num_of_images, noise_dim, number_of_samples)

    

         