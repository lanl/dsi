---
language:
- en # ISO language tag
tags:
- project:genesis # include on all GENESIS project models
- project:NSEC # include your _short_ model team name e.g. MOAT
- type:model # use other types include {agent, eval, framework, model, etc...}
- science:wildfire # what kind of science is this for (e.g., materials, biology, lightsource, fusion, climate, etc.)
- risk:general # indicates level of risk review {general, reviewed, restricted}
license: BSD-3 # use an SPDX license identifier https://spdx.org/licenses/
license_name: BSD-3  # If license = other (license not in https://hf.co/docs/hub/repositories-licenses), specify an id for it here, like `my-license-1.0`. if not delete this line
license_link: https://opensource.org/license/bsd-3-clause  # If license = other, specify "LICENSE" or "LICENSE.md" to link to a file of that name inside the repo, or a URL to a remote file. if not delete this line
base_model: random forest # if fine tuning, include the basemodel url here
new_version: # if this model has been superseeded by a new version, omit for now
datasets:
    - https://github.com/lanl/dsi/blob/main/examples/genesis_demo/wildfiredata.csv
metrics:
    - MSE
---

# ${MODEL_NAME}
Wildfire 
The model description provides basic details about the model. This includes the architecture, version, if it was introduced in a paper, if an original implementation is available, the author, and general information about the model. Any copyright should be attributed here. General information about training procedures, parameters, and important disclaimers can also be mentioned in this section. 

*Last Updated*: **2024-11-01**

## Developed by
Megan

## Contributed by  
Vedent Iyer 

## Model Changelog 
 
+ **2024-10-01** initial public version

## Model short description

Examples:  
+ Random Forest

## Model description

Examples: 

1. This model is trained on RoBERTa large with the binary classification setting of the Stanford Sentiment Treebank. It achieves 95.11% accuracy on the test set. 
2. BLIP-2 consists of 3 models: a CLIP-like image encoder, a Querying Transformer (Q-Former) and a large language model. 
3. The authors initialize the weights of the image encoder and large language model from pre-trained checkpoints and keep them frozen while training the Querying Transformer, which is a BERT-like Transformer encoder that maps a set of "query tokens" to query embeddings, which bridge the gap between the embedding space of the image encoder and the large language model. 
4. The goal for the model is to predict the next text token, given the query embeddings and the previous text. 
5. This allows the model to be used for tasks like image captioning, visual question answering (VQA), or scientific text summarization.

## Finetuned from model (optional)

If your model is a fine-tune, an adapter, or a quantized version of a base (parent) model, you can specify the base model here. This information can also be used to indicate if your model is a merge of multiple existing models.   

List of related/parent models (optional) 

## Model Type

Examples:  

RoBERTA Large, VIT-huge-patch14-224-in21k, CLIP ViT-g/14, RESNET-50, GPT-OSS, etc. 
 
## Inputs and outputs

(text, images, time series, etc.) 

Examples of Inputs and Outputs:  

1. Input: The model was trained with 224 x 224 input images and 512 token input/output text sequences.  
2. Input: The input images are expected to have color values in the range [0,1]. The model was trained with the input images of height x width = 224 x 224 pixels. 
3. Output: The output is a batch of feature vectors. For each input image, the feature vector has size num_features = 1280. The feature vectors can then be used further, e.g., for classification.  
4. Output: The output is the prediction score denoting.  ... 

## Compute Infrastructure

Example: This model was trained on a slurm cluster. 

### Hardware

Please include a link to the DOE resource(s) used for training

1. Example: This model was trained on 4 NVIDIA’s A100 GPUs on the [ALCF Polaris machine](https://www.alcf.anl.gov/polaris).
2. Example: The model was evaluated on 4 AMD MI250X GPUs  on [OLCF Frontier](https://www.olcf.ornl.gov/frontier/).


### Software

Example: This model was trained with FlashAttention and PyTorch. Please attach packages via conda list or pip list or container initialization script.  

Code snippets for getting configurations: 

if Python: pip freeze > requirements.txt 
if Spack: provide a spack lock file
if Conda: conda list --explicit > spec-file.txt 
if docker: Include docker file and docker compose script if needed.  
else, another software package is used, please include reproducibility steps

```txt
put output or link to output here of the above commands
```

## Papers and Scientific Outputs 

Citations in [bibtex format](https://www.bibtex.com/g/bibtex-format/). Please include either a `doi` or `url` field in the citation.

## Model License

If using a non-standard license (e.g. BSD, Apache2, MIT, etc...), please include it or a link to it here. If the model is not open-sourced, also mention that here.

## Contact Info and Model Card Authors

Provide one or more corresponding authors with emails.


# Intended Uses 

This section describes the use cases the model is intended for, including the languages and domains where it can be applied. Document areas that are out of scope or where performance may be limited.

## Intended Use

Cases/examples/tasks for which the model was intended to be used.

### Primary Intended Users 

Example: The model will be used by researchers to understand robustness, generalization, and performance of domain-specific AI systems.

### Mission Relevance 

This could include tasks linked to DOE projects or internal/external funded work.

## Out-of-Scope Use Cases

Describe cases not recommended for this model’s use.


# How to use 

This section is the most important for reusability of the model.  This section should include: 

## Install Instructions 

## Training configuration 

## Inference configuration 
   

# Code snippets of how to use the model  

Include code for training and inference and running the model on CPU and GPU. This can showcase usage of the model and tokenizer classes, and any other code that is needed to use the model and any other code you think might be helpful. 


# Limitations

## Risks

> The most powerful AI systems may pose novel national security risks in the near future in areas
> such as cyberattacks and the development of chemical, biological, radiological, nuclear, or
> explosives (CBRNE) weapons, as well as novel security vulnerabilities. Because America
> currently leads on AI capabilities, the risks present in American frontier models are likely to be
> a preview for what foreign adversaries will possess in the near future. Understanding the
> nature of these risks as they emerge is vital for national defense and homeland security.

From the AI Action Plan, please document risks associated with your model consistent with this definition, if they exist

## Limitations

Any additional concerns, or tests/data needed. Please include discussion of potential biases and systematic errors.

Other relevant cases not covered by the testing data data 

Examples: 

Like other large language models for which the homogeneity (or lack thereof) of training data induces downstream impact on the quality of our model, OPT-175B has limitations. OPT-175B can also have quality issues in terms of generation homogeneity and hallucination. In general, OPT-175B is not immune from the plethora of issues that plague modern large language models. 


# Training details 

In this section you should describe all the relevant aspects of training that are useful from a reproducibility perspective. This includes any preprocessing and postprocessing that were done on the data, as well as details such as the number of epochs the model was trained for, the batch size, the learning rate, and so on. 

## Training data 

Description, includes where the data came from, why it was chosen, size(s) of datasets, URL(s), date (when the dataset was downloaded) & version of the dataset, preprocessing methods, data augmentation methods 

URL(s) to dataset card(s) with relevant information, if available  

## Training Procedure 

Describe all the relevant aspects of training that are useful from a reproducibility perspective. This includes any preprocessing and postprocessing that were done on the data, as well as details such as the number of epochs the model was trained for, the batch size, the learning rate, and so on. 

### Reproducibility Information (optional)

- Random seed used: 42  
- Machine/environment info: GPU type, software versions  
- Link to evaluation or training pipeline: [URL or repository path]  

## Pre-training information 

+ Hyperparameter values, and whether hyperparameter tuning was performed  
+ Model initialization  
+ Information about any fine-tuning, chain of thought, and n-shot learning performed 
+ Optimizer information 
+ Loss function 
+ Stopping criterion 
+ Number of training epochs 
+ Number of training steps 
+ Batch size 
+ Training speed: number of steps per second and number of samples per second 
+ Training Loss: the average training loss and a specific training loss 
+ What prompting templates/functions were used? Were prompts optimized? Prompt examples 
+ Model optimization techniques -- Distillation/Quantization/Pruning/Sparsity (learned during training or posthoc) 

Examples:  

This model was trained for 100k gradient steps with a batch size of 512k tokens and a linearly decaying learning rate from 6e-4 to zero, with a linear warmup of 5k steps. ... 

The model uses NormalFloat4 datatype and LoRA adapters on all linear layers with BFloat16 as computation datatype. We set LoRA r=64, alpha=16. We also use Adam beta2 of 0.999, max grad norm of 0.3 and LoRA dropout of 0.1 for models up to 13B and 0.05 for 33B and 65B models. For the finetuning process, we use constant learning rate schedule and paged AdamW optimizer. ... 


# Evaluation details 

Provide an indication of how well the model performs on the evaluation dataset. If the model uses a decision threshold, either provide the decision threshold used in the evaluation, or provide details on evaluation at different thresholds for the intended uses. 

## Evaluation data  

– see also instructions for training data 

Description, includes where the data came from, why it was chosen, size(s) of datasets, URL, date (when the dataset was downloaded) & version of the dataset, preprocessing methods, data augmentation methods 

URL-Link(s) to dataset card(s) with relevant information, if available  

## Evaluation Procedure 

Use cases – relevant context amongst which the model was evaluated  (e.g particular methods, materials, etc...)

Performance metrics, benchmarks tested, the baseline(s) and the current SOTA (achieved by other models) on the specified metrics. 

## Uncertainty Quantification. 
Describe how  uncertainty/variability is calculated 

## Evaluation results 

Evaluation Quality Metrics: E.g., the model achieved an accuracy of 0.997 during evaluation on X benchmark
Evaluation Runtime:  E.g., the evaluation took 2 days

If using inference time compute or tools, it may be appropriate to plot these as a function of compute effort.

 
# More Information (optional) 

Anything else you think it is important to communicate, but doesn't clearly fit under any other heading
