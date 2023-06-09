Installation
===================

1. Create or activate a DSI virtual environment. 
2. ``cd`` into the project space root
3. ``python -m pip install .``
4. [Optional] If you are running DSI Unit tests ``python -m pip install pytest gitpython coverage-badge pytest-cov``. 
5. [Optional] If you are HTML documentation ``python -m pip install sphinx sphinx_rtd_theme``

How to create and activate a virtual environment
--------------------------------------------------
We recommend Miniconda for virtual environment management (`https://docs.conda.io/en/latest/miniconda.html`). To create and activate a Miniconda virtual environment:

1. Download and install the appropriate Miniconda installer for your platform.
2. If this is the first time creating a DSI virtual environment: ``conda create -n 'dsi' python=3.11``. The ``-n`` name argument can be anything you like.
3. Once the virtual environment is created, activate it with ``conda activate dsi``, or whatever name you picked in the preceding step.
4. Proceed with Step 2 in the "Installation" section.
5. When you've completed work, deativate the conda environment with ``conda deactivate``.
 
