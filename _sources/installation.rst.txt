Quick Start: Installation
=========================

#. Users should create a DSI virtual environment with a name of your choice, e.g., **mydsi**:

   Python virtual environment

   .. code-block:: unixconfig

      python3 -m venv mydsi
      source mydsi/bin/activate           # start from here if virtual environment exists
      pip install --upgrade pip
   
   Miniconda3 virtual environment
   
   .. code-block:: unixconfig

      . ~/miniconda3/bin/activate
      conda create -n mydsi python=3.11 
      conda activate mydsi                # only run this if virtual environment exists

#. Install DSI:

   Alpha-version: Clone the dsi repo, move into the new dsi directory, and use pip to install all base packages:

   .. code-block:: unixconfig

      git clone https://github.com/lanl/dsi.git
      cd dsi
      pip install .

   Supported release: Published versions on PyPI at https://pypi.org/project/dsi-workflow/. Install the latest version by:

   .. code-block:: unixconfig

      python3 -m pip install dsi-workflow

#. (Optional) If using all DSI functionalities, it is necessary to install requirements.extras.txt as well:

   .. code-block:: unixconfig

      pip install -r requirements.extras.txt

#. When you've completed work, deactivate the environment with:

   .. code-block:: unixconfig

      deactivate
