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
      pip install . -r requirements.txt

   Supported release: Published versions on PyPI at https://pypi.org/project/dsi-workflow/. Install the latest version by:

   .. code-block:: unixconfig

      python3 -m pip install dsi-workflow

#. (Optional) For intermediate or full DSI functionality, install one of the additional requirement files:

   .. code-block:: unixconfig

      pip install -r requirements.extras.txt
      pip install -r requirements.heavy.txt

#. When you've completed work, deactivate the environment with:

   .. code-block:: unixconfig

      deactivate


Quick Start: Installation
=========================

#. Create a virtual environment

   Choose one of the following methods:

   **Option A: Python venv**

   .. code-block:: unixconfig

      python3 -m venv mydsi
      source mydsi/bin/activate           # start here if virtual environment exists
      pip install --upgrade pip

   **Option B: Miniconda3**

   .. code-block:: unixconfig

      . ~/miniconda3/bin/activate
      conda create -n mydsi python=3.11
      conda activate mydsi                # start here if virtual environment exists

   **Option C: uv**

   .. code-block:: unixconfig

      uv venv
      source .venv/bin/activate

#. Install DSI

   **Option A: Alpha version (From source)**

   .. code-block:: unixconfig

      git clone https://github.com/lanl/dsi.git
      cd dsi

      pip install . -r requirements.txt

      # uv alternative
      uv pip install . -r requirements.txt

   **Option B: Supported release**

   Published versions are available on `PyPI <https://pypi.org/project/dsi-workflow/>`_:

   .. code-block:: unixconfig

      pip install dsi-workflow

      # uv alternative
      uv pip install dsi-workflow

#. (Optional) Install additional dependencies

   .. code-block:: unixconfig

      pip install -r requirements.extras.txt
      pip install -r requirements.heavy.txt

      # uv alternative
      uv pip install -r requirements.extras.txt
      uv pip install -r requirements.heavy.txt

#. Deactivate the environment

   .. code-block:: unixconfig

      deactivate