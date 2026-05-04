Quick Start: Installation
=========================

#. Create a virtual environment - choose one of the following options:

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