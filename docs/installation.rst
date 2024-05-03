Quick Start: Installation
=========================

#. If this is the first time using DSI, start by creating a DSI virtual environment with a name of your choice, e.g., **mydsi**:

   .. code-block:: unixconfig

      python -m venv mydsi

#. Then activate the environment (start here if you already have a DSI virtual environment) and install the latest pip in your environment:

   .. code-block:: unixconfig

      source mydsi/bin/activate
      pip install --upgrade pip

#. Go down into the project space root, clone the dsi repo and use pip to install dsi:

   .. code-block:: unixconfig

      git clone https://github.com/lanl/dsi.git
      cd dsi
      pip install .


#. When you've completed work, deactivate the environment with:

   .. code-block:: unixconfig

      deactivate
