Quick Start: Installation
=========================

#. If this is the first time using DSI, start by creating a DSI virtual environment with a name of your choice, e.g., **mydsi**:

   .. code-block:: unixconfig

      python -m venv mydsi

#. Then activate the environment (start here if you already have a DSI virtual environment):

   .. code-block:: unixconfig

      source mydsi/bin/activate

#. Go down into the project space root and use pip to install dsi:

   .. code-block:: unixconfig

      cd dsi
      pip install .


#. [Optional] If you are running DSI unit tests, you may need other packages:

   .. code-block:: unixconfig

      pip install pytest gitpython coverage-badge pytest-cov .

   Plus ``pip install`` any other packages that your unit tests may need.

#. [Optional] If you are updating the GitHub pages documentation, see `DSI Documentation README <https://github.com/lanl/dsi/blob/main/docs/README.rst>`_ for additional python packages needed.

#. When you've completed work, deactivate the environment with:

   .. code-block:: unixconfig

      deactivate
