===========================
How to build documentation
===========================

1. Create or activate a DSI virtual environment †
2. Go down into the project space root and pip install dsi along with other libraries needed for the documentation:

.. code-block:: unixconfig

   cd dsi
   pip install .
   pip install sphinx sphinx_rtd_theme pytest-cov coverage-badge gitpython .
   pip install pyarrow nbconvert pydantic pandas pydot .
   pip install graphviz .

Note that if graphviz does not install, you may need to install via homebrew or manually build.

3. Go down into the docs directory.

.. code-block:: unixconfig

   cd dsi

4. Make your changes to the documentation.  Use:

.. code-block:: unixconfig

   make html

to build the documents.  When complete, push the changes to the repo.

5. Then trigger the changes to the gh-pages via:

.. code-block:: unixconfig

   make gh-pages

6. And update the official documentation on Github (this may take a few minutes to propogate):

.. code-block:: unixconfig

   make publish

====================================================
† How to create and activate a virtual environment
====================================================
We suggest creating a python environment:

1. Navigate to the project workspace.
2. If this is the first time creating a DSI virtual environment, choose a name, e.g. *mydsi*:

.. code-block:: unixconfig

   python -m venv mydsi

3. Then activate the environment:

.. code-block:: unixconfig

   source mydsi/bin/activate

4. Proceed with Step 2 in the "How to build documentation" section.
5. When you've completed work, deativate the environment with ``deactivate`` .
