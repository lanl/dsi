# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
from dsi._version import __version__
from pathlib import Path

project = 'DSI'
copyright = '2025, Triad National Security, LLC. All rights reserved. LA-UR-25-29248'
author = 'The DSI Project team'
release = __version__

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ['sphinx_rtd_theme', 'sphinx.ext.autodoc', 'sphinx.ext.autosectionlabel']

# Make sure the target is unique
autosectionlabel_prefix_document = True
autosectionlabel_maxdepth = 3

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store', 'README.rst']

rst_prolog = f".. |version_num| replace:: {__version__}"

# change version in cli output automatically
template_path = Path(__file__).parent / "images" / "cli_output.txt.in"
output_path = Path(__file__).parent / "images" / "cli_output.txt"

rendered = template_path.read_text(encoding="utf-8").replace("{{VERSION}}", __version__)
if not output_path.exists() or output_path.read_text(encoding="utf-8") != rendered:
    output_path.write_text(rendered, encoding="utf-8")

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
html_css_files = ['custom.css']
