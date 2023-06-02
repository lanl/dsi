===========================
How to build documentation
===========================

1. Create or activate a DSI virtual environmnet
2. ``cd`` into the project space root
3. ``python -m pip install sphinx sphinx_rtd_theme .``
4. ``cd ./docs``
5. ``make html``
6. ``make gh-pages``
7. Only if you want to update official documentation on Github: ``make publish``
