Pylama
======
First, make sure you have pylama in your pip venv::

        python -m pip install pylama


If you want to ensure your commits meet PEP8 style guidelines for every commit you make. Do the following::

        cp .githooks/pre-commit .git/hooks/
        chmod +x .git/hooks/pre-commit

For a special ASCII treat to reward your righteous labors, also do::
        
        cp .githooks/post-commit .git/hooks/
        chmod +x .git/hooks/post-commit

How to use it
=============

First, blessings upon you for helping. It is likely that there are many style problems with the code most of the time. The git commit hook will only tell you about the style present in your current commit. If you are on a quest to improve style, we recommend that you delete any cached python files in directories like ``build/`` or ``*.egg*``, and then run `pylama` from the root of the project. A list of current style problems will appear. Pick one of the files affected, and begin correcting the problems it flags, or create exceptions using syntax which is documented elsewhere online. It's useful to include comments about why the style note has been ignored. When you are done with the file, ``git add`` and then ``git commit``. Your changes to this specific file will be linted, and new errors might appear. Iterate until you can commit your changes, then run pylama again. Rinse and repeat.

How to go faster
================

To go faster (highly recommended), first install autopep8 in your pip venv::

        python -m pip install autopep8


You can use the ``autopep8`` automatic formatter to fix many mistakes in the code automatically. It won't catch everything, but it will get a lot of it. Using this to blindly replace strings in the code seems like a bad idea. Instead, we recommend that you do the following:

1.``autopep8 my_file >tmp``
2. ``diff my_file tmp``
3. Confirm that the changes are not breaking or bad some other way.
4. If the changes look reasonable ``autopep8 -i my_file``
5. ``git add my_file && git commit -m 'Style changes for my_file'``
6. If the pylama commit hook still fails, fix the remaining problems manually.
7. Add and commit the file, repeat for all other files you wish to fix.
