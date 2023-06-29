Pylama
======

If you want to ensure your commits meet PEP8 style guidelines for every commit you make. Do the following::

        cp .githooks/pre-commit .git/hooks/
        chmod +x .git/hooks/pre-commit

How to use it
=============

First, blessings upon you for helping. It is likely that there are many style problems with the code most of the time. The git commit hook will only tell you about the style present in your current commit. If you are on a quest to improve style, I recommend that you delete any cached python files in directories like ``build/`` or ``*.egg*``, and then run `pylama` from the root of the project. A list of current style problems will appear. Pick one of the files affected, and begin correcting the problems it flags, or create exceptions using syntax which is documented elsewhere online. It's useful to include comments about why the style note has been ignored. When you are done with the file, ``git add`` and then ``git commit``. Your changes to this specific file will be linted, and new errors might appear. Iterate until you can commit your changes, then run pylama again. Rinse and repeat.

Alternatively, you could use an autoformatter like autopep8. I am personally fearful of this.
