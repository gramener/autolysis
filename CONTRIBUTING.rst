.. highlight:: shell

============
Contributing
============

.. _issues page: https://github.com/gramener/autolysis/issues

- **Report Bugs** on the `issues page`_ with detailed steps to reproduce the bug,
  and your Anaconda version
- **Fix Bugs** on the `issues page`_ for anything tagged "bug".
- **Implement features** on the `issues page`_ for anything tagged "feature".
- **Write Documentation**, either as part of the official Autolysis docs, in
  docstrings, or in blog posts.

Setup
-----

- The `master <https://github.com/gramener/autolysis/tree/master/>`__ branch
  holds the latest stable version.
- The `dev <https://github.com/gramener/autolysis/tree/dev/>`__ branch has the
  latest development version
- Any other branches are temporary feature branches

Autolysis runs on Python 2.7+ and Python 3.4+ in Windows and Linux.
To set up the development environment on Ubuntu, run this script::

    source <(wget -qO- https://github.com/gramener/autolysis/issues/raw/master/setup.sh)

To manually set up the development environment, follow these steps.

1. Install `Anaconda <http://continuum.io/downloads>`__ (version 2.4 or higher)
2. Install `git` and `make`. On Windows, use
   `git <https://git-scm.com/>`__ and
   `make <http://gnuwin32.sourceforge.net/packages/make.htm>`__, or use
   `cygwin <https://cygwin.com/install.html>`__.
3. Clone the `Autolysis repo <https://github.com/gramener/autolysis>`__::

        git clone git@github.com:gramener/autolysis.git
        cd autolysis
        git checkout dev

4. Install development requirements, and also this branch in editable mode. This
   "installs" the autolysis folder in development mode. Changes to this folder
   are reflected in the environment::

      pip install -r requirements.txt         # Base requirements
      pip install -r requirements-dev.txt     # Additional development requirements
      pip uninstall autolysis                 # Uninstall prior autolysis repo
      pip install -e .                        # Install this repo as autolysis

   Any changes made to the ``autolysis`` repo will automatically be reflected
   when you import the package.

5. Install MySQL and PostgreSQL. Ensure that you can connect using the following
   SQLAlchemy strings::

        postgresql://postgres@localhost/autolysistest
        mysql+pymysql://root@localhost/autolysistest

   Notes: mysql.ini must not have a ``secure-file-priv`` flag set.

Contributing to autolysis
-------------------------

1. Create a branch for local development to make local changes::

        $ git checkout -b <branch-name>

2. When you're done making changes, check that your changes pass flake8 and the
   tests, as well as provide reasonable test coverage::

        make release-test

   To run a subset of tests::

        python -m unittest tests.test_types

   **Note**: This uses the ``python.exe`` in your ``PATH``. To change the Python
   used, run::

        export PYTHON=/path/to/python         # e.g. path to Python 3.4+

3. Commit your changes and push your branch::

      $ git add .
      $ git commit -m "Your detailed description of your changes."
      $ git push --set-upstream origin <branch-name>

4. Submit a pull request through the code.gramener.com website.

5. To delete your branch::

      git branch -d <branch-name>
      git push origin --delete <branch-name>

Pull Request Guidelines
-----------------------

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include tests.
2. If the pull request adds functionality, the docs should be updated. Put
   your new functionality into a function with a docstring, and add the
   feature to the list in README.rst.
3. The pull request should work for Python 2.7, 3.4 and 3.5

Release
-------

When releasing a new version of Autolysis:

1. Test the ``dev`` branch by running::

    export PYTHON=/path/to/python2.7
    make release-test
    export PYTHON=/path/to/python3.4
    make release-test
    export PYTHON=/path/to/python3.5
    make release-test

2. Build and upload the release::

    make release

3. Update the following and commit:
    - ``docs/HISTORY.rst`` -- add release notes
    - ``README.rst`` -- update the version number

4. Merge with master, create an annotated tag and push the code::

    git checkout master
    git merge dev
    git tag -a v1.x.x           # Annotate with a one-line summary of features
    git push --follow-tags
