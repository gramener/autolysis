#!/usr/bin/env bash
#
# Autolysis setup script on Linux system. Usage:
#
#   source <(wget -qO- https://raw.githubusercontent.com/gramener/autolysis/setup.sh)

# Install git and make
# Clone autolysis
# Install databases

# Install MiniConda 2.4.1
# -------------------------------------
# Install in $CONDAPATH, default to $HOME/miniconda
export BASE=${CONDAPATH:-$HOME/miniconda}

# Get latest Miniconda, 64-bit
wget -qO miniconda.sh http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh
bash miniconda.sh -b -p $BASE             # Install Conda
export PATH="$BASE/bin:$PATH"             # Add Conda to path

conda config --set always_yes True        # Don't prompt user
conda config --set changeps1 False        # Don't show (env) in command prompt after "activate <env>"
conda config --add channels menpo         # Add channel to install pathlib
conda config --add channels pdrops        # Add channel to install sqlalchemy-utils
conda update conda                        # Update Miniconda

# Create a test environment called autolysis
conda create -n autolysis --file requirements.txt --file requirements-dev.txt python=$TRAVIS_PYTHON_VERSION
source activate autolysis
