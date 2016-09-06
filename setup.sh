#!/usr/bin/env bash
#
# Autolysis setup script on Linux system. See this link for usage:
# https://github.com/gramener/autolysis/blob/dev/CONTRIBUTING.rst

command_exists() {
  command -v "$@" > /dev/null 2>&1
}

install_databases() {
    echo "Installing PostgreSQL"
    sudo apt-get -y install postgresql postgresql-contrib
    echo "Installing MySQL"
    sudo DEBIAN_FRONTEND=noninteractive apt-get -y -q install mysql-server
}

install_packages() {
    echo "Installing 7zip, rar"
    sudo apt-add-repository multiverse
    sudo apt-get update
    sudo apt-get install p7zip-full p7zip-rar rar
}

setup_python() {
    # Install in $CONDAPATH, default to $HOME/miniconda
    export BASE=${CONDAPATH:-$HOME/miniconda}
    export PATH="$BASE/bin:$PATH"             # Add Conda to path

    if command_exists conda; then
        echo "Using existing $(conda -V 2>&1)"
    else
        rm -rf $BASE                          # Delete conda dir to re-install
        # Get latest Miniconda, 64-bit
        wget -qO miniconda.sh http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh
        bash miniconda.sh -b -p $BASE         # Install Conda
        rm miniconda.sh                       # Remove miniconda setup
        # Create a test environment called autolysis
        conda create --yes --name autolysis-$TRAVIS_PYTHON_VERSION python=$TRAVIS_PYTHON_VERSION
    fi

    conda config --set always_yes True        # Don't prompt user
    conda config --set changeps1 False        # Don't show (env) in command prompt after "activate <env>"
    conda config --add channels menpo         # Add channel to install pathlib
    conda update conda                        # Update Miniconda

    # Install requirements into autolysis environment
    source activate autolysis-$TRAVIS_PYTHON_VERSION
    conda install --file requirements-conda.txt
    pip install -r requirements.txt
    pip install -r requirements-dev.txt
}

create_databases() {
    echo "Creating PostgreSQL database and user"
    sudo -u postgres psql --quiet <<EOF
CREATE DATABASE autolysistest;
CREATE USER autolysistest WITH SUPERUSER PASSWORD 'autolysistest';
GRANT ALL PRIVILEGES ON DATABASE autolysistest TO autolysistest;
EOF

    echo "Creating MySQL database and user"
    mysql --user root --force <<EOF
CREATE USER 'autolysistest'@'localhost' IDENTIFIED BY 'autolysistest';
CREATE DATABASE autolysistest;
GRANT ALL PRIVILEGES ON autolysistest.* TO 'autolysistest'@'localhost';
GRANT FILE ON *.* TO 'autolysistest'@'localhost';
FLUSH PRIVILEGES;
EOF
}


drop_databases() {
    echo "Dropping PostgreSQL database and user"
    psql --quiet --username postgres <<EOF
DROP DATABASE IF EXISTS autolysistest;
DROP USER IF EXISTS autolysistest;
EOF

    echo "Dropping MySQL database and user"
    mysql --user root --force <<EOF
DROP DATABASE IF EXISTS autolysistest;
DROP USER 'autolysistest'@'localhost';
EOF
}

$@
