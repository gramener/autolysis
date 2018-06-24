#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from io import open
# Require setuptools -- distutils does not support install_requires
from setuptools import setup, find_packages


install_requires = [
    # Libraries required for Autolysis
    'pandas >= 0.17',              # Installs NumPy and python-dateutil
    'scipy >= 0.16',
    'blaze == 0.9.1',
    'odo == 0.4.2',
    # For setup
    'setuptools >= 16.0',          # 16.0 has good error message support
    # General utilities
    'six',                         # Python 3 compatibility
    'pathlib',                     # Manipulate paths. Part of Python 3.3+
    'orderedattrdict >= 1.4.2',    # Treat ordered dict keys as attributes
    'tqdm',                        # For displaying progress
    'XlsxWriter >= 0.9',           # For metadata to read/write Excel files
    'xlrd',                        # For reading Excel data
    # PyMySQL 0.7.8 causes threading failures consistently.
    # PyMySQL 0.7.2 is the last known working version.
    'PyMySQL == 0.7.2'
]

setup(
    long_description=(open('README.rst', encoding='utf-8').read() + '\n\n' +
                      open('HISTORY.rst', encoding='utf-8').read().replace('.. :changelog:', '')),
    packages=find_packages(),
    package_dir={'autolysis': 'autolysis'},
    # Read: http://stackoverflow.com/a/2969087/100904
    # package_data includes data files for binary & source distributions
    # include_package_data is only for source distributions, uses MANIFEST.in
    package_data={
      'autolysis': ['release.json']
    },
    include_package_data=True,
    install_requires=install_requires,
    zip_safe=False,
    test_suite='tests',
    tests_require=[],
    **json.load(open('autolysis/release.json', encoding='utf-8'))
)
