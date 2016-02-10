#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Require setuptools -- distutils does not support install_requires
from setuptools import setup, find_packages
from pip.req import parse_requirements
from pip.download import PipSession
import json

setup(
    long_description=(open('README.rst').read() + '\n\n' +
                      open('HISTORY.rst').read().replace('.. :changelog:', '')),
    packages=find_packages(),
    package_dir={'autolysis':
                 'autolysis'},
    # Read: http://stackoverflow.com/a/2969087/100904
    # package_data includes data files for binary & source distributions
    # include_package_data is only for source distributions, uses MANIFEST.in
    package_data={
      'autolysis': ['release.json']
    },
    include_package_data=True,
    install_requires=[str(entry.req) for entry in
                      parse_requirements('requirements.txt', session=PipSession())],
    zip_safe=False,
    test_suite='tests',
    tests_require=[],
    **json.load(open('autolysis/release.json'))
)
