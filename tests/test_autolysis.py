#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_autolysis
----------------------------------

Tests for `autolysis` module.
"""

import os
import yaml
import logging
import unittest
import autolysis
from blaze import Data
from six.moves.urllib.request import urlretrieve

_DIR = os.path.split(os.path.abspath(__file__))[0]
_DATA_DIR = os.path.join(_DIR, 'data')

# datasets.yaml documents sources & test results on datasets to be tested
with open(os.path.join(_DIR, 'datasets.yaml')) as _dataset_file:
    datasets = yaml.load(_dataset_file)


def setUpModule():
    'Download test data files into data/ target folder'
    if not os.path.exists(_DATA_DIR):
        os.makedirs(_DATA_DIR)
    for dataset in datasets:
        path = os.path.join(_DATA_DIR, dataset['path'])
        if not os.path.exists(path):
            logging.info('Downloading %s', dataset['path'])
            urlretrieve(dataset['url'], path)


class TestTypes(unittest.TestCase):
    'Test autolysis.types'
    longMessage = True

    def check_type(self, result, expected, msg):
        'result = expected, but order does not matter. Both are dict of lists'
        self.assertEqual(set(result.keys()),
                         set(expected.keys()), '%s keys' % msg)
        for key in expected:
            self.assertEqual(set(result[key]),
                             set(expected[key]), '%s - %s' % (msg, key))

    def test_types(self):
        for dataset in datasets:
            data = Data(os.path.join(_DATA_DIR, dataset['path']))
            result = autolysis.types(data)
            self.check_type(result, dataset['types'], dataset['path'])

if __name__ == '__main__':
    import sys
    sys.exit(unittest.main())
