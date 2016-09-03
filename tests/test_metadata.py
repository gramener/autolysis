# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import os
import unittest
import subprocess
from nose.tools import eq_
from autolysis import meta


class TestMetaData(unittest.TestCase):

    def setUp(self):
        # Run the tests from the data/ sub-folder
        self.cwd = os.getcwd()
        folder = os.path.dirname(os.path.abspath(__file__))
        os.chdir(os.path.join(folder, 'data'))
        subprocess.check_output(['make'])
        self.eq_ = self.assertEqual

        files = [
            'x.csv', 'y.csv', 'z.json',                 # BASE
            'x.csv.xz', 'x.csv.gz', 'x.csv.bz2',        # COMPRESSED
            'y.csv.xz', 'y.csv.gz', 'y.csv.bz2',
            'xy.zip', 'xy.7z', 'xy.rar', 'xy.tar',      # ARCHIVES
            'xy.tar.xz', 'xy.tar.gz', 'xy.tar.bz2',     # COMPRESSED ARCHIVES
            'xy.db', 'xy.xlsx', 'y.h5',                 # DATABASES
            'zz.zip', 'zz.7z', 'zz.rar', 'zz.tar',      # COMPOSITE ARCHIVES
            'zz.tar.xz', 'zz.tar.gz', 'zz.tar.bz2',     # COMPOSITE COMPRESSED ARCHIVES
        ]
        self.result = {}
        for filename in files:
            self.result[filename] = meta.analyze(filename)


    def tearDown(self):
        # Restory the previous working directory
        os.chdir(self.cwd)

    def test_format(self):
        formats = [
            ('x.csv', 'csv'),
            ('z.json', 'json'),
            ('x.csv.xz', 'xz'),
            ('x.csv.gz', 'gz'),
            ('x.csv.bz2', 'bz2'),
            ('xy.zip', 'zip'),
            ('xy.7z', '7z'),
            ('xy.tar', 'tar'),
            ('xy.rar', 'rar'),
            ('xy.db', 'sqlite3'),
            ('xy.xlsx', 'xlsx'),
            ('y.h5', 'hdf5'),
        ]
        for path, fmt in formats:
            self.eq_(meta._guess_format(path), fmt)
            self.eq_(meta._guess_format(path, True), fmt)

    def test_recursion(self):
        def children(sources, names):
            for source in sources:
                datasets = self.result[source].get('datasets', [])
                children = [dataset['name'] for dataset in datasets]
                self.eq_(set(children), set(names))
                # TODO: check equality
                # for dataset in datasets:
                #     self.eq_(dataset, self.result[dataset['name']])

        children(['x.csv', 'y.csv', 'z.json'], [])
        children(['x.csv.xz', 'x.csv.gz', 'x.csv.bz2'], ['x.csv'])
        children(['y.csv.xz', 'y.csv.gz', 'y.csv.bz2'], ['y.csv'])
        children(['xy.zip', 'xy.7z', 'xy.rar', 'xy.tar'], ['x.csv', 'y.csv', 'z.json'])

    def test_metadata(self):
        m = self.result['x.csv']
        self.eq_(m['source'], 'x.csv')
        self.eq_(m['format'], 'csv')
        self.eq_(m['rows'], 2)

        head = {
            'columns': ['à', 'è', 'Unnamed: 2', '1'],
            'index': [0, 1],
            'data': [[1, 2, '½', 4.0], [3, 4, None, None]],
        }
        eq_(m['head'], head)
        eq_(m['sample'], head)

        self.assertDictContainsSubset({
            'name': 'à',
            'type:pandas': 'int64',
            'missing': 0,
            'nunique': 2,
            'top': {'1': 1, '3': 1}
        }, m['columns'][0])
        self.eq_(m['columns'][0]['moments'], {
            '25%': 1.5,
            '50%': 2.0,
            '75%': 2.5,
            'count': 2.0,
            'max': 3.0,
            'mean': 2.0,
            'min': 1.0,
            'std': 1.4142135624
        })
        self.assertDictContainsSubset({
            'name': 'è',
            'type:pandas': 'int64',
            'missing': 0,
            'nunique': 2,
            'top': {'2': 1, '4': 1}
        }, m['columns'][1])
        self.assertDictContainsSubset({
            'name': 'Unnamed: 2',
            'type:pandas': 'object',
            'missing': 1,
            'nunique': 1,
            'top': {'½': 1}
        }, m['columns'][2])
