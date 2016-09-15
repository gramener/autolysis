# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import os
import shutil
import unittest
import subprocess
import pandas as pd
from nose.tools import eq_
from autolysis import meta, metadata
from pandas.util.testing import assert_frame_equal, assert_series_equal

BASE = {'x.csv', 'y.csv', 'z.json', 'x.dta'}


class TestMeta(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Run the tests from the data/ sub-folder
        cls.cwd = os.getcwd()
        folder = os.path.dirname(os.path.abspath(__file__))
        os.chdir(os.path.join(folder, 'data'))
        cls.root = os.path.join(folder, 'data', 'tempdir')
        if os.path.exists(cls.root):
            shutil.rmtree(cls.root)
        os.mkdir(cls.root)
        subprocess.check_output(['make'])

        files = list(BASE) + [
            'x.csv.xz', 'x.csv.gz', 'x.csv.bz2',        # COMPRESSED
            'y.csv.xz', 'y.csv.gz', 'y.csv.bz2',
            'xy.zip', 'xy.7z', 'xy.rar', 'xy.tar',      # ARCHIVES
            'xy.tar.xz', 'xy.tar.gz', 'xy.tar.bz2',     # COMPRESSED ARCHIVES
            'xy.db', 'xy.xlsx', 'y.h5',                 # DATABASES
            'zz.zip', 'zz.7z', 'zz.rar', 'zz.tar',      # COMPOSITE ARCHIVES
            'zz.tar.xz', 'zz.tar.gz', 'zz.tar.bz2',     # COMPOSITE COMPRESSED ARCHIVES
        ]
        cls.result = {}
        for filename in files:
            cls.result[filename] = metadata(filename, tqdm_disable=True)

    @classmethod
    def tearDownClass(cls):
        # Restory the previous working directory
        if os.path.exists(cls.root):
            shutil.rmtree(cls.root)
        os.chdir(cls.cwd)

    def test_filename(self):
        eq_(meta.filename('http://a.co/file.ext?q=1&y=2#z=3', root='/path'),
            os.path.abspath('/path/56f90b4e3b-file.ext'))
        eq_(meta.filename('http://a.co/?q=1&y=2#z=3', root='/path', path='file.ext'),
            os.path.abspath('/path/d8f477152f-file.ext'))

    def test_fetch(self):
        raise unittest.SkipTest('To be implemented')

    def test_guess_format(self):
        formats = [
            ('x.csv', 'csv'),
            ('z.json', 'json'),
            ('x.dta', 'dta'),
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
            eq_(meta.guess_format(path), fmt)
            eq_(meta.guess_format(path, True), fmt)

    def test_read_csv_encoded(self):
        raise unittest.SkipTest('To be implemented')

    def test_extract_archive(self):
        mapping = [
            ({'xy.zip', 'xy.7z', 'xy.rar', 'xy.tar'}, BASE),
            ({'x.csv.xz', 'x.csv.gz', 'x.csv.bz2'}, {'x.csv'}),
            ({'xy.tar.xz', 'xy.tar.gz', 'xy.tar.bz2'}, {'xy.tar'}),
        ]
        for archives, files in mapping:
            for archive in archives:
                target = meta.filename(archive, self.root)
                meta.extract_archive(archive, target, meta.guess_format(archive))
                eq_(set(os.listdir(target)), files)

    def test_unzip_files(self):
        raise unittest.SkipTest('To be implemented')

    def test_metadata(self):
        m = self.result['x.csv']
        self.assertDictContainsSubset({
            'source': 'x.csv',
            'format': 'csv',
            'rows': 2,
        }, m)

        data = pd.read_csv('x.csv', encoding='cp1252')
        assert_frame_equal(m['head'], data)
        assert_frame_equal(m['sample'], data)

        self.assertDictContainsSubset({
            'name': 'à',
            'type_pandas': 'int64',
            'missing': 0,
            'nunique': 2,
        }, m['columns']['à'])
        assert_series_equal(m['columns']['à']['top'],
                            pd.Series([1, 1], index=[3, 1], name='à'))
        assert_series_equal(
            m['columns']['à']['moments'],
            pd.Series(      [    2.0,    2.0, 1.4142135624,   1.0,   1.5,   2.0,   2.5,   3.0],
                      index=['count', 'mean',        'std', 'min', '25%', '50%', '75%', 'max'],
                      name='à'))
        self.assertDictContainsSubset({
            'name': 'è',
            'type_pandas': 'int64',
            'missing': 0,
            'nunique': 2,
        }, m['columns']['è'])
        assert_series_equal(m['columns']['è']['top'],
                            pd.Series([1, 1], index=[2, 4], name='è'))
        self.assertDictContainsSubset({
            'name': 'Unnamed: 2',
            'type_pandas': 'object',
            'missing': 1,
            'nunique': 1,
        }, m['columns']['Unnamed: 2'])
        assert_series_equal(m['columns']['Unnamed: 2']['top'],
                            pd.Series([1], index=['½'], name='Unnamed: 2'))

    def test_recursion(self):
        def children(sources, names):
            for source in sources:
                datasets = self.result[source].get('datasets', {})
                eq_(set(datasets.keys()), names)
                # TODO: check if dataset contents are equal

        children(BASE, set())
        children({'x.csv.xz', 'x.csv.gz', 'x.csv.bz2'}, {'x.csv'})
        children({'y.csv.xz', 'y.csv.gz', 'y.csv.bz2'}, {'y.csv'})
        children({'xy.zip', 'xy.7z', 'xy.rar', 'xy.tar'}, BASE)

    def test_output(self):
        for key in self.result:
            for method in ['json', 'yaml', 'text', 'markdown', 'dict']:
                getattr(self.result[key], 'to_' + method)()
