# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import os
import shutil
import unittest
import subprocess
from nose.tools import eq_
from autolysis import meta, metadata


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
            ({'xy.zip', 'xy.7z', 'xy.rar', 'xy.tar'}, {'x.csv', 'y.csv', 'z.json'}),
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

        head = {
            'columns': ['à', 'è', 'Unnamed: 2', '1'],
            'index': [0, 1],
            'data': [[1, 2, '½', 4.0], [3, 4, None, None]],
        }
        eq_(m['head'], head)
        eq_(m['sample'], head)

        self.assertDictContainsSubset({
            'name': 'à',
            'type_pandas': 'int64',
            'missing': 0,
            'nunique': 2,
            'top': {'1': 1, '3': 1}
        }, m['columns']['à'])
        eq_(m['columns']['à']['moments'], {
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
            'type_pandas': 'int64',
            'missing': 0,
            'nunique': 2,
            'top': {'2': 1, '4': 1}
        }, m['columns']['è'])
        self.assertDictContainsSubset({
            'name': 'Unnamed: 2',
            'type_pandas': 'object',
            'missing': 1,
            'nunique': 1,
            'top': {'½': 1}
        }, m['columns']['Unnamed: 2'])

    def test_recursion(self):
        def children(sources, names):
            for source in sources:
                datasets = self.result[source].get('datasets', {})
                eq_(set(datasets.keys()), set(names))
                # TODO: check if dataset contents are equal

        children(['x.csv', 'y.csv', 'z.json'], [])
        children(['x.csv.xz', 'x.csv.gz', 'x.csv.bz2'], ['x.csv'])
        children(['y.csv.xz', 'y.csv.gz', 'y.csv.bz2'], ['y.csv'])
        children(['xy.zip', 'xy.7z', 'xy.rar', 'xy.tar'], ['x.csv', 'y.csv', 'z.json'])
