# -*- coding: utf-8 -*-

"""
test_autolysis
----------------------------------

Tests for `autolysis` module.
"""
from __future__ import absolute_import, division, print_function

import os
import re
import logging
import traceback
import pandas as pd
import autolysis as al
import sqlalchemy as sa
from odo import odo
from blaze import Data
from nose.tools import eq_, ok_
from numpy.testing import assert_array_almost_equal as aaq_

from . import DATA_DIR, config, server_exists


def setUpModule():
    "Download test data files into data/ target folder"

    # Set root logger logging level to INFO
    logging.basicConfig(level=logging.INFO)

    # Download datasets
    for dataset in config['datasets']:
        dataset['path'] = os.path.join(DATA_DIR, dataset['table'] + '.csv')
        dataset['uris'] = [dataset['path']]
        if not os.path.exists(dataset['path']):
            logging.info('Downloading %s', dataset['table'])
            pd.read_csv(dataset['url']).to_csv(dataset['path'], index=False)

    # Create autolysis databases (sqlite3 data directory is DATA_DIR)
    os.chdir(DATA_DIR)
    dburl = {}
    for db, url in config['databases'].items():
        if not server_exists(url):
            continue
        dburl[db] = url

    # Load datasets into databases
    for dataset in config['datasets']:
        for db in dataset.get('databases', []):
            if db not in dburl:
                logging.warning('%s cannot use unconfigured DB %s', dataset['table'], db)
                continue
            url = dburl[db]

            # Don't load data if a non-empty table already exists
            target = dburl[db] + '::' + dataset['table']
            dataset['uris'].append(target)
            engine = sa.create_engine(url)
            if engine.dialect.has_table(engine.connect(), dataset['table']):
                if Data(target).count() > 0:
                    continue
            logging.info('Creating table %s on %s', dataset['table'], db)
            try:
                odo(dataset['path'], target)
            except sa.exc.InternalError:
                logging.warning('Loading %s into %s failed: %s',
                                dataset['table'], db, traceback.format_exc(0))


def getengine(uris):
    'Return sql engine or csv for given uri'
    engines = []
    for uri in uris:
        engines.append(
            re.sub(
                r'[^a-zA-Z+]',
                r'',
                re.search(r'([\w|+]+:\/\/)|(.csv)', uri).group())
            )
    return engines


class TestImport(object):
    "Test autolysis import basics"
    def test_version(self):
        "autolysis has a version"
        ok_(hasattr(al, '__version__'))

    def test_release(self):
        "autolysis has a release information dict"
        ok_(hasattr(al, 'release'))
        ok_(isinstance(al.release, dict))


class TestGetNumericCols(object):
    "Test autolysis.get_numeric_cols"
    def test_numeric_cols(self):
        for dataset in config['datasets']:
            for uri in dataset['uris']:
                data = Data(uri)
                result = al.get_numeric_cols(data.dshape)
                eq_(set(result), set(dataset['types']['numbers']))
            print('for %s on %s' % (dataset['table'], getengine(dataset['uris'])))


class TestTypes(object):
    "Test autolysis.types"
    def check_type(self, result, expected, msg):
        "result = expected, but order does not matter. Both are dict of lists"
        eq_(set(result.keys()),
            set(expected.keys()), 'Mismatch: %s keys' % msg)
        for key in expected:
            eq_(set(result[key]),
                set(expected[key]), 'Mismatch: %s - %s' % (msg, key))

    def test_detect_types(self):
        for dataset in config['datasets']:
            for uri in dataset['uris']:
                data = Data(uri)
                result = al.types(data)
                self.check_type(result, dataset['types'], dataset['table'])
            print('for %s on %s' % (dataset['table'], getengine(dataset['uris'])))


class TestGroupMeans(object):
    "Test autolysis.groupmeans"
    def check_gain(self, result, expected, msg):
        gains = []
        while True:
            try:
                item = next(result)
                gains.append(item['gain'])
            except StopIteration:
                break
        if len(gains):
            # Ignoring type check, SQL might return Decimal
            # TODO: Sorting & comparing might not be a good idea.
            # Look for alternative comparison. create a series instead
            aaq_([float(i) for i in sorted(gains)],
                 sorted(expected),
                 4,
                 'Mismatch with URI: %s ' % msg)
        else:
            eq_([], expected, 'Mismatch with URI: %s ' % msg)

    def test_gains(self):
        for dataset in config['datasets']:
            for uri in dataset['uris']:
                data = Data(uri)
                types = al.types(data)
                result = al.groupmeans(data, types['groups'], types['numbers'])
                self.check_gain(result, dataset['groupmeans']['gain'], uri)
            print('for %s on %s' % (dataset['table'], getengine(dataset['uris'])))

    def test_gains_changed_types(self):
        # Issue #24
        for dataset in config['datasets']:
            if 'changedtypes' not in dataset:
                continue
            for uri in dataset['uris']:
                data = Data(uri)
                types = dataset['changedtypes']
                result = al.groupmeans(data, types['groups'], types['numbers'])
                self.check_gain(result, types['groupmeans']['gain'], uri)
            print('for %s on %s' % (dataset['table'], getengine(dataset['uris'])))


class TestCrossTabs(object):
    "Test autolysis.crosstabs"
    def check_stats(self, result, expected, msg):
        if result:
            stats = {}
            while True:
                try:
                    stats.update(next(result))
                except StopIteration:
                    break
            result = [stats[k]['stats'] for k in sorted(stats)]
            aaq_(pd.DataFrame(result),
                 pd.DataFrame(expected),
                 4,
                 'Mismatch with URI: %s ' % msg)
        else:
            eq_([], expected, 'Mismatch with URI: %s ' % msg)

    def test_stats(self):
        for dataset in config['datasets']:
            for uri in dataset['uris']:
                data = Data(uri)
                groups = dataset['types']['groups']
                result = al.crosstabs(data, groups)
                self.check_stats(result, dataset['crosstabs'], uri)
            print('for %s on %s' % (dataset['table'], getengine(dataset['uris'])))
