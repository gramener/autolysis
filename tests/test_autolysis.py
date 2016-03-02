# -*- coding: utf-8 -*-

"""
test_autolysis
----------------------------------

Tests for `autolysis` module.
"""

import os
import logging
import warnings
import autolysis
import traceback
import pandas as pd
import sqlalchemy as sa
from odo import odo
from blaze import Data
from nose.tools import eq_, ok_

from . import DATA_DIR, config, server_exists


def setUpModule():
    'Download test data files into data/ target folder'

    # Set root logger logging level to INFO
    logging.basicConfig(level=logging.INFO)

    # Download datasets
    for dataset in config['datasets']:
        dataset['path'] = os.path.join(DATA_DIR, dataset['table'] + '.csv')
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


class TestImport(object):
    'Test autolysis import basics'
    def test_version(self):
        'autolysis has a version'
        ok_(hasattr(autolysis, '__version__'))

    def test_release(self):
        'autolysis has a release information dict'
        ok_(hasattr(autolysis, 'release'))
        ok_(isinstance(autolysis.release, dict))


class TestTypes(object):
    'Test autolysis.types'
    def check_type(self, result, expected, msg):
        'result = expected, but order does not matter. Both are dict of lists'
        eq_(set(result.keys()),
            set(expected.keys()), 'Mismatch: %s keys' % msg)
        for key in expected:
            eq_(set(result[key]),
                set(expected[key]), 'Mismatch: %s - %s' % (msg, key))

    def test_types(self):
        for dataset in config['datasets']:
            uris = [dataset['path']]
            for db in dataset['databases']:
                if db in config['databases']:
                    uris.append(config['databases'][db] + '::' + dataset['table'])
            for uri in uris:
                data = Data(uri)
                result = autolysis.types(data)
                yield self.check_type, result, dataset['types'], dataset['table']


class TestGroupMeans(object):
    'Test autolysis.groupmeans'
    def test_groupmeans(self):
        for dataset in config['datasets']:
            uris = [dataset['path']]
            for db in dataset['databases']:
                if db in config['databases']:
                    uris.append(config['databases'][db] + '::' + dataset['table'])
            for uri in uris:
                data = Data(uri)
                types = autolysis.types(data)
                result = autolysis.groupmeans(data, types['groups'], types['numbers'])
                warnings.warn("Only checking if autolysis.groupmeans"
                              " is running without throwing any error.")
