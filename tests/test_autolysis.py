#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_autolysis
----------------------------------

Tests for `autolysis` module.
"""

import os
import sys
import yaml
import logging
import unittest
import autolysis
import traceback
import sqlalchemy as sa
from odo import odo
from blaze import Data
from six.moves.urllib.request import urlretrieve
from sqlalchemy_utils.functions import database_exists, create_database, drop_database

_DIR = os.path.split(os.path.abspath(__file__))[0]
_DATA_DIR = os.path.join(_DIR, 'data')

# test_config.yaml documents sources & test results on datasets to be tested
with open(os.path.join(_DIR, 'test_config.yaml')) as _dataset_file:
    config = yaml.load(_dataset_file)


def setUpModule():
    'Download test data files into data/ target folder'

    # Download datasets
    if not os.path.exists(_DATA_DIR):
        os.makedirs(_DATA_DIR)
    for dataset in config['datasets']:
        dataset['path'] = os.path.join(_DATA_DIR, dataset['table'] + '.csv')
        if not os.path.exists(dataset['path']):
            logging.info('Downloading %s', dataset['table'])
            urlretrieve(dataset['url'], dataset['path'])

    # Create autolysis databases
    # _databases('drop')
    dburl = _databases('create')

    # Load datasets into databases
    for dataset in config['datasets']:
        for db in dataset.get('databases', []):
            if db not in dburl:
                logging.warning('%s cannot use unconfigured DB %s', dataset['table'], db)
                continue
            db_url = dburl[db]

            # Don't load data if a non-empty table already exists
            target = dburl[db] + '::' + dataset['table']
            engine = sa.create_engine(db_url)
            if engine.dialect.has_table(engine.connect(), dataset['table']):
                if Data(target).count() > 0:
                    continue
            logging.info('Creating table %s on %s', dataset['table'], db)
            try:
                odo(dataset['path'], target)
            except sa.exc.InternalError as e:
                logging.warning('Loading %s into %s failed: %s',
                             dataset['table'], db, traceback.format_exc(0))


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
        for dataset in config['datasets']:
            data = Data(os.path.join(_DATA_DIR, dataset['path']))
            result = autolysis.types(data)
            self.check_type(result, dataset['types'], dataset['path'])


def _databases(operation):
    '''
    _databases('create') creates all databases.
    _databases('drop') drops all databases.
    '''
    # Drop autolysis databases. (sqlite3 data directory is _DATA_DIR)
    os.chdir(_DATA_DIR)
    result = {}
    for db, db_url in config['databases'].items():
        # If we can't connect to the database, skip.
        try:
            base_url = sa.engine.url.make_url(db_url)
            base_url.database = None
            engine = sa.create_engine(base_url)
            engine.connect()
        except sa.exc.OperationalError:
            logging.warning('Unable to connect to %s to %s', db_url, operation)
            continue

        # If we can connect to the database, try creating / dropping tables
        if 'create' in operation.lower():
            if not database_exists(db_url):
                logging.warning('Creating database %s', db_url)
                create_database(db_url)
            result[db] = db_url
        if 'drop' in operation.lower():
            if database_exists(db_url):
                logging.warning('Dropping database %s', db_url)
                drop_database(db_url)
            result[db] = db_url
    return result


if __name__ == '__main__':
    import sys
    sys.exit(unittest.main())
