# -*- coding: utf-8 -*-

"""
test_autolysis
----------------------------------

Tests for `autolysis` module.
"""

import os
import logging
import autolysis
import traceback
import pandas as pd
import sqlalchemy as sa
from odo import odo
from blaze import Data
from nose.tools import eq_
from six.moves.urllib.request import urlretrieve
from sqlalchemy_utils.functions import database_exists, create_database

from . import DATA_DIR, config, server_exists, big_tests

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
        if not database_exists(url):
            logging.warning('Creating database %s', url)
            create_database(url)
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
            data = Data(os.path.join(DATA_DIR, dataset['path']))
            result = autolysis.types(data)
            yield self.check_type, result, dataset['types'], dataset['table']
