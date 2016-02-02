# -*- coding: utf-8 -*-

import os
import yaml
import logging
import sqlalchemy as sa

DIR = os.path.split(os.path.abspath(__file__))[0]
DATA_DIR = os.path.join(DIR, 'data')

# Ensure that data directory exists (to download files, save .sqlite3 data)
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# test_config.yaml documents sources & test results on datasets to be tested
with open(os.path.join(DIR, 'test_config.yaml')) as _dataset_file:
    config = yaml.load(_dataset_file)

# If the AUTOLYSIS_BIG environment variable is set, test large datasets
big_tests = os.environ.get('AUTOLYSIS_BIG')

# Ignore large datasets if AUTOLYSIS_BIG is unset
result = []
for dataset in config['datasets']:
    if dataset.get('big') and not big_tests:
        logging.warning('%s skipped (big dataset: set AUTOLYSIS_BIG to test)', dataset['table'])
    else:
        result.append(dataset)
config['datasets'] = result


def server_exists(url):
    'Return True if we can connect to the server in SQLAlchemy url'
    try:
        base_url = sa.engine.url.make_url(url)
        base_url.database = None
        engine = sa.create_engine(base_url)
        engine.connect()
        return True
    except sa.exc.OperationalError:
        logging.warning('Cannot connect to %s', url)
        return False
