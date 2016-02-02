# -*- coding: utf-8 -*-

"""
drop_test
----------------------------------

Drops databases and other setup files for tests.

Usage: python setup.py nosetests -m"drop_test"
"""

import os
import logging
from sqlalchemy_utils.functions import database_exists, drop_database
from . import DATA_DIR, config, server_exists


def drop_test_databases():
    os.chdir(DATA_DIR)
    for db, url in config['databases'].items():
        if server_exists(url) and database_exists(url):
            logging.warning('Dropping database %s', url)
            drop_database(url)
