'''
Generates standardised metadata for tabular datasets. Typical usage::

    from autolysis import meta
    info = meta.analyze('tests/data/x.csv')
    info = meta.analyze('file:///tests/data/x.csv')
'''
from __future__ import unicode_literals

import io
import os
import json
import time
import shutil
import requests
import patoolib
import pandas as pd
import sqlalchemy as sa
from hashlib import md5
from six import text_type
from collections import OrderedDict
from six.moves.urllib_parse import urlparse

OK = 200                    # HTTP status code
seconds_per_day = 86400     # Number of seconds in a day


def read_csv(*args, **kwargs):
    '''
    Read a CSV file via Pandas ``read_csv`` with different encodings. By default,
    try CP-1252 and then UTF-8. Set ``encodings=('encoding', ...)`` to change the
    list of encodings to try.
    '''
    for encoding in kwargs.pop('encodings', ('cp1252', 'utf-8')):
        try:
            kwargs['encoding'] = encoding
            return pd.read_csv(*args, **kwargs)
        except UnicodeError:
            pass


class Metadata(object):
    '''
    Retrieve metatadata from a data source.

        meta = Metadata(path='.')   # Metadata object that stores temp data in path
        meta.metadata('tests/data/x.csv')   # Return metadata dict for a local file
    '''

    def __init__(self, path=None):
        if path is None:
            path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.cache')
        if not os.path.exists(path):
            os.makedirs(path)
        self.path = path

    def extract(self, source, tables=None):
        tree = OrderedDict(source=source)
        urlinfo = urlparse(source)
        scheme = urlinfo.scheme

        if os.path.exists(source) or scheme in {'file'}:
            tree.update(self._extract_file(source))
        elif scheme in {'http', 'https', 'ftp'}:
            target = self._filename(source)
            fetch(source, target)
            tree.update(self._extract_file(target))
        else:
            tree['format'] = 'sql'
            tree.update(self._extract_sql(source, tables))
        return tree

    def _extract_sql(self, source, tables=None):
        try:
            engine = sa.create_engine(source)
        except sa.exc.ArgumentError:
            raise NotImplementedError('Cannot process source %s' % source)
        if tables is None:
            inspector = sa.inspect(engine)
            tables = inspector.get_table_names()
        tree = OrderedDict()
        tree['datasets'] = []
        for table in tables:
            tree['datasets'].append(OrderedDict([
                ('name', table),
                ('format', 'table'),
                ('command', ['sql', table, source]),
            ]))
        return tree

    def _extract_file(self, path, tables=None):
        '''
        Support function for get_metadata(). Returns the metadata for a file.
        There are 3 types of file formats:

        1. Archives (7z, zip, rar, tar) / compressed (xz, bzip2, gzip). Decompress and process
        2. Database (sqlite3, hdf5, xls, xlsx). Process each table/sheet as a sub-dataset
        3. Data (csv, json). Process directly
        '''
        tree = OrderedDict()
        format = self._guess_format(path)
        if format is not None:
            tree['format'] = format

        if format in {'7z', 'zip', 'rar', 'tar', 'xz', 'gz', 'bz2'}:
            tree['datasets'] = []
            for name, source in self._unzip_files(path, format):
                submeta = OrderedDict(name=name)
                submeta.update(self._extract_file(source))
                tree['datasets'].append(submeta)
        elif format == 'sqlite3':
            tree.update(self._extract_sql('sqlite:///' + path, tables))
        elif format in {'hdf5', 'xls', 'xlsx'}:
            if format == 'hdf5':
                store = pd.HDFStore(path)
                table_list = store.keys()
                store.close()
            else:
                xls = pd.ExcelFile(path)
                table_list = xls.sheet_names
                format = 'xlsx'
            tree['datasets'] = []
            for table in table_list:
                if tables is None or table in tables:
                    tree['datasets'].append(OrderedDict([
                        ('name', table),
                        ('format', 'table'),
                        ('command', [format, path, table])
                    ]))
        elif format == 'csv':
            tree['command'] = ['csv', path]
        elif format == 'json':
            tree['command'] = ['json', path]
        return tree

    def walk(self, tree):
        yield tree
        for node in tree.get('datasets', []):
            for subnode in self.walk(node):
                yield subnode

    _read_command = {
        'csv': read_csv,
        'json': pd.read_json,
        'sql': pd.read_sql_table,
        'xlsx': pd.read_excel,
        'hdf5': pd.read_hdf,
    }

    def analyze(self, source, tables=None, merge=True, **kwargs):
        tree = self.extract(source, tables)
        for node in self.walk(tree):
            cmd = node.pop('command', [None])
            if cmd[0] in self._read_command:
                data = self._read_command[cmd[0]](*cmd[1:])
                node.update(self._get_metadata_frame(data, **kwargs))
        # Merge column metadata of common datasets'
        if merge:
            for node in self.walk(tree):
                if 'datasets' in node:
                    sign_lookup = {}
                    for data in node['datasets']:
                        if 'columns' in data:
                            sign = tuple(col['name'] for col in data['columns'])
                            if sign in sign_lookup:
                                data['columns'] = {'see': sign_lookup[sign]['name']}
                            else:
                                sign_lookup[sign] = data
        return tree

    _ext_map = {
        '7zip': '7z',
        'db': 'sqlite3',
        'h5': 'hdf5',
    }

    def _guess_format(self, path, ignore_ext=False):
        '''
        Returns file format for data files based on the file extension or signature.
        File signatures / magic are from http://www.garykessler.net/library/file_sigs.html
        '''
        base, ext = os.path.splitext(path)
        ext = ext.lower()
        if ext and not ignore_ext:
            ext = ext[1:]
            return self._ext_map.get(ext, ext)

        # Guess based on signature
        signature_length = 20       # It's enough to read ~20 bytes to identify the file
        with io.open(path, 'rb') as handle:
            head = handle.read(signature_length)
        if head.startswith(b'7z\xbc\xaf\x27\x1c'):
            return '7z'
        if head.startswith(b'PK') and head[2:4] in {b'\x03\x04', b'\x05\x06', b'\x07\x08'}:
            from zipfile import ZipFile
            for filename in ZipFile(path).namelist():
                if filename.startswith('xl/'):
                    return 'xlsx'
            return 'zip'
        if head.startswith(b'Rar!\x1A\x07\x00'):
            return 'rar'
        if head.startswith(b'ustar') or head.startswith(b'./PaxHeaders'):
            return 'tar'
        if head.startswith(b'\xfd7zXZ\x00'):
            return 'xz'
        if head.startswith(b'\x1f\x8b'):
            return 'gz'
        if head.startswith(b'BZh'):
            return 'bz2'
        if head.startswith(b'SQLite format 3\x00'):
            return 'sqlite3'
        if head.startswith(b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1'):
            return 'xls'
        if head.startswith(b'\x89\x48\x44\x46\x0d\x0a\x1a\x0a'):
            return 'hdf5'
        try:
            pd.read_json(path)
            return 'json'
        except ValueError:
            pass
        try:
            # Warning: almost any file will match CSV
            next(read_csv(path, chunksize=1000))
            return 'csv'
        except pd.parser.CParserError:
            pass

    def _get_metadata_frame(self, data, top=3, preview=10, **kwargs):
        '''
        Compute the metadata for a Pandas DataFrame
        '''
        columns = []
        for col, series in data.iteritems():
            meta = OrderedDict(name=text_type(col))
            meta['type:pandas'] = series.dtype.name
            meta['missing'] = int(pd.isnull(series).sum())
            meta['nunique'] = series.nunique()
            meta['top'] = json.loads(series.value_counts().head(top).to_json())
            if pd.np.issubdtype(series.dtype, int) or pd.np.issubdtype(series.dtype, float):
                meta['moments'] = json.loads(series.dropna().describe().to_json())
            columns.append(meta)
        preview = min(preview, len(data))
        return OrderedDict([
            ('rows', len(data)),
            ('columns', columns),
            ('head', json.loads(data.head(preview).to_json(orient='split'))),
            ('sample', json.loads(data.sample(preview).sort_index().to_json(orient='split'))),
        ])

    def _unzip_files(self, path, format):
        '''
        Extract all files in the archive at path, using format specified.
        Yield (name, path) tuples for each file in the archive.

        For multi-file archives like 7z, ZIP, RAR, TAR, the name is the relative
        path to the archived file name. For single file archives like gz, bzip2,
        xz, the name is the extracted filename.

        In all cases, the path is the full path to the extracted file.
        '''
        if format in {'7z', 'zip', 'rar', 'tar'}:
            target = self._filename(path)
            # If the target is outdated, delete it.
            # Note: for ZIP inside ZIP, the first extraction creates a new file.
            # So the inner ZIP will ALWAYS be extracted. TODO: resolve this.
            if os.path.exists(target) and os.stat(target).st_mtime < os.stat(path).st_mtime:
                shutil.rmtree(target)
            # If the target does not exist, create it
            if not os.path.exists(target):
                os.makedirs(target)
                self._extract_archive(path, target=target, format=format)
            # Return each file
            for root, dirs, files in os.walk(target):
                for filename in files:
                    # Filename is path to the file relative to archive root
                    name = os.path.join(os.path.relpath(root, target), filename)
                    yield os.path.normpath(name), os.path.join(root, filename)
        elif format in {'xz', 'gz', 'bz2'}:
            # Strip out the extension from the filename. That's the new name of the dataset
            name = os.path.split(path)[-1]
            if name.endswith(format):
                name = name[:-len(format) - 1]
            target = os.path.join(self.path, name)
            # TODO: if target is fresh, do not re-extract
            if os.path.exists(target):
                os.unlink(target)
            self._extract_archive(path, target=self.path, format=format)
            yield name, target

    _format_map = {
        'gz': 'gzip',
        'bz2': 'bzip2',
    }

    def _extract_archive(self, archive, target, format=None):
        '''Extract archive file into target directory. Guess format form extension'''
        # Convert to relative paths with forward slashes (for Cygwin on Windows
        archive = os.path.relpath(archive).replace(os.path.sep, '/')
        target = os.path.relpath(target).replace(os.path.sep, '/')
        patoolib._extract_archive(
            archive, interactive=False, outdir=target, format=self._format_map.get(format, format))

    def _filename(self, url, path=None):
        '''Return unique filename from url (urlhash-name.ext) under self.path'''
        if path is None:
            path = urlparse(url).path
        name, ext = os.path.splitext(os.path.split(path)[-1])
        filename = '{urlhash}-{name}{ext}'.format(
            name=name, ext=ext, urlhash=md5(url.encode('utf-8')).hexdigest()[:10])
        return os.path.join(self.path, filename)


def fetch(url, path, expiry_days=1):
    '''
    Retrieves the HTTP or FTP url and saves it into path, unless path is newer than expiry_days.
    Returns the path
    '''
    now = time.time()
    if os.path.exists(path) and os.stat(path.st_mtime) > now - expiry_days * seconds_per_day:
        return path
    r = requests.get(url)
    if r.status_code == OK and len(r.content):
        with io.open(path, 'wb') as handle:
            handle.write(r.content)
    # TODO: in case of ANY failure, raise an exception
