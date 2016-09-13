import os
import sys
import pandas as pd


if __name__ == '__main__':
    if len(sys.argv) == 0:
        print('Usage: make_dataset.py [o.xlsx|o.db|o.h5|o.dta] a.csv b.csv ...')    # noqa
        sys.exit(0)

    outfile = sys.argv[1]
    ext = os.path.splitext(outfile)[1].lower()

    datasets = []
    for path in sys.argv[2:]:
        # HDF5 does not work well with unicode. So don't encode for it
        data = pd.read_csv(path, encoding=None if ext in {'.h5', '.hdf5'} else 'cp1252')
        datasets.append((os.path.splitext(os.path.split(path)[-1])[0], data))

    if ext not in {'.xlsx', '.db', '.sqlite3', '.h5', '.hdf5', '.dta'}:
        raise NotImplementedError('Unsupported extension %s' % ext)

    if os.path.exists(outfile):
        os.unlink(outfile)

    if ext in {'.xls', '.xlsx'}:
        with pd.ExcelWriter(outfile) as writer:
            for key, df in datasets:
                df.to_excel(writer, sheet_name=key, encoding='cp1252')

    elif ext in {'.db', '.sqlite3'}:
        import sqlalchemy as sa
        engine = sa.create_engine('sqlite:///' + outfile, encoding='utf-8')
        for key, df in datasets:
            df.to_sql(key, engine)

    elif ext in {'.h5', '.hdf5'}:
        store = pd.HDFStore(outfile)
        for key, df in datasets:
            store.put(key, df, format='fixed', encoding='utf-8')

    elif ext in {'.dta'}:
        # Files are overwritten. The last file written will survive
        for key, df in datasets:
            df.to_stata(outfile)
