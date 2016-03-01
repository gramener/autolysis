'''
Autolyse - Automated analysis library.
'''
import os
import json
import datetime
import dateutil
import numpy as np
import blaze as bz
import pandas as pd

from scipy.stats.mstats import ttest_ind

__folder__ = os.path.split(os.path.abspath(__file__))[0]

# Load autolysis.__version__ from release.json
with open(os.path.join(__folder__, 'release.json')) as _release_file:
    release = json.load(_release_file)
    __version__ = release['version']


def is_date(series):
    '''
    Returns ``True`` if the first 1000 non-null values in a ``series`` are
    parseable as dates

    Parameters
    ----------
    series : Pandas Series

    Examples
    --------
    Usage::

        is_date(pd.Series(['Jul 31, 2009', '2010-01-10', None]))
        # True

        is_date(pd.Series(['Jul 31, 2009', '2010-101-10', None]))
        # False

        is_date(pd.Series(pd.date_range('1/1/2011', periods=72, freq='H')))
        # True
    '''
    series = series.dropna()[:1000]
    if len(series) == 0:
        return False
    if series.apply(lambda v: issubclass(type(v), datetime.datetime)).all():
        return True
    try:
        series.apply(dateutil.parser.parse)
    except (ValueError,      # Values that cannot be converted into dates
            TypeError,       # Values that cannot be converted into dates
            AttributeError,  # Long ints do not have a .read attribute
            OverflowError):  # Long ints like mobile numbers raise this
        return False
    return True


def has_keywords(series, sep=' ', thresh=2):
    '''
    Returns ``True`` if any of the first 1000 non-null values in a string
    ``series`` are strings that have more than ``thresh`` =2 separators
    (space, by default) in them

    Parameters
    ----------
    series : pd.Series
        Must be a string series. ``series.str.count()`` should be valid.
    sep : str
        Separator within the words. Defaults to ``' '`` space.
    thresh : int
        Threshold number of times a separator should occur in the word.
        Defaults to 2.

    Examples
    --------
    Usage::

        series = pd.Series(['Curd ', 'GOOG APPL MS', 'A B C', 'T Test is'])
        has_keywords(series)
        # False
        has_keywords(series, thresh=1)
        # True
    '''
    return (series.dropna()[:1000].str.count(sep) > thresh).any()


def get_numeric_cols(dshape):
    shape = dshape.parameters[-1].dict
    cols = []
    for k in shape:
        type = str(shape[k]).replace('?', '')
        if type.startswith("int") or type.startswith("float"):
            cols.append(k)
    return cols


def types(data):
    '''
    Returns the column names in groups for the given DataFrame

    Parameters
    ----------
    data : Blaze DataFrame

    Returns
    -------
    dict : dictionary of data-types
        | groups : categorical variables that you can group by
        | dates : date parseable columns (subset of groups)
        | numbers : numerical variables that you can average
        | keywords : strings with at least two spaces

    Examples
    --------
    Consider this DataFrame::

            A   B     C           D
        0   1   2   A B C D    Jul 31, 20
        1   2   3   World is   2010-11-10

    Running ``types(data)`` returns::

        {'dates': ['D'],
         'groups': ['C', 'D'],
         'keywords': ['C'],
         'numbers': ['A', 'B']}
    '''
    typ = {}
    typ['numbers'] = get_numeric_cols(data.dshape)
    typ['groups'] = list(set(data.fields) - set(typ['numbers']))
    typ['dates'] = [group for group in typ['groups']
                    if str(data[group].dshape[-1]) == '?datetime' or
                    is_date(bz.into(pd.Series, data[group].head(1000)))]
    typ['keywords'] = [group for group in typ['groups']
                       if str(data[group].dshape[-1]) == '?string' and
                       has_keywords(bz.into(
                                    pd.Series, data[group].head(1000)))]
    return typ


def groupmeans(data, groups, numbers,
               cutoff=.01,
               quantile=.95,
               minsize=None):
    '''
    Yields the significant differences in average between every pair of
    groups and numbers.

    Parameters
    ----------
    data : blaze data object
    groups : non-empty iterable containing category column names in data
    numbers : non-empty iterable containing numeric column names in data
    cutoff : ignore anything with prob > cutoff.
        cutoff=None ignores significance checks, speeding it up a LOT.
    quantile : number that represents target improvement. Defaults to .95.
        The ``diff`` returned is the % impact of everyone moving to the 95th
        percentile
    minsize : each group should contain at least minsize values.
        If minsize=None, automatically set the minimum size to
        1% of the dataset, or 10, whichever is larger.
    '''

    if minsize is None:
        # compute nrows, bz.compute(data.nrows) doesn't work for sqlite
        minsize = max(bz.into(int, data.nrows) / 100, 10)

    # compute mean of each number column
    means = {col: bz.into(float, data[col].mean()) for col in numbers}
    results = []
    for group in groups:
        agg = {number: bz.mean(data[number]) for number in numbers}
        agg['#'] = bz.count(data)
        ave = bz.by(data[group], **agg).sort('#', ascending=False)
        ave = bz.into(pd.DataFrame, ave)
        ave.index = ave[group]
        sizes = ave['#']
        # Each group should contain at least minsize values
        biggies = sizes[sizes >= minsize].index
        # ... and at least 2 groups overall, to compare.
        if len(biggies) < 2:
            continue
        for number in numbers:
            if number == group:
                continue
            sorted_cats = ave[number][biggies].dropna().sort_values()
            if len(sorted_cats) < 2:
                continue
            lo = bz.into(list,
                         data[number][data[group] == sorted_cats.index[0]])
            hi = bz.into(list,
                         data[number][data[group] == sorted_cats.index[-1]])
            _, prob = ttest_ind(
                np.ma.masked_array(lo, np.isnan(lo)),
                np.ma.masked_array(hi, np.isnan(hi))
            )
            if prob > cutoff:
                continue
            results.append({
                'group': group,
                'number': number,
                'prob': prob,
                'gain': sorted_cats.iloc[-1] / means[number] - 1,
                'biggies': ave.ix[biggies][number],
                'means': ave[[number, '#']].sort_values(by=number),
            })

    results = pd.DataFrame(results)
    if len(results) > 0:
        results = results.set_index(['group', 'number'])
    return results
