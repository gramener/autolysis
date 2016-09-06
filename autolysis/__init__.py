'''
Autolyse - Automated analysis library.
'''
import os
import json
import datetime
import dateutil
import itertools
import numpy as np
import blaze as bz
import pandas as pd
from io import open
from scipy.stats.mstats import ttest_ind
from scipy.stats import chi2_contingency
from .meta import metadata
__folder__ = os.path.split(os.path.abspath(__file__))[0]

# Load autolysis.__version__ from release.json
with open(os.path.join(__folder__, 'release.json'), encoding='utf-8') as _release_file:
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
               min_size=None):
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
    min_size : each group should contain at least min_size values.
        If min_size=None, automatically set the minimum size to
        1% of the dataset, or 10, whichever is larger.
    '''

    if min_size is None:
        # compute nrows, bz.compute(data.nrows) doesn't work for sqlite
        min_size = max(bz.into(int, data.nrows) / 100, 10)

    # compute mean of each number column
    means = {col: bz.into(float, data[col].mean()) for col in numbers}
    # pre-create aggregation expressions (mean, count)
    agg = {number: bz.mean(data[number]) for number in numbers}
    for group in groups:
        agg['#'] = data[group].count()
        ave = bz.by(data[group], **agg).sort('#', ascending=False)
        ave = bz.into(pd.DataFrame, ave)
        ave.index = ave[group]
        sizes = ave['#']
        # Each group should contain at least min_size values
        biggies = sizes[sizes >= min_size].index
        # ... and at least 2 groups overall, to compare.
        if len(biggies) < 2:
            continue
        for number in numbers:
            if number == group:
                continue
            sorted_cats = ave[number][biggies].dropna().sort_values()
            if len(sorted_cats) < 2:
                continue
            sohi = sorted_cats.index[-1]
            solo = sorted_cats.index[0]

            # If sorted_cats.index items are of numpy type, then
            # convert them to native type, skip conversion for unicode, str
            # See https://github.com/blaze/blaze/issues/1461
            if isinstance(solo, np.generic):
                solo, sohi = solo.item(), sohi.item()

            lo = bz.into(list, data[number][data[group] == solo])
            hi = bz.into(list, data[number][data[group] == sohi])

            _, prob = ttest_ind(
                np.ma.masked_array(lo, np.isnan(lo)),
                np.ma.masked_array(hi, np.isnan(hi))
            )
            if prob > cutoff:
                continue

            yield ({
                'group': group,
                'number': number,
                'prob': float(prob),
                'gain': sorted_cats.iloc[-1] / means[number] - 1,
                'biggies': ave.ix[biggies][number].to_dict(),
                'means': ave[[number, '#']].sort_values(by=number).reset_index().to_dict(
                    orient='records'),
            })


def _crosstab(index, column, values=None, correction=False):
    '''
    Computes a crosstab of two series (an ``index`` and a ``column``), and
    returns its observed cross-tabulation, expected cross-tabulation, and
    statistical parameters.

    Parameters
    ----------
    index : array-like, Series
        Labels for the index
    column : array-like, Series
        Labels for the column
    values : array-like, Series
        Optional weights to aggregate by summing up
    correction : boolean
        If True, and the degrees of freedom is 1, apply Yates' correction for
        continuity. The effect of the correction is to adjust each observed
        value by 0.5 towards the corresponding expected value. Defaults to False
        since Cramer's V (a more useful metric than chi-squared) must be computed
        without this correction.

    Returns
    -------
    observed : Pandas DataFrame
        Actual observed frequencies, i.e. count of the observations
    expected : Pandas DataFrame
        Expected frequencies if index and column are independent
    p : float
        Significance of the crosstab. Low values indicate index and column and
        dependent. Ranges from 0 (perfectly dependent) to 1 (independent)
    chi2 : float
        chi-squared value. High values indicate index and column are dependent.
    dof : int
        Degrees of freedom
    V : float
        `Cramer's V <http://en.wikipedia.org/wiki/Cram%C3%A9r%27s_V>`_ ranges from
        0 to 1. 0 indicates independent columns. 0.5 indicates strong relation.
        1 indicates a perfect relation.

    Examples
    --------
    Let's check, for example, whether language and city are independent. This
    is, do people in a specific city prefer a certain language, or vice versa::

        data = pd.DataFrame({
            'city':  ['London', 'Shanghai', 'London', 'Shanghai'],
            'language': ['English', 'English', 'Mandarin', 'Mandarin'],
            'people': [90, 50, 40, 350],
        })

        result = autolyse._crosstab(
            index=data['city'],
            column=data['language'],
            values=data['people'])

    This provides an actual observation::

        In [5]: result['observed']
        Out[5]:
        language  English  Mandarin
        city
        London         90        40
        Shanghai       50       350

    However, if the ``city`` and ``language`` were independent, we would expect::

        In [6]: result['expected']
        Out[6]:
        language     English    Mandarin
        city
        London     34.339623   95.660377
        Shanghai  105.660377  294.339623

    `Cramer's V <http://en.wikipedia.org/wiki/Cram%C3%A9r%27s_V>`_
    tells us whether this difference is significant enough to say that
    ``language`` and ``city`` are independent::

        In [7]: result['V']
        Out[7]: 0.54866228139712236

    Any value above 0.5 is a strong indication of a relationship. This says
    that ``language`` and ``city`` are dependent.
    '''
    observed = pd.crosstab(index, column, values, aggfunc='sum').fillna(0)
    chi2, p, dof, expected_values = chi2_contingency(observed.values,
                                                     correction=correction)
    expected = pd.DataFrame(expected_values,
                            index=observed.index,
                            columns=observed.columns)
    n = pd.np.sum(observed.values)
    nindex, ncolumns = len(observed.index), len(observed.columns)
    return {
        'observed': observed,
        'expected': expected,
        'chi2': chi2,
        'p': p,
        'dof': dof,
        'V': (chi2 / n / (min(nindex, ncolumns) - 1)) ** 0.5,
    }


def crosstabs(data, columns=None, values=None,
              correction=False,
              pairs_top=10000,
              details=False):
    '''
    Identifies the strength of relationship between every pair of categorical
    columns in a DataFrame

    Parameters
    ----------
    data : Blaze data
        A data with at least 2 columns having categorical values.
    columns : list of column names in data
        If not specified, uses ``autolyse.types(data)['groups']`` to identify
        all columns with categorical data.
    values : str, column name
        Optional column that contains weights to aggregate by summing up. By
        default, each row is counted as an observation.
    correction : boolean
        If True, and the degrees of freedom is 1, apply Yates' correction for
        continuity. The effect of the correction is to adjust each observed
        value by 0.5 towards the corresponding expected value. Defaults to False
        since Cramer's V (a more useful metric than chi-squared) must be computed
        without this correction.
    pairs_top: integer, Pick only top 10000 pairs by default
    details: boolean
        If True, will return observed and expected dataframes for pairs.
        Defaults to False.
    '''
    if columns is None:
        columns = types(data)['groups']

    parameters = ('p', 'chi2', 'dof', 'V')
    for index, column in itertools.combinations(columns, 2):
        agg_col = values if values in data.fields else column
        agg_func = bz.count(data[agg_col]) if agg_col == column else bz.sum(data[agg_col])
        data_grouped = bz.into(pd.DataFrame,
                               bz.by(bz.merge(data[index], data[column]),
                                     values=agg_func)
                               .sort('values')  # Generated SQL inefficient
                               .head(pairs_top))
        # BUG: bz.count: non-null count, gives 0 count for NULL groups
        # .nrows needs to fixed blaze/issues/1484
        # For now, we'll ignore NULL groups
        # Remove NULL groups
        data_grouped = data_grouped.dropna()
        if data_grouped.empty:
            result = {(index, column): {}}
        else:
            r = _crosstab(data_grouped[index],
                          column=data_grouped[column],
                          values=data_grouped['values'],
                          correction=correction)
            if details:
                result = {
                    (index, column): {
                        'observed': r['observed'],
                        'expected': r['expected'],
                        'stats': {param: r[param] for param in parameters}
                    }
                }
            else:
                result = {
                    (index, column): {
                        'stats': {param: r[param] for param in parameters}
                    }
                }

        yield result


__all__ = [
    'is_date',
    'has_keywords',
    'types',
    'groupmeans',
    'crosstabs',
    'metadata',
]
