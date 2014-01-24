import pandas as pd
import StringIO
import collections
import os

import urlparse
import urllib2
import boto
import cPickle as pickle

DATA_PICKLE_URL = os.environ.get('DATA_PICKLE_URL')

# unfortunately pandas code for reading pickle doesn't handle URLs (I should add an issue about that),
# so have to do some magic here

parsed_url = urlparse.urlparse(DATA_PICKLE_URL)
if parsed_url.scheme == 's3':
    connection = boto.connect_s3()
    bucket = connection.get_bucket(parsed_url.netloc)
    k = boto.s3.key.Key(bucket)
    k.key = parsed_url.path
    buffer = StringIO.StringIO(k.get_contents_as_string())
else:
    buffer = urllib2.urlopen(DATA_PICKLE_URL)

# "singleton" cached instance, for now
# eventually I expect to do proper database queries
FRAME = pickle.load(buffer)


def find_rows_matching_code_filters(source_rows, code_filters):
    rows = source_rows

    # group codes by filter type
    codes_per_filter_type = collections.defaultdict(list)
    for code_filter in code_filters:
        codes_per_filter_type[code_filter.filter_type].append(code_filter.code)

    # do an OR across all filters of a given filtertype, and then an AND across filter types
    for filter_type, codes in codes_per_filter_type.iteritems():
        column = filter_type + 'code'
        boolean_mask = pd.Series(False, index=rows.index)

        for code in codes:
            boolean_mask |= (rows[column] == code)

        # AND is implicit here... we reset rows to the result of the previous filtertype
        rows = rows[boolean_mask]

    return rows


def find_rows_matching_query_text(source_rows, text):
    # not exactly sure why the decode is necessary, but we get a UnicodeDecodeError otherwise
    row_filter = lambda x: isinstance(x, basestring) and text in x.decode('utf-8').lower()

    matching_projecttitle = FRAME.projecttitle.apply(row_filter)
    matching_shortdescription = FRAME.shortdescription.apply(row_filter)
    matching_longdescription = FRAME.longdescription.apply(row_filter)

    return source_rows[matching_projecttitle | matching_shortdescription | matching_longdescription]


def get_matching_rows_for_query(query):
    rows = FRAME

    # first, reduce data size as much as possible using filters
    rows = find_rows_matching_code_filters(rows, query.codefilter_set.all())

    # then do the somewhat expensive operation of text search
    rows = find_rows_matching_query_text(rows, query.text)

    # mark manual exclusions
    rows['excluded'] = False
    for manual_exclusion in query.manualexclusion_set.all():
        if manual_exclusion.pandas_row_id in rows.index:
            rows['excluded'][manual_exclusion.pandas_row_id] = True

    return rows


def get_matching_rows_for_combo(combo):
    results = [get_matching_rows_for_query(query) for query in combo.queries.all()]

    if not results:
        return pd.DataFrame()

    rows = pd.concat(results)

    # don't worry about excluded rows, we'll apply those manually,
    # but remove the column so as not to interfere with drop_duplicates
    del rows["excluded"]

    rows = rows.drop_duplicates()

    # if excluded in one query, row is excluded from the combination
    rows['excluded'] = False
    for query in combo.queries.all():
        for manual_exclusion in query.manualexclusion_set.all():
            if manual_exclusion.pandas_row_id in rows.index:
                rows['excluded'][manual_exclusion.pandas_row_id] = True

    return rows


def convert_to_csv_string_for_export(dataframe):
    """
    Get a string containing a CSV representation of the rows returned by a query, removing excluded rows.
    Maybe belongs more in the view code directly but nice to keep pandas code all in one file.
    """
    dataframe = dataframe[~dataframe.excluded]
    del dataframe['excluded']

    stringio = StringIO.StringIO()

    dataframe.to_csv(stringio, index=False)

    return stringio.getvalue()


def get_all_name_code_pairs(prefix):
    """
    Returns a dataframe containing ___code/___name pairs.
    This method makes a minor argument for separating this data out at the processing step.
    """
    code_column = prefix + 'code'
    name_column = prefix + 'name'

    rows = FRAME[[code_column, name_column]].drop_duplicates()

    # filter out missing codes (can happen for channel), and missing values will mean that pandas will have
    # interpreted code column as float (so that it can use NaN), need to set as int
    rows = rows.dropna()
    rows[code_column] = rows[code_column].astype(int)

    rows = rows.sort(name_column)

    return rows.rename(columns={code_column: 'code', name_column: 'name'})
