import pandas as pd
import StringIO
import collections
from django.db import connection

# TODO we need to order the columns properly for CSV export, perhaps use the list in build_crs_database.py?
# note that could be done either in the select or in pandas, afterwards, maybe the latter is cleaner
# or could even just be done in the view... we could make a CSV view. Maybe that's best,
BASE_SQL = 'SELECT crs.*, recipient.recipientname, donor.donorname, channel.channelname, ' \
           'sector.sectorname, purpose.purposename, agency.agencyname ' \
           'FROM crs ' \
           'INNER JOIN recipient ON (crs.recipientcode = recipient.recipientcode) ' \
           'INNER JOIN donor ON (crs.donorcode = donor.donorcode) ' \
           'INNER JOIN sector ON (crs.sectorcode = sector.sectorcode) ' \
           'INNER JOIN purpose ON (crs.purposecode = purpose.purposecode) ' \
           'INNER JOIN agency ON (crs.donorcode = agency.donorcode AND crs.agencycode = agency.agencycode) ' \
           'LEFT OUTER JOIN channel ON (crs.channelcode = channel.channelcode) '


class CodeFilterParams(object):
    def __init__(self, filter_type, code):
        self.filter_type = filter_type
        self.code = code


class QueryParams(object):
    def __init__(self, search_terms):
        self.search_terms = search_terms
        self.code_filters = []

    def add_code_filter(self, filter_type, code):
        self.code_filters.append(CodeFilterParams(filter_type, code))


def execute_query(query_text, code_filters):
    # we will want to revisit plainto_tsquery to provide fancier searching, but not at this phase
    where_clause = 'WHERE crs.searchable_text @@ plainto_tsquery(%s) '
    params = [query_text]

    # group codes by filter type
    codes_per_filter_type = collections.defaultdict(list)
    for code_filter in code_filters:
        codes_per_filter_type[code_filter.filter_type].append(code_filter.code)

    # do an OR across all filters of a given filtertype, and then an AND across filter types
    # TODO fancier logic to properly handle agencies
    # TODO fancier logic to handle null channels
    for filter_type, codes in codes_per_filter_type.iteritems():
        column = filter_type + '.' + filter_type + 'code'

        code_strings = [str(code) for code in codes]

        # could use sql params here but ints are pretty safe to just write in explicitly
        where_clause += ' AND ' + column + ' IN (' + ','.join(code_strings) + ') '

    limit_clause = 'ORDER BY crs.crs_pk LIMIT 25;'

    return pd.read_sql(BASE_SQL + where_clause + limit_clause, connection, index_col="crs_pk", params=params)


def get_matching_rows_for_query_new(query_params):
    return execute_query(query_params.search_terms, query_params.code_filters)


def get_matching_rows_for_query(query):
    rows = execute_query(query.text, query.codefilter_set.all())

    # manual exclusions are now broken, but they are going away anyhow

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

    dataframe.to_csv(stringio, index=False, encoding='utf-8')

    return stringio.getvalue()


def get_all_name_code_pairs(filtertype):
    """
    Returns a dataframe containing ___code/___name pairs.
    """
    rows = pd.read_sql('SELECT * FROM ' + filtertype + ';', connection)

    code_column = filtertype + 'code'
    name_column = filtertype + 'name'
    rows = rows.sort(name_column)

    return rows.rename(columns={code_column: 'code', name_column: 'name'})
