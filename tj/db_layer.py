import pandas as pd
import StringIO
import collections
import re
import django.db


BASE_SQL = 'SELECT crs.*, recipient.recipientname, donor.donorname, channel.channelname, ' \
           'sector.sectorname, purpose.purposename, agency.agencyname, ' \
           'region.regionname, incomegroup.incomegroupname, flow.flowname, ' \
           'tj_inclusion.tj_inclusion_name, tj_category.tj_category_name ' \
           'FROM crs ' \
           'INNER JOIN recipient ON (crs.recipientcode = recipient.recipientcode) ' \
           'INNER JOIN donor ON (crs.donorcode = donor.donorcode) ' \
           'INNER JOIN sector ON (crs.sectorcode = sector.sectorcode) ' \
           'INNER JOIN purpose ON (crs.purposecode = purpose.purposecode) ' \
           'INNER JOIN region ON (crs.regioncode = region.regioncode) ' \
           'INNER JOIN incomegroup ON (crs.incomegroupcode = incomegroup.incomegroupcode) ' \
           'INNER JOIN flow ON (crs.flowcode = flow.flowcode) ' \
           'INNER JOIN agency ON (crs.donorcode = agency.donorcode AND crs.agencycode = agency.agencycode) ' \
           'LEFT OUTER JOIN channel ON (crs.channelcode = channel.channelcode) ' \
           'LEFT OUTER JOIN tj_inclusion ON (crs.tj_inclusion_id = tj_inclusion.tj_inclusion_id) ' \
           'LEFT OUTER JOIN tj_category ON (crs.tj_category_id = tj_category.tj_category_id) '

ROW_LIMIT = 25
NO_ROW_LIMIT = 10000000

CSV_COLUMNS = [
    'tj_inclusion_name',
    'tj_category_name',
    'year',
    'donorcode',
    'donorname',
    'agencycode',
    'agencyname',
    'crsid',
    'projectnumber',
    'initialreport',
    'recipientcode',
    'recipientname',
    'regioncode',
    'regionname',
    'incomegroupcode',
    'incomegroupname',
    'flowcode',
    'flowname',
    'bi_multi',
    'category',
    'finance_t',
    'aid_t',
    'usd_commitment',
    'usd_disbursement',
    'usd_received',
    'usd_commitment_defl',
    'usd_disbursement_defl',
    'usd_received_defl',
    'usd_adjustment',
    'usd_adjustment_defl',
    'usd_amountuntied',
    'usd_amountpartialtied',
    'usd_amounttied',
    'usd_amountuntied_defl',
    'usd_amountpartialtied_defl',
    'usd_amounttied_defl',
    'usd_irtc',
    'usd_expert_commitment',
    'usd_expert_extended',
    'usd_export_credit',
    'currencycode',
    'commitment_national',
    'disbursement_national',
    'shortdescription',
    'projecttitle',
    'purposecode',
    'purposename',
    'sectorcode',
    'sectorname',
    'channelcode',
    'channelname',
    'channelreportedname',
    'geography',
    'expectedstartdate',
    'completiondate',
    'longdescription',
    'gender',
    'trade',
    'ftc',
    'pba',
    ]


class QueryParams(object):
    def __init__(self, search_terms):
        self.search_terms = search_terms
        self.codefilter_type_to_codes = collections.defaultdict(list)
        self.customfilter_type_to_codes = collections.defaultdict(list)
        self.yearfilters = []

    def add_code_filter(self, filter_type, code):
        self.codefilter_type_to_codes[filter_type].append(code)

    def add_custom_column_filter(self, filter_type, code):
        self.customfilter_type_to_codes[filter_type].append(code)

    def add_year_filter(self, year):
        self.yearfilters.append(year)


def get_db_connection():
    django.db.close_old_connections()  # otherwise it seems our connections are going stale?

    return django.db.connection


def convert_to_tsquery(search_terms):
    # trim whitespace on either side
    ts_query = search_terms.strip()

    # internal whitespace becomes "AND"
    whitespace_pattern = re.compile(' +')
    ts_query = whitespace_pattern.sub(' & ', ts_query)

    # convert any leading minus signs into "NOT"
    minus_pattern = re.compile('(^| )-')  # this means something quite different from the pattern above!
    ts_query = minus_pattern.sub(' !', ts_query)

    # convert any trailing asterisks into :*
    trailing_asterisk_pattern = re.compile('(\w)\*')
    ts_query = trailing_asterisk_pattern.sub(r'\1:*', ts_query)

    return ts_query


def generate_where_clause_and_params(query_params):
    where_clause = 'WHERE 1=1 '  # throw-away starter clause so that ANDing works
    params = []

    if query_params.search_terms:
        where_clause += ' AND crs.searchable_text @@ to_tsquery(%s) '
        params += [convert_to_tsquery(query_params.search_terms)]

    # do an OR across all filters of a given filtertype, and then an AND across filter types
    # TODO fancier logic to properly handle agencies
    # TODO fancier logic to handle null channels
    for filter_type, codes in query_params.codefilter_type_to_codes.iteritems():
        column = 'crs.' + filter_type + 'code'

        code_strings = [str(code) for code in codes]

        # could use sql params here but if we make the assumption that these are ints we should be okay
        # (for a commercial website we'd want to have sanitized these)
        where_clause += ' AND ' + column + ' IN (' + ','.join(code_strings) + ') '

    for filter_type, codes in query_params.customfilter_type_to_codes.iteritems():
        column = 'crs.tj_' + filter_type + '_id'

        code_strings = [str(code) for code in codes]

        # could use sql params here but ints are pretty safe to just write in explicitly
        where_clause += ' AND ' + column + ' IN (' + ','.join(code_strings) + ') '

    if query_params.yearfilters:
        where_clause += ' AND crs.year IN (' + ','.join(query_params.yearfilters) + ') '

    return where_clause, params


def generate_where_clause_and_params_for_unanalyzed_data(query_params):
    where_clause, params = generate_where_clause_and_params(query_params)

    # only include results for which we haven't made an inclusion decision
    where_clause += ' AND crs.tj_inclusion_id IS NULL '

    return where_clause, params


def get_matching_rows_for_query(query_params):
    where_clause, params = generate_where_clause_and_params_for_unanalyzed_data(query_params)

    # could possibly accomplish this by only reading however many rows from the cursor?
    # that's likely slower though, and then integrating with pandas might be a pain
    limit_clause = 'ORDER BY crs.crs_pk LIMIT {row_limit};'.format(row_limit=ROW_LIMIT)

    return pd.read_sql(BASE_SQL + where_clause + limit_clause, get_db_connection(), index_col="crs_pk", params=params)


def get_count_of_matching_rows_for_query(query_params):
    where_clause, params = generate_where_clause_and_params_for_unanalyzed_data(query_params)

    count_sql = 'SELECT count(*) FROM crs ' + where_clause

    cursor = get_db_connection().cursor()
    cursor.execute(count_sql, params)
    rowcount = int(cursor.fetchone()[0])
    cursor.close()

    return rowcount


def get_rows_for_analyzed_data(query_params, additional_where_condition):
    where_clause, params = generate_where_clause_and_params(query_params)
    where_clause += ' AND ' + additional_where_condition

    order_clause = ' ORDER BY crs.crs_pk;'

    return pd.read_sql(BASE_SQL + where_clause + order_clause, get_db_connection(), index_col="crs_pk", params=params)


def get_tj_dataset_rows(query_params=QueryParams(None)):
    return get_rows_for_analyzed_data(query_params, '(crs.tj_inclusion_id > 0)')


def get_included_but_uncategorized_rows(query_params):
    return get_rows_for_analyzed_data(query_params, '((crs.tj_inclusion_id > 0) AND (crs.tj_category_id = 0))')


def get_categorized_but_no_inclusion_decision_rows(query_params):
    return get_rows_for_analyzed_data(query_params, '((crs.tj_inclusion_id IS NULL) AND (crs.tj_category_id != 0))')


def get_excluded_rows(query_params):
    return get_rows_for_analyzed_data(query_params, '(crs.tj_inclusion_id = 0)')


def convert_to_csv_string_for_export(dataframe):
    """
    Get a string containing a CSV representation of pandas rows.
    Maybe belongs more in the view code directly but nice to keep pandas code all in one file.
    """
    dataframe = dataframe[CSV_COLUMNS]

    stringio = StringIO.StringIO()

    dataframe.to_csv(stringio, index=False, encoding='utf-8')

    return stringio.getvalue()


def get_all_rows_from_table(tablename):
    return pd.read_sql('SELECT * FROM ' + tablename + ';', get_db_connection())


def standardize_columns_for_filter(rows, code_column, name_column):
    return rows.rename(columns={code_column: 'code', name_column: 'name'})
    

def get_all_name_code_pairs(filtertype):
    """
    Returns a dataframe containing ___code/___name pairs.
    """
    rows = get_all_rows_from_table(filtertype)

    code_column = filtertype + 'code'
    name_column = filtertype + 'name'
    rows = rows.sort(name_column)

    return standardize_columns_for_filter(rows, code_column, name_column)


def get_all_inclusion_rows(as_filter=False):
    """
    Returns a dataframe of the tj_inclusion table.
    """
    rows = get_all_rows_from_table('tj_inclusion')
    
    if as_filter:
        return standardize_columns_for_filter(rows, code_column='tj_inclusion_id', name_column='tj_inclusion_name')
    else:
        return rows


def get_all_category_rows(as_filter=False):
    """
    Returns a dataframe of the tj_category table.
    """
    rows = get_all_rows_from_table('tj_category')

    if as_filter:
        return standardize_columns_for_filter(rows, code_column='tj_category_id', name_column='tj_category_name')
    else:
        return rows


def get_years_as_filter_rows():
    """
    This could be a db query, but we'll just fake it for now
    """
    years = range(2000, 2014)
    return pd.DataFrame({'code': years, 'name': years})


def update_inclusions(inclusion_actions):
    sql = 'UPDATE crs SET tj_inclusion_id=%(tj_inclusion_id)s WHERE crs_pk=%(crs_pk)s;'

    connection = get_db_connection()
    cursor = connection.cursor()

    for crs_pk, tj_inclusion_id in inclusion_actions.iteritems():
        cursor.execute(sql, {'tj_inclusion_id': tj_inclusion_id, 'crs_pk': crs_pk})

    connection.commit()
    cursor.close()


def update_categories(category_actions):
    sql = 'UPDATE crs SET tj_category_id=%(tj_category_id)s WHERE crs_pk=%(crs_pk)s;'

    connection = get_db_connection()
    cursor = connection.cursor()

    for crs_pk, tj_category_id in category_actions.iteritems():
        cursor.execute(sql, {'tj_category_id': tj_category_id, 'crs_pk': crs_pk})

    connection.commit()
    cursor.close()
