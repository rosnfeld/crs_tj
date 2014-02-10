"""
For building PostgreSQL database tables to house CRS data for the web app.
"""

import psycopg2
import os
import pandas as pd
import StringIO

# somewhat different than what is in models.py
# 'agency' is omitted here as it's more complicated
CODE_TABLES = ['donor', 'recipient', 'region', 'incomegroup', 'flow', 'purpose', 'sector', 'channel']

# list of (column name, postgres type) tuples for the columns in the CRS data
# note that postgres will remove any capitalization, e.g. Year -> year
CRS_COLUMN_SPEC = [
    ('Year', 'integer'),
    ('donorcode', 'integer REFERENCES donor'),
    ('agencycode', 'integer'),
    ('crsid', 'varchar (63)'),
    ('projectnumber', 'varchar (63)'),
    ('initialreport', 'integer'),
    ('recipientcode', 'integer REFERENCES recipient'),
    ('regioncode', 'integer REFERENCES region'),
    ('incomegroupcode', 'integer REFERENCES incomegroup'),
    ('flowcode', 'integer REFERENCES flow'),
    ('bi_multi', 'integer'),
    ('category', 'integer'),
    ('finance_t', 'integer'),
    ('aid_t', 'varchar (63)'),
    ('usd_commitment', 'double precision'),
    ('usd_disbursement', 'double precision'),
    ('usd_received', 'double precision'),
    ('usd_commitment_defl', 'double precision'),
    ('usd_disbursement_defl', 'double precision'),
    ('usd_received_defl', 'double precision'),
    ('usd_adjustment', 'double precision'),
    ('usd_adjustment_defl', 'double precision'),
    ('usd_amountuntied', 'double precision'),
    ('usd_amountpartialtied', 'double precision'),
    ('usd_amounttied', 'double precision'),
    ('usd_amountuntied_defl', 'double precision'),
    ('usd_amountpartialtied_defl', 'double precision'),
    ('usd_amounttied_defl', 'double precision'),
    ('usd_IRTC', 'double precision'),
    ('usd_expert_commitment', 'double precision'),
    ('usd_expert_extended', 'double precision'),
    ('usd_export_credit', 'double precision'),
    ('currencycode', 'integer'),
    ('commitment_national', 'double precision'),
    ('disbursement_national', 'double precision'),
    ('shortdescription', 'text'),
    ('projecttitle', 'text'),
    ('purposecode', 'integer REFERENCES purpose'),
    ('sectorcode', 'integer REFERENCES sector'),
    # should be 'integer REFERENCES channel', but pandas data has channelcode as float to handle nulls
    ('channelcode', 'double precision'),
    ('channelreportedname', 'text'),
    ('geography', 'text'),
    ('expectedstartdate', 'varchar (63)'),
    ('completiondate', 'varchar (63)'),
    ('longdescription', 'text'),
    ('gender', 'double precision'),
    ('trade', 'double precision'),
    ('FTC', 'double precision'),
    ('PBA', 'double precision')
]

# custom columns we want to add to the data
CATEGORY_COLUMN_NAME = 'tj_category_id'
INCLUSION_COLUMN_NAME = 'tj_inclusion_id'


def get_db_connection(host, database, user, password):
    return psycopg2.connect(host=host, database=database, user=user, password=password)


# duplicated with query_processor.py
def get_all_name_code_pairs(dataframe, filter_type):
    """
    Returns a dataframe containing ___code/___name pairs.
    """
    code_column = filter_type + 'code'
    name_column = filter_type + 'name'

    rows = dataframe[[code_column, name_column]].drop_duplicates()

    # filter out missing codes (can happen for channel), and missing values will mean that pandas will have
    # interpreted code column as float (so that it can use NaN), need to set as int
    rows = rows.dropna()
    rows[code_column] = rows[code_column].astype(int)

    rows = rows.sort(name_column)

    if filter_type == 'purpose':
        # hack around an unfortunate issue in CRS data
        # There are two names assigned to purposecode 11420, so we have to remove one of them
        # It shouldn't affect this analysis since these aren't TJ-related
        rows = rows[rows.purposename != 'Imputed student costs']

    return rows.rename(columns={code_column: 'code', name_column: 'name'})


def build_code_tables(cursor, dataframe):
    create_template = 'CREATE TABLE {table_name} ({code_column} integer primary key, {name_column} varchar(127));'
    insert_template = "INSERT INTO {table_name} VALUES (%(code)s, %(name)s);"
    index_template = 'CREATE UNIQUE INDEX ON {table_name} ({code_column});'

    for filter_type in CODE_TABLES:
        table_name = filter_type
        rows = get_all_name_code_pairs(dataframe, filter_type)

        # these are the database table names, not the pandas dataframe column names
        code_column = filter_type + 'code'
        name_column = filter_type + 'name'

        name_map = {'table_name': table_name, 'code_column': code_column, 'name_column': name_column}

        # create table, populate it, and add an index
        create_sql = create_template.format(**name_map)
        cursor.execute(create_sql)

        insert_sql = insert_template.format(**name_map)
        for i, row in rows.iterrows():
            cursor.execute(insert_sql, {'code': row['code'], 'name': row['name']})

        index_sql = index_template.format(**name_map)
        cursor.execute(index_sql)


def build_agency_table(cursor, dataframe):
    """
    Agencies are special in that they have the compound key (donorcode, agencycode)
    """
    rows = dataframe[['donorcode', 'agencycode', 'agencyname']].drop_duplicates()

    create_sql = 'CREATE TABLE agency (donorcode integer, agencycode integer, agencyname varchar(127), ' \
                 'PRIMARY KEY(donorcode, agencycode));'
    cursor.execute(create_sql)

    insert_sql = 'INSERT INTO agency VALUES (%(donorcode)s, %(agencycode)s, %(agencyname)s);'
    for i, row in rows.iterrows():
        cursor.execute(insert_sql, {'donorcode': row['donorcode'], 'agencycode': row['agencycode'],
                                    'agencyname': row['agencyname']})

    index_sql = 'CREATE UNIQUE INDEX ON agency(donorcode, agencycode);'
    cursor.execute(index_sql)


def build_custom_data_tables(cursor):
    # yes, these are just code-tables that we don't expect to change...
    # they are hard-coded here, could be hardcoded in the web app,
    # but this makes it nicer if people ever want to query the db directly
    # or build another app on top of this data
    inclusion_create_sql = "CREATE TABLE tj_inclusion (" + INCLUSION_COLUMN_NAME +\
                           " smallint PRIMARY KEY, tj_inclusion_name varchar(31));"
    cursor.execute(inclusion_create_sql)

    inclusion_insert_sql = "INSERT INTO tj_inclusion VALUES (%(tj_inclusion_id)s, %(tj_inclusion_name)s);"
    cursor.execute(inclusion_insert_sql, {'tj_inclusion_id': 0, 'tj_inclusion_name': 'Exclude'})
    cursor.execute(inclusion_insert_sql, {'tj_inclusion_id': 1, 'tj_inclusion_name': 'Include'})
    cursor.execute(inclusion_insert_sql, {'tj_inclusion_id': 2, 'tj_inclusion_name': 'Maybe include'})
    # and if we ever come up with other tiers of inclusion, we can add them here

    inclusion_index_sql = "CREATE UNIQUE INDEX on tj_inclusion (" + INCLUSION_COLUMN_NAME + ");"
    cursor.execute(inclusion_index_sql)

    category_create_sql = "CREATE TABLE tj_category (" + CATEGORY_COLUMN_NAME +\
                          " smallint PRIMARY KEY, tj_category_name varchar(31));"
    cursor.execute(category_create_sql)

    category_insert_sql = "INSERT INTO tj_category VALUES (%(tj_category_id)s, %(tj_category_name)s);"
    cursor.execute(category_insert_sql, {'tj_category_id': 1, 'tj_category_name': 'Truth and memory'})
    cursor.execute(category_insert_sql, {'tj_category_id': 2, 'tj_category_name': 'Criminal justice'})
    cursor.execute(category_insert_sql, {'tj_category_id': 3, 'tj_category_name': 'Reparations'})
    cursor.execute(category_insert_sql, {'tj_category_id': 4, 'tj_category_name': 'Institutional reform'})
    cursor.execute(category_insert_sql, {'tj_category_id': 5, 'tj_category_name': 'Reconciliation'})
    cursor.execute(category_insert_sql, {'tj_category_id': 6, 'tj_category_name': 'General TJ'})
    # and if we ever come up with other categories, we can add them here

    category_index_sql = "CREATE UNIQUE INDEX on tj_category (" + CATEGORY_COLUMN_NAME + ");"
    cursor.execute(category_index_sql)


def create_crs_table(cursor):
    sql = 'CREATE TABLE crs ('

    # auto-generated primary key
    sql += 'crs_pk serial PRIMARY KEY,'

    # custom columns
    sql += INCLUSION_COLUMN_NAME + ' smallint REFERENCES tj_inclusion,'
    sql += CATEGORY_COLUMN_NAME + ' smallint REFERENCES tj_category,'

    # desired columns from CRS file
    column_spec_list = [column_name + ' ' + column_type for column_name, column_type in CRS_COLUMN_SPEC]
    sql += ','.join(column_spec_list)

    sql += ');'
    cursor.execute(sql)


def populate_crs_table(cursor, dataframe):
    # write the dataframe to a buffer
    byte_buffer = StringIO.StringIO()
    columns_of_interest = [column_name for column_name, column_type in CRS_COLUMN_SPEC]
    dataframe[columns_of_interest].to_csv(byte_buffer, header=False, index=False)
    byte_buffer.seek(0)  # rewind to beginning of the buffer

    # bulk-load the table from the buffer
    copy_sql = "COPY crs({columns}) FROM STDIN WITH CSV".format(columns=",".join(columns_of_interest))
    cursor.copy_expert(copy_sql, byte_buffer)


def index_crs_table(cursor):
    # index code table columns in main crs table as we'll use them to filter queries
    code_index_template = 'CREATE INDEX {index_name} ON crs ({code_column});'

    for filter_type in CODE_TABLES:
        name_map = {'index_name': 'crs_' + filter_type + 'code_idx', 'code_column': filter_type + 'code'}
        index_sql = code_index_template.format(**name_map)
        cursor.execute(index_sql)

    # also index the custom columns as they will be important for filtering
    # note these indices are on the only non-static data (maybe need to be rebuilt if ever used heavily?)
    custom_index_sql_template = 'CREATE INDEX {index_name} ON crs ({custom_column});'
    for custom_column in (INCLUSION_COLUMN_NAME, CATEGORY_COLUMN_NAME):
        name_map = {'index_name': 'crs_' + custom_column + '_idx', 'custom_column': custom_column}
        custom_index_sql = custom_index_sql_template.format(**name_map)
        cursor.execute(custom_index_sql)


    # Add a special column to crs just for text search, then index it.

    alter_sql = 'ALTER TABLE crs ADD COLUMN searchable_text tsvector;'
    cursor.execute(alter_sql)

    # Unfortunately we have to pick a language here, and our text columns are in several different languages.
    # If we could figure out the language of each row (by donor, recipient, or text analysis?) and store it in
    # a column, then we could use to_tsvector(language_column, text_column).
    update_sql = "UPDATE crs SET searchable_text = to_tsvector('english'," \
                 "coalesce(projecttitle,'') || ' ' || " \
                 "coalesce(shortdescription,'') || ' ' || " \
                 "coalesce(longdescription,''));"
    cursor.execute(update_sql)

    text_index_sql = "CREATE INDEX crs_textsearch_idx ON crs USING gin(searchable_text);"
    cursor.execute(text_index_sql)


if __name__ == "__main__":
    host = os.environ['POSTGRES_HOST']
    database = os.environ['POSTGRES_DB']
    user = os.environ['POSTGRES_USER']
    password = os.environ['POSTGRES_PASSWORD']

    connection = get_db_connection(host, database, user, password)
    cursor = connection.cursor()

    # dataframe = pd.read_pickle('/home/andrew/oecd/crs/processed/2014-01-30/all_data.pkl')
    dataframe = pd.read_pickle('/home/andrew/oecd/crs/processed/2014-01-30/filtered.pkl')

    build_code_tables(cursor, dataframe)
    build_agency_table(cursor, dataframe)

    build_custom_data_tables(cursor)

    create_crs_table(cursor)
    populate_crs_table(cursor, dataframe)
    index_crs_table(cursor)

    connection.commit()
    cursor.close()
    connection.close()
