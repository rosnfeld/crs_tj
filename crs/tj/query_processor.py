import pandas as pd
import StringIO

# HACK just to get this up and running
DATA_FILE = "/home/andrew/oecd/crs/processed/2014-01-05/filtered.pkl"

# "singleton" cached instance, for now
# eventually I expect to do proper PyTables or database queries
FRAME = pd.read_pickle(DATA_FILE)


def find_rows_matching_query_text(text):
    # not exactly sure why the decode is necessary, but we get a UnicodeDecodeError otherwise
    row_filter = lambda x: isinstance(x, basestring) and text in x.decode('utf-8').lower()

    matching_projecttitle = FRAME.projecttitle.apply(row_filter)
    matching_shortdescription = FRAME.shortdescription.apply(row_filter)
    matching_longdescription = FRAME.longdescription.apply(row_filter)

    return FRAME[matching_projecttitle | matching_shortdescription | matching_longdescription]


def get_matching_rows_for_query(query):
    # eventually process other query components beyond just text
    rows = find_rows_matching_query_text(query.text)
    rows['excluded'] = False
    for manual_exclusion in query.manualexclusion_set.all():
        rows['excluded'][manual_exclusion.pandas_row_id] = True

    return rows


def get_matching_rows_for_combo(combo):
    results = [get_matching_rows_for_query(query) for query in combo.queries.all()]

    rows = pd.concat(results)

    # don't worry about excluded rows, we'll apply those manually,
    # but remove the column so as not to interfere with drop_duplicates
    del rows["excluded"]

    rows = rows.drop_duplicates()

    # if excluded in one query, row is excluded from the combination
    rows['excluded'] = False
    for query in combo.queries.all():
        for manual_exclusion in query.manualexclusion_set.all():
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
