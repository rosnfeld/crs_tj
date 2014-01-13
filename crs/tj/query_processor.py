import pandas as pd

# HACK just to get this up and running
DATA_FILE = "/home/andrew/oecd/crs/processed/2014-01-05/sector_152.pkl"

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
