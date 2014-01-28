import io
import os
import zipfile
import pandas as pd

import psycopg2

DELIMITER = '|'
NUM_COLUMNS = 80
MASTER_PICKLE_FILE_NAME = 'all_data.pkl'


def strip_additional_byte_order_marks(unicode_contents):
    """
    Creditor Reporting System data seems to be in utf-16, but with multiple "Byte Order Marks" (BOMs) per file.
    (probably due to cat'ing together several utf-16 files)
    There is generally only supposed to be a leading BOM for a Unicode file.
    This function converts the file contents (already decoded into unicode) so that there is only a leading BOM.
    """
    return unicode_contents.replace(u'\ufeff', '')


def strip_nulls(unicode_contents):
    """
    CRS data seems to have null control characters (NUL) in various fields, which confuse pandas
    """
    return unicode_contents.replace('\0', '')


def clean_line_endings(unicode_contents):
    """
    Oddly there are some fields with embedded "\n" newlines, while "\r\n" is reserved for new rows.
    pandas gets (understandably) confused by the embedded newlines.
    To solve this, remove all \n from the file, and then convert \r to \n, so we end up with no embedded newlines and
    only use \n for new rows, Unix-style.
    """
    return unicode_contents.replace('\n', '').replace('\r', '\n')


def check_delimiter_counts(unicode_contents):
    lines = unicode_contents.split('\n')
    bad_lines = [line for line in lines if len(line) > 0 and line.count(DELIMITER) != NUM_COLUMNS - 1]
    if bad_lines:
        print "Warning", len(bad_lines), "bad lines found"


def clean_crs_file(raw_contents):
    # yes, a little duplicative/wasteful in terms of performance, but easier to read/maintain
    unicode_contents = raw_contents.decode('utf-16')
    unicode_contents = strip_additional_byte_order_marks(unicode_contents)
    unicode_contents = strip_nulls(unicode_contents)
    unicode_contents = clean_line_endings(unicode_contents)

    # a good sniff test
    check_delimiter_counts(unicode_contents)

    # Intentionally encode as utf-8, to be less of a pain.
    # If you let python write the unicode to a file, it chooses this anyhow, I'm just trying to be explicit about it.
    return unicode_contents.encode('utf-8')


def process_zip_file(zip_path, output_path):
    print 'Converting', zip_path, 'to', output_path

    with zipfile.ZipFile(zip_path) as zipped:
        namelist = zipped.namelist()
        assert len(namelist) == 1

        inner_filename = namelist[0]
        inner_file = zipped.open(inner_filename)
        cleaned_contents = clean_crs_file(inner_file.read())

        with io.open(output_path, 'wb') as output_file:
            output_file.write(cleaned_contents)


def convert_directory(source_dir, dest_dir):
    source_files = os.listdir(source_dir)
    for source_file in sorted(source_files):
        if source_file.startswith("CRS") and source_file.endswith(".zip"):
            source_path = os.path.join(source_dir, source_file)
            dest_path = os.path.join(dest_dir, source_file.replace(' ', '_')).replace('.zip', '.psv')
            process_zip_file(source_path, dest_path)


def build_master_file(processed_dir):
    """
    Concatenates the individual files into one pandas DataFrame and pickles that file back to the same directory
    """
    source_files = os.listdir(processed_dir)
    psv_files = [filename for filename in sorted(source_files) if filename.endswith(".psv")]
    psv_paths = [os.path.join(processed_dir, psv_file) for psv_file in psv_files]
    crs_dataframes = [pd.read_csv(psv_path, delimiter=DELIMITER, low_memory=False) for psv_path in psv_paths]
    master_frame = pd.concat(crs_dataframes, ignore_index=True)

    output_path = os.path.join(processed_dir, MASTER_PICKLE_FILE_NAME)
    master_frame.to_pickle(output_path)
    print "Wrote", output_path


def apply_purpose_code_filter(dataframe):
    desired_purpose_code_prefixes =\
        (11,  # Education
         15,  # Government/CivilSociety
         16,  # Other Social Infrastructure and Services
         22,  # Communications
         43,  # Other Multisector
         72,  # Emergency Response
         73,  # Reconstruction Relief and Rehabilitation
         74,  # Disaster Prevention and Preparedness
         99  # Unallocated
    )

    def is_desired_purpose_code(purpose_code):
        purpose_code_prefix = purpose_code/1000
        return purpose_code_prefix in desired_purpose_code_prefixes

    return dataframe[dataframe.purposecode.apply(is_desired_purpose_code)]


def apply_country_filter(dataframe):
    # TODO remove this filter once we have more data compression in place
    desired_countries = ('Cambodia', 'Peru', 'Sierra Leone', 'Guatemala', 'Kenya')

    return dataframe[dataframe.recipientname.apply(lambda x: x in desired_countries)]


def apply_year_filter(dataframe):
    # OECD puts big caveats on data before 2002 , but let's go back just a little into that period
    return dataframe[dataframe.Year >= 2000]


def remove_unnecessary_columns(dataframe):
    del dataframe['environment']
    del dataframe['pdgg']
    del dataframe['biodiversity']
    del dataframe['climateMitigation']
    del dataframe['climateAdaptation']
    del dataframe['desertification']
    del dataframe['investmentproject']
    del dataframe['assocfinance']
    del dataframe['commitmentdate']
    del dataframe['typerepayment']
    del dataframe['numberrepayment']
    del dataframe['interest1']
    del dataframe['interest2']
    del dataframe['repaydate1']
    del dataframe['repaydate2']
    del dataframe['grantelement']
    del dataframe['usd_interest']
    del dataframe['usd_outstanding']
    del dataframe['usd_arrears_principal']
    del dataframe['usd_arrears_interest']
    del dataframe['usd_future_DS_principal']
    del dataframe['usd_future_DS_interest']


def filter_master_file(input_path, output_path):
    master = pd.read_pickle(input_path)
    filtered = apply_purpose_code_filter(master)
    filtered = apply_year_filter(filtered)
    filtered = apply_country_filter(filtered)
    remove_unnecessary_columns(filtered)

    filtered.to_pickle(output_path)
    print "Wrote", output_path


def get_db_connection(host, database, user, password):
    return psycopg2.connect(host=host, database=database, user=user, password=password)


def create_crs_table(connection):
    cursor = connection.cursor()

    sql = '''CREATE TABLE crs (
        year integer,
        donorcode integer,
        donorname varchar (63),
        agencycode integer,
        agencyname varchar (63),
        crsid varchar (63),
        projectnumber varchar (63),
        initialreport integer,
        recipientcode integer,
        recipientname varchar (63),
        regioncode integer,
        regionname varchar (63),
        incomegroupcode integer,
        incomegroupname varchar (63),
        flowcode integer,
        flowname varchar (63),
        bi_multi integer,
        category integer,
        finance_t integer,
        aid_t varchar (63),
        usd_commitment double precision,
        usd_disbursement double precision,
        usd_received double precision,
        usd_commitment_defl double precision,
        usd_disbursement_defl double precision,
        usd_received_defl double precision,
        usd_adjustment double precision,
        usd_adjustment_defl double precision,
        usd_amountuntied double precision,
        usd_amountpartialtied double precision,
        usd_amounttied double precision,
        usd_amountuntied_defl double precision,
        usd_amountpartialtied_defl double precision,
        usd_amounttied_defl double precision,
        usd_IRTC double precision,
        usd_expert_commitment double precision,
        usd_expert_extended double precision,
        usd_export_credit double precision,
        currencycode integer,
        commitment_national double precision,
        disbursement_national double precision,
        shortdescription text,
        projecttitle text,
        purposecode integer,
        purposename varchar (63),
        sectorcode integer,
        sectorname varchar (63),
        channelcode double precision,
        channelname varchar (63),
        channelreportedname text,
        geography text,
        expectedstartdate varchar (63),
        completiondate varchar (63),
        longdescription text,
        gender double precision,
        trade double precision,
        FTC double precision,
        PBA double precision
    );'''



if __name__ == "__main__":
    # convert_directory('/home/andrew/oecd/crs/downloads/2014-01-10/', '/home/andrew/oecd/crs/processed/2014-01-10/')
    # build_master_file('/home/andrew/oecd/crs/processed/2014-01-10/')
    filter_master_file('/home/andrew/oecd/crs/processed/2014-01-05/' + MASTER_PICKLE_FILE_NAME,
                       '/home/andrew/oecd/crs/processed/2014-01-05/' + 'filtered.pkl')
