"""
For processing downloaded CRS data into a more manageable format
"""
import io
import os
import zipfile
import pandas as pd

RAW_CRS_DELIMITER = '|'
RAW_CRS_NUM_COLUMNS = 80
MASTER_PICKLE_FILE_NAME = 'all_data.pkl'


def strip_additional_byte_order_marks(unicode_contents):
    """
    Creditor Reporting System data seems to be in utf-16, but with multiple "Byte Order Marks" (BOMs) per file.
    (probably due to cat'ing together several utf-16 files)
    There is generally only supposed to be a leading BOM for a Unicode file.
    This function removes BOMs so that there is at most only the (implicit inside python) leading BOM.
    """
    return unicode_contents.replace(u'\ufeff', u'')


def strip_nulls(unicode_contents):
    """
    CRS data seems to have null control characters (NUL) in various fields, which confuse pandas
    """
    return unicode_contents.replace(u'\0', u'')


def clean_line_endings(unicode_contents):
    """
    Oddly there are some fields with embedded "\n" newlines, while "\r\n" is reserved for new rows.
    pandas gets (understandably) confused by the embedded newlines.
    To solve this, remove all \n from the file, and then convert \r to \n, so we end up with no embedded newlines and
    only use \n for new rows, Unix-style.
    """
    return unicode_contents.replace(u'\n', u'').replace(u'\r', u'\n')


def check_delimiter_counts(unicode_contents):
    lines = unicode_contents.split('\n')
    bad_lines = [line for line in lines if len(line) > 0 and line.count(RAW_CRS_DELIMITER) != RAW_CRS_NUM_COLUMNS - 1]
    if bad_lines:
        print "Warning", len(bad_lines), "bad lines found"


def clean_crs_file(raw_contents):
    """
    Given the (byte) contents of an unzipped raw CRS file, returns a "sanitized" utf-8 version of those contents
    """

    # multi-pass is a little duplicative/wasteful in terms of performance, but easier to read/maintain
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
    """
    Unzips a downloaded CRS file and "cleans" the raw contents into a more manageable format
    """
    print 'Converting', zip_path, 'to', output_path

    with zipfile.ZipFile(zip_path) as zipped:
        namelist = zipped.namelist()
        assert len(namelist) == 1

        inner_filename = namelist[0]
        inner_file = zipped.open(inner_filename)
        cleaned_contents = clean_crs_file(inner_file.read())

        with io.open(output_path, 'wb') as output_file:
            output_file.write(cleaned_contents)


def convert_download_directory(download_dir, processed_dir):
    """
    Processes all files within a "download" directory and outputs them to a "processed" directory
    """
    downloaded_files = os.listdir(download_dir)
    for source_file in sorted(downloaded_files):
        if source_file.startswith('CRS') and source_file.endswith('.zip'):
            source_path = os.path.join(download_dir, source_file)
            dest_path = os.path.join(processed_dir, source_file.replace(' ', '_')).replace('.zip', '.psv')
            process_zip_file(source_path, dest_path)


def build_master_file(processed_dir):
    """
    Concatenates the individual files into one pandas DataFrame and pickles that file back to the same directory
    """
    source_files = os.listdir(processed_dir)
    psv_files = [filename for filename in sorted(source_files) if filename.endswith(".psv")]
    psv_paths = [os.path.join(processed_dir, psv_file) for psv_file in psv_files]
    crs_dataframes = [pd.read_csv(psv_path, delimiter=RAW_CRS_DELIMITER, low_memory=False) for psv_path in psv_paths]
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


if __name__ == "__main__":
    download_dir = '/home/andrew/oecd/crs/downloads/2014-01-30/'
    processed_dir = download_dir.replace('downloads', 'processed')

    os.makedirs(processed_dir)
    convert_download_directory(download_dir, processed_dir)
    build_master_file(processed_dir)
    filter_master_file(processed_dir + MASTER_PICKLE_FILE_NAME, processed_dir + 'filtered.pkl')
