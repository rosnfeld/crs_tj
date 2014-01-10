import io
import os
import zipfile
import pandas as pd


DELIMITER = '|'
NUM_COLUMNS = 80


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

    output_path = os.path.join(processed_dir, 'all_data.pkl')
    master_frame.to_pickle(output_path)
    print "Wrote", output_path


if __name__ == "__main__":
    convert_directory('/home/andrew/oecd/crs/downloads/2014-01-10/', '/home/andrew/oecd/crs/processed/2014-01-10/')
    build_master_file('/home/andrew/oecd/crs/processed/2014-01-10/')
