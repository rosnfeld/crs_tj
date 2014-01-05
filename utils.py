import zipfile
import os.path
import io

# TODO operate on unicode here rather than raw bytes, as it's possible you operate on unaligned data


def ensure_only_leading_byte_order_mark(raw_contents):
    """
    Creditor Reporting System data seems to be in utf-16, but with multiple "Byte Order Marks" (BOMs) per file.
    (probably due to cat'ing together several utf-16 files)
    There is generally only supposed to be a leading BOM for a Unicode file.
    This function converts the file contents so that there is only a leading BOM.
    """
    bom_stripped_contents = raw_contents.replace('\xff\xfe', '')
    return '\xff\xfe' + bom_stripped_contents


def strip_nulls(raw_contents):
    """
    CRS data seems to have null control characters (NUL) in various fields, which confuse pandas
    """
    # need to specify double null, as it's UTF-16
    return raw_contents.replace('\0\0', '')


def convert_to_unix_line_endings(raw_contents):
    return raw_contents.replace('\r\n', '\n')


def clean_crs_file(raw_contents):
    # yes, a little duplicative/wasteful in terms of performance, but easier to read/maintain
    contents = raw_contents
    contents = ensure_only_leading_byte_order_mark(contents)
    contents = strip_nulls(contents)
    contents = convert_to_unix_line_endings(contents)
    return contents


def process_zip_file(zip_path):
    with zipfile.ZipFile(zip_path) as zipped:
        namelist = zipped.namelist()
        assert len(namelist) == 1

        inner_filename = namelist[0]
        inner_file = zipped.open(inner_filename)
        cleaned_contents = clean_crs_file(inner_file.read())
        output_path = os.path.join(os.path.dirname(zip_path), inner_filename.replace(' ', '_'))

        with io.open(output_path, 'wb') as output_file:
            output_file.write(cleaned_contents)

if __name__ == "__main__":
    # do a test conversion
    process_zip_file('/home/andrew/oecd/crs/processed/2014-01-05/CRS 2000-01 data.zip')
