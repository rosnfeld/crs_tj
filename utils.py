import zipfile
import os.path
import io


def ensure_only_leading_byte_order_mark(raw_contents):
    """
    Creditor Reporting System data seems to be in utf-16, but with multiple "Byte Order Marks" (BOMs) per file.
    (probably due to cat'ing together several utf-16 files)
    There is generally only supposed to be a leading BOM for a Unicode file.
    This function converts the file contents so that there is only a leading BOM.
    """
    bom_stripped_contents = raw_contents.replace('\xff\xfe', '')
    return '\xff\xfe' + bom_stripped_contents


def process_zip_file(zip_path):
    with zipfile.ZipFile(zip_path) as zipped:
        namelist = zipped.namelist()
        assert len(namelist) == 1

        inner_filename = namelist[0]
        inner_file = zipped.open(inner_filename)
        leading_bom_contents = ensure_only_leading_byte_order_mark(inner_file.read())
        output_path = os.path.join(os.path.dirname(zip_path), inner_filename.replace(' ', '_'))

        with io.open(output_path, 'wb') as output_file:
            output_file.write(leading_bom_contents)

if __name__ == "__main__":
    # do a test conversion
    process_zip_file('/home/andrew/oecd/crs/processed/2014-01-05/CRS 2000-01 data.zip')
