import zipfile
import os
import io


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


def convert_to_unix_line_endings(unicode_contents):
    return unicode_contents.replace('\r\n', '\n')


def clean_crs_file(raw_contents):
    # yes, a little duplicative/wasteful in terms of performance, but easier to read/maintain
    unicode_contents = raw_contents.decode('utf-16')
    unicode_contents = strip_additional_byte_order_marks(unicode_contents)
    unicode_contents = strip_nulls(unicode_contents)
    unicode_contents = convert_to_unix_line_endings(unicode_contents)
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


if __name__ == "__main__":
    convert_directory('/home/andrew/oecd/crs/downloads/2014-01-05/', '/home/andrew/oecd/crs/processed/2014-01-05/')
