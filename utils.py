
def standardize_utf16(input_file_path, output_file_path):
    """
    Creditor Reporting System data seems to be in utf-16, but with multiple "Byte Order Marks" (BOMs) per file.
    (probably due to cat'ing together several utf-16 files)
    There is generally only supposed to be a leading BOM for a Unicode file.
    This function converts an input file so that it only has a leading BOM.
    """
    with open(input_file_path, 'rb') as input_file:
        raw_contents = input_file.read()
        bom_stripped_contents = raw_contents.replace('\xff\xfe', '')
        leading_bom_contents = '\xff\xfe' + bom_stripped_contents

        with open(output_file_path, 'wb') as output_file:
            output_file.write(leading_bom_contents)
