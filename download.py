"""
For downloading data from OECD
"""

import urllib2
import re
import os

CRS_BASE_URL = 'http://stats.oecd.org'
CRS_FILES_URL = CRS_BASE_URL + '/DownloadFiles.aspx?HideTopMenu=yes&DatasetCode=crs1'
CRS_DOWNLOAD_LINK_PATTERN = r"\.(/FileView2.aspx\?IDFile=.*?)'"


def get_download_links():
    response = urllib2.urlopen(CRS_FILES_URL)
    download_text = response.read()
    relative_links = re.findall(CRS_DOWNLOAD_LINK_PATTERN, download_text)
    return [CRS_BASE_URL + relative_link for relative_link in relative_links]


def download_csv_file(url, dest_dir):
    response = urllib2.urlopen(url)

    attachment_text = response.headers['content-disposition']
    filename = attachment_text.replace('attachment; filename=', '').replace(';', '')

    content = response.read()

    output_path = os.path.join(dest_dir, filename)

    with open(output_path, 'wb') as filehandle:
        print "Writing " + url + " to " + output_path
        filehandle.write(content)


# TODO create versioned directory (use date?)
# TODO download all links to versioned directory
# TODO apply processing currently in utils... combine scripts? call it build_repository.py or similar?
