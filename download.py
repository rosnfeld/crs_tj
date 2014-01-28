"""
For downloading data from OECD
"""

import urllib2
import re

CRS_BASE_URL = 'http://stats.oecd.org'
CRS_FILES_URL = CRS_BASE_URL + '/DownloadFiles.aspx?HideTopMenu=yes&DatasetCode=crs1'
CRS_DOWNLOAD_LINK_PATTERN = r"\.(/FileView2.aspx\?IDFile=.*?)'"


def get_download_links():
    url_handle = urllib2.urlopen(CRS_FILES_URL)
    download_text = url_handle.read()
    relative_links = re.findall(CRS_DOWNLOAD_LINK_PATTERN, download_text)
    return [CRS_BASE_URL + relative_link for relative_link in relative_links]

