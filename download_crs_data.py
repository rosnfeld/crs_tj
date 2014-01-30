"""
For downloading CRS data from OECD
"""

import urllib2
import re
import os
import datetime

CRS_BASE_URL = 'http://stats.oecd.org'
CRS_FILES_URL = CRS_BASE_URL + '/DownloadFiles.aspx?HideTopMenu=yes&DatasetCode=crs1'
CRS_DOWNLOAD_LINK_PATTERN = r"\.(/FileView2.aspx\?IDFile=.*?)'"


def get_crs_download_links():
    """
    Goes to the OECD Stats website and finds the URLs to download raw CRS data
    """
    response = urllib2.urlopen(CRS_FILES_URL)
    download_text = response.read()
    relative_links = re.findall(CRS_DOWNLOAD_LINK_PATTERN, download_text)
    return [CRS_BASE_URL + relative_link for relative_link in relative_links]


def download_csv_file(url, dest_dir):
    """
    Downloads a file from a CRS URL, saving it to the specified directory
    under the filename indicated in the HTTP headers
    """
    response = urllib2.urlopen(url)

    attachment_text = response.headers['content-disposition']
    filename = attachment_text.replace('attachment; filename=', '').replace(';', '')

    content = response.read()

    output_path = os.path.join(dest_dir, filename)

    with open(output_path, 'wb') as filehandle:
        print "Writing " + url + " to " + output_path
        filehandle.write(content)


# could maybe create a CRSDownloader class and this would be a method on it?
# might be nicer in terms of holding on to the download dir path
def create_download_directory(base_dir):
    """
    Creates a subdirectory based on the current date
    """
    current_date_str = str(datetime.date.today())

    download_dir_path = os.path.join(base_dir, current_date_str)

    os.makedirs(download_dir_path)

    return download_dir_path


def download_all_crs_data(base_dir):
    """
    Find the latest raw CRS data and download it to a subdirectory of the specified base_dir.
    Note: this method will download significant data and take significant time.
    """
    links = get_crs_download_links()
    download_dir = create_download_directory(base_dir)
    for link in links:
        download_csv_file(link, download_dir)
