import mechanize
from bs4 import BeautifulSoup
import cookielib
import re
import json
import time
import os
import logging
import pickle
import sys
import traceback

FORMAT = '%(asctime)s [%(levelname)-8s] %(message)s'
formatter = logging.Formatter(FORMAT)
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(formatter)
handler.setLevel(logging.DEBUG)
log = logging.Logger("scraper")
log.addHandler(handler)

BASE_URL = "http://pems.dot.ca.gov"
START_YEAR = 2010
END_YEAR = 2015
BASE_DIR = "/video/dse_capstone/traffic"
PICKLE_FILENAME = BASE_DIR + "/completed_files.pkl"

# define the types of files we want
FILE_TYPES = {'station_5min','station_hour', 'meta', 'chp_incidents_day'}

# Setup download location
if not os.path.exists(BASE_DIR):
    os.makedirs(BASE_DIR)

# Browser
br = mechanize.Browser()

# Cookie Jar
cj = cookielib.LWPCookieJar()
br.set_cookiejar(cj)

# Browser options
br.set_handle_equiv(True)
br.set_handle_referer(True)
br.set_handle_robots(False)
br.set_handle_redirect(mechanize.HTTPRedirectHandler)

# Follows refresh 0 but not hangs on refresh > 0
br.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1)

# Want debugging messages?
#br.set_debug_http(True)
#br.set_debug_redirects(True)
#br.set_debug_responses(True)

log.info("Requesting initial page...")

# User-Agent (this is cheating!  But we need data!)
br.addheaders = [('User-agent', 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')]
br.open(BASE_URL + "/?dnode=Clearinghouse")

log.info("Opened initial page")

br.select_form(nr=0)
br.form['username'] = 'csw009@eng.ucsd.edu'
br.form['password'] = 'password'

log.info("Logging in...")

br.submit()

return_html = br.response().read()
soup = BeautifulSoup(return_html)
log.debug(soup)

log.info("Logged in.")

# Extract the script containing the JSON-like structure containing valid request parameter values
script = soup.find('script', text=re.compile('YAHOO\.bts\.Data'))
j = re.search(r'^\s*YAHOO\.bts\.Data\s*=\s*({.*?})\s*$',
                      script.string, flags=re.DOTALL | re.MULTILINE).group(1)

# The structure is not valid JSON.  The keys are not quoted. Enclose the keys in quotes.
j = re.sub(r"{\s*(\w)", r'{"\1', j)
j = re.sub(r",\s*(\w)", r',"\1', j)
j = re.sub(r"(\w):", r'\1":', j)

# Now that we have valid JSON, parse it into a dict
data = json.loads(j)
assert data['form_data']['reid_raw']['all'] == 'all' # sanity check

ft = {l: data['form_data'][l].values() for l in data['labels'].keys()}

copySet = set(FILE_TYPES)

# filetype -> year -> district -> month -> set of completed files
completedFiles = {}

if os.path.exists(PICKLE_FILENAME):
    f = open(PICKLE_FILENAME, 'rb')
    completedFiles = pickle.load(f)
    f.close()
    log.info("Restored state from pickle file")

try:

    for fileType in FILE_TYPES:
        completedFiles.setdefault(fileType, {})
        for year in [str(x) for x in range(START_YEAR, END_YEAR+1)]:
            completedFiles[fileType].setdefault(year, {})
            for d in ft[fileType]:
                fileSet = completedFiles[fileType][year].setdefault(d, set())
                url = "%s/?srq=clearinghouse&district_id=%s&yy=%s&type=%s&returnformat=text" % (BASE_URL, d, year, fileType)
                br.open(url)
                json_response =  br.response().read()
                responseDict = json.loads(json_response)
                if not responseDict:
                    log.info("No data available for district: %s, year:%s, filetype: %s" % (d, year, fileType))
                    continue
                data = responseDict['data']
                for month in data.keys():
                    destDir = "%s/%s/%s/d%s/" % (BASE_DIR, fileType, year, d)
                    if not os.path.exists(destDir):
                        os.makedirs(destDir)

                    for link in data[month]:
                        filename= link['file_name']
                        if filename not in fileSet:
                            download_link = "%s%s" % (BASE_URL, link['url'])
                            log.info("Starting to download %s", download_link)
                            br.retrieve(download_link, destDir + filename)[0]
                            log.info("Downloaded %s", filename)
                            fileSet.add(link['file_name'])
                            time.sleep(5)
                        else:
                            log.debug("Already downloaded %s.", filename)
        copySet.remove(fileType)
except:
    log.error(traceback.format_exc())
finally:
    pickle.dump(completedFiles, open(PICKLE_FILENAME, "wb"), 2)

# Sanity check to make sure all filetypes were downloaded.  If not, the scraper needs to updated.
if len(copySet) > 0:
    log.error("Could not complete downloads of filetypes %s", list(copySet))
