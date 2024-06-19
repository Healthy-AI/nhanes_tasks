import pandas as pd
import numpy as np
import os, subprocess
import urllib.request
import requests
from bs4 import BeautifulSoup

BASE_URL = 'https://wwwn.cdc.gov'
URLs = {   
    '1999-2000': {
        'DEMOGRAPHICS': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Demographics&CycleBeginYear=1999",
        'DIETARY': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Dietary&CycleBeginYear=1999",
        'EXAMINATION': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Examination&CycleBeginYear=1999",
        'LABORATORY': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Laboratory&CycleBeginYear=1999",
        'QUESTIONNAIRE': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Questionnaire&CycleBeginYear=1999"
    },
    '2001-2002': {
        'DEMOGRAPHICS': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Demographics&CycleBeginYear=2001",
        'DIETARY': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Dietary&CycleBeginYear=2001",
        'EXAMINATION': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Examination&CycleBeginYear=2001",
        'LABORATORY': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Laboratory&CycleBeginYear=2001",
        'QUESTIONNAIRE': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Questionnaire&CycleBeginYear=2001"
    },
    '2003-2004': {
        'DEMOGRAPHICS': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Demographics&CycleBeginYear=2003",
        'DIETARY': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Dietary&CycleBeginYear=2003",
        'EXAMINATION': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Examination&CycleBeginYear=2003",
        'LABORATORY': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Laboratory&CycleBeginYear=2003",
        'QUESTIONNAIRE': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Questionnaire&CycleBeginYear=2003"
    },
    '2005-2006': {
        'DEMOGRAPHICS': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Demographics&CycleBeginYear=2005",
        'DIETARY': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Dietary&CycleBeginYear=2005",
        'EXAMINATION': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Examination&CycleBeginYear=2005",
        'LABORATORY': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Laboratory&CycleBeginYear=2005",
        'QUESTIONNAIRE': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Questionnaire&CycleBeginYear=2005"
    },
    '2007-2008': {
        'DEMOGRAPHICS': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Demographics&CycleBeginYear=2007",
        'DIETARY': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Dietary&CycleBeginYear=2007",
        'EXAMINATION': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Examination&CycleBeginYear=2007",
        'LABORATORY': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Laboratory&CycleBeginYear=2007",
        'QUESTIONNAIRE': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Questionnaire&CycleBeginYear=2007"
    },
    '2009-2010': {
        'DEMOGRAPHICS': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Demographics&CycleBeginYear=2009",
        'DIETARY': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Dietary&CycleBeginYear=2009",
        'EXAMINATION': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Examination&CycleBeginYear=2009",
        'LABORATORY': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Laboratory&CycleBeginYear=2009",
        'QUESTIONNAIRE': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Questionnaire&CycleBeginYear=2009"
    },
    '2011-2012': {
        'DEMOGRAPHICS': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Demographics&CycleBeginYear=2011",
        'DIETARY': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Dietary&CycleBeginYear=2011",
        'EXAMINATION': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Examination&CycleBeginYear=2011",
        'LABORATORY': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Laboratory&CycleBeginYear=2011",
        'QUESTIONNAIRE': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Questionnaire&CycleBeginYear=2011"
    },
    '2013-2014': {
        'DEMOGRAPHICS': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Demographics&CycleBeginYear=2013",
        'DIETARY': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Dietary&CycleBeginYear=2013",
        'EXAMINATION': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Examination&CycleBeginYear=2013",
        'LABORATORY': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Laboratory&CycleBeginYear=2013",
        'QUESTIONNAIRE': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Questionnaire&CycleBeginYear=2013"
    },
    '2015-2016': {
        'DEMOGRAPHICS': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Demographics&CycleBeginYear=2015",
        'DIETARY': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Dietary&CycleBeginYear=2015",
        'EXAMINATION': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Examination&CycleBeginYear=2015",
        'LABORATORY': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Laboratory&CycleBeginYear=2015",
        'QUESTIONNAIRE': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Questionnaire&CycleBeginYear=2015"
    },
    '2017-2018': {
        'DEMOGRAPHICS': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Demographics&CycleBeginYear=2017",
        'DIETARY': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Dietary&CycleBeginYear=2017",
        'EXAMINATION': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Examination&CycleBeginYear=2017",
        'LABORATORY': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Laboratory&CycleBeginYear=2017",
        'QUESTIONNAIRE': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Questionnaire&CycleBeginYear=2017"
    },
    # Removed since it has a different format for the columns
    #'2017-2020': {
    #    'DEMOGRAPHICS': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Demographics&Cycle=2017-2020",
    #    'DIETARY': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Dietary&Cycle=2017-2020",
    #    'EXAMINATION': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Examination&Cycle=2017-2020",
    #    'LABORATORY': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Laboratory&Cycle=2017-2020",
    #    'QUESTIONNAIRE': "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Questionnaire&Cycle=2017-2020"
    #},
}

def format_size(s):
    if s.endswith('MB'): 
        return float(s.partition(' ')[0])
    elif s.endswith('KB'): 
        return float(s.partition(' ')[0])/1000
    elif s.endswith('GB'): 
        print(s)
        return float(s.partition(' ')[0])*1000
    else:
        print(s)
        return 0

def download_if_not_exist(url, path, file=True):
    if file:
        if not os.path.isfile(path):
            subprocess.run(["curl", url, "-o", path]) 
            
    # Not really necessary
    if not os.path.isfile(path):
        page = requests.get(url)
        f = open(path, 'w')
        f.write(page.text)
        f.close()

def download_NHANES():
    """ Downloads data .XPT files and documentation .HTM files from the NHANES website """

    out = []
    for year, v in URLs.items():
        for key, URL in v.items():
            print(year, key, URL)
            page = requests.get(URL)

            soup = BeautifulSoup(page.content, "html.parser")
            results = soup.find(id="GridView1")
            rows = results.find_all("tr")[1:]

            for r in rows: 
                r
                cols = r.find_all("td")
                #years = cols[0].text.strip() # Not the same columns past 2018
                label = cols[0].text.strip()
                #updated = cols[4].text.strip()

                file1 = cols[1].find("a")
                file2 = cols[2].find("a")
                if file1 is None or file2 is None:
                    print('Couldnt find %s, %s, %s. Skipping' % (year, key, r))
                    continue
                
                url1 = BASE_URL+file1['href']
                url2 = BASE_URL+file2['href']
                link1 = file1.text
                link2 = file2.text
                size = link2.partition('[')[-1].partition(' - ')[-1][:-1]

                out += [{'section': key, 'years': year, 'label': label,  'doc_url': url1, 'doc_label': link1, 'data_url': url2, 'data_label': link2, 'data_size': size}]

    files = pd.DataFrame(out)

    """ remove files that are not .htm - .xpt pairs """
    all_files = files.copy()
    files = all_files.iloc[[i for i in range(all_files.shape[0]) if all_files.iloc[i]['data_url'].lower().endswith('.xpt')  and all_files.iloc[i]['doc_url'].lower().endswith('.htm')]]
    #files[files['data_size']=='8.7 GB']

    """ Download files if they don't exist """
    for i in range(files.shape[0]):
        r = files.iloc[i]
        data_url = r['data_url']
        doc_url = r['doc_url']
        data_size = format_size(r['data_size'])
        year = r['years']
        print(year, doc_url)
        
        if data_size > 1000:
            print('File %s, %s is larger than 1GB. Skipping' % (data_url, doc_url))
        
        fdir = os.path.join('data', 'NHANES', year, r['section'])
        os.makedirs(fdir, exist_ok=True)

        data_path = os.path.join(fdir, data_url.split('/')[-1])
        download_if_not_exist(data_url, data_path)
                            
        doc_path = os.path.join(fdir, doc_url.split('/')[-1])
        download_if_not_exist(doc_url, doc_path)

if __name__ == "__main__":
    
    download_NHANES()