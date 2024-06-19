import sys, os
import argparse
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import json

from nhanes.util import *

def create_codebook(cfg):
    """ Creates a codebook for each year specified in the config file, and
        saves as a json in the base path directory
    """
    base_path = cfg.path

    for year in cfg.years:
        print(year)

        # Gather all .htm files 
        base_dir = os.path.join(base_path, year)
        files = []
        for root, dirs, _files in os.walk(base_dir):
            for file in _files:
                if file.endswith(".htm"):# and file.startswith("P_"):            
                    file = os.path.join(root, file).replace(base_dir,'')
                    file = file if file[0] != '/' else file[1:]
                    files.append(file)

        # Parse each file and store the code book
        all_variables = {}
        for file in files:
            print('Parsing %s...' % file)
            
            f = open(os.path.join(base_dir, file), 'r')
            S = f.read()
            soup = BeautifulSoup(S, "html.parser")

            rows = soup.find_all("div", attrs={'class': 'pagebreak'})

            variables = {}

            for r in rows: 
                title_row = r.find("h3", attrs={'class': 'vartitle'})
                ps = title_row.text.split(' - ')
                code, label = ps[0].upper(), ps[1]

                # Extract variable meta data
                desc = r.find('dl')
                ks = [c.text.strip().replace(':','') for c in r.find_all('dt')]
                vs = [c.text.strip().replace('\n',' ').replace('\t','') for c in r.find_all('dd')]
                D = dict([(ks[i], vs[i]) for i in range(len(ks))]) # @TODO: Actually, the same key might appear twice (e.g., Target in AUAEXSTS)
                D['source'] = file.replace('.htm', '.XPT')
                
                variables[code] = D

                # Extract code table
                tab = r.find("table", attrs={'class': 'values'})
                if tab is None: 
                    variables[code]['type'] = 'id' # @TODO: Not sure this is true for all such cases. Thinking of SEQN
                    variables[code]['codes'] = None
                    continue

                trs = tab.find_all('tr')
                tabhead = [th.text.strip() for th in trs[0].find_all('th')]
                tabrows = [[th.text.strip().replace('\n',' ').replace('\t','') for th in tr.find_all('td')] for tr in trs[1:]]
                df = pd.DataFrame(tabrows, columns=tabhead)
                
                tabd = [{'code': tr[0], 'desc': tr[1], 'count': tr[2], 'cum': tr[3], 'skip': tr[4]} for tr in tabrows]

                # Detect variable type
                if 'Range of Values' in df['Value Description'].values:
                    vtype = 'numeric'
                else:
                    vtype = 'categorical'

                variables[code]['type'] = vtype
                variables[code]['codes'] = tabd

            all_variables[file] = variables

        with open(os.path.join(base_path, cfg.codebook.replace('*year*', year)), "w") as out_file:
            json.dump(all_variables, out_file, indent = 4)  


if __name__ == "__main__":
    
    # Parse arguments
    parser = argparse.ArgumentParser(description='Create NHANES json codebook')
    parser.add_argument('-c', '--config', type=str, dest='config', help='Path to config file', default='configs/NHANES_hypertension.yml')
    args = parser.parse_args()

    # Load config file
    cfg = load_config(args.config, as_namespace=True, deep=False)

    # Fit simulator
    create_codebook(cfg)