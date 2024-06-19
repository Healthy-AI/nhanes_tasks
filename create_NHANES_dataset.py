import sys
from os import path
import argparse
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import json

import sklearn
from sklearn.model_selection import train_test_split
sklearn.set_config(transform_output="pandas")

from nhanes.util import *

C_ID = 'SEQN'
C_YEAR = 'YEAR'
C_OUT = 'HYPERT'
SPECIAL_VARS = [C_ID, 'YEAR']
LOG_FILE = 'log_creation.txt'
VERBOSE = False

def create_dataset(cfg, verbose=False):
    """ Takes variables and years specified in the config file, loads them 
        from the corresponding XPT files, as specified in the codebook. 

        The outcome HYPERT is created from blood pressure measurements
            in 'BPXSY1', 'BPXOSY1', 'BPXDI1', 'BPXODI1', depending on the year

        Any observations with missing blood pressure measurements are dropped. 

        Variables with no observations in the selected years are also dropped. 

    args: 
        cfg (Namespace) - Configuration namespace
        verbose (bool)  - If true, prints additional debug messages
    """

    base_path = cfg.path
    out_path = cfg.out_path

    flog = open(os.path.join(out_path, LOG_FILE), 'w')

    # Create a variable and file list for each year
    vars = list(cfg.variables.keys())
    files = {}
    V = {}
    vtypes = {}
    Ds = []
    for year in cfg.years: 
        log(flog, 'Handling year: %s...' % year)

        code_path = path.join(base_path, cfg.codebook.replace('*year*', year))
        with open(code_path, 'r') as f:
            codes = json.load(f)

        files_y = set()
        # Add to global variable dict
        for f, vs in codes.items():
            for k, v in vs.items():
                source = os.path.join(year, v['source'])
                target = v['Target'] if 'Target' in v else None
                if k in vars:
                    files_y.add(source)
                if k not in V: 
                    V[k] = v
                    V[k]['source'] = [source]
                    V[k]['Target'] = [target]
                else:
                    V[k]['source'] += [source]
                    V[k]['Target'] += [target]

        files_y = list(files_y)
        files[year] = files_y
        
        # Build data frame for the year
        D_ys = []
        for f in files_y:     
            log(flog, '    Loading data file: %s...' % f)
            D_yf = pd.read_sas(path.join(base_path, f))
            log(flog, '    Found %d observations' % D_yf.shape[0])

            if C_ID not in D_yf.columns: 
                log(flog, '    Column %s not present. Skipping.' % C_ID)
                continue

            D_yf = D_yf.set_index(D_yf['SEQN'])
            D_ys.append(D_yf[[c for c in D_yf.columns if c != C_ID]])

        log_n_print(flog, 'Concatenating %d data files for year(s) %s...' % (len(D_ys), year))

        D_y = pd.concat(D_ys, axis=1)
        D_y['SEQN'] = D_y.index.values
        D_y[C_YEAR] = year

        log_n_print(flog, 'Found %d observations for the year' % D_y.shape[0])

        sel_vars = [c for c in vars if c in D_y.columns]
        D_y = D_y[SPECIAL_VARS+sel_vars]

        # Handle codes 
        for v in vars: 

            # Skip if not in data frame
            if not v in D_y.columns: 
                log(flog, '    WARNING: Variable %s not found for year %s' % (v, year))
                continue

            # Set variable type
            codes = V[v]['codes']
            t = 'category'
            if 'Range of Values' in [c['desc'] for c in codes]:
                t = 'numeric'
            vtypes[v] = t

            # Replace codes
            for c in codes:
                if c['desc'] == 'Range of Values':
                    pass
                elif c['desc'] == 'Missing':
                    pass
                elif c['desc'] == 'Refused':
                    D_y.loc[:,v] = D_y.loc[:,v].replace(float(c['code']), np.nan)
                elif c['desc'] == 'Don\'t know':
                    D_y.loc[:,v] = D_y.loc[:,v].replace(float(c['code']), np.nan)

            # Some 0-codes end up as 5e-79 for example
            D_y.loc[D_y[v]<1e-16, v] = 0
    
        Ds.append(D_y)


    log_n_print(flog, 'Concatenating %d time spans in total...' % len(Ds))    
    D = pd.concat(Ds, axis=0).copy()    
    log_n_print(flog, 'Found %d observations' % D.shape[0])

    # Add missing columns 
    for v in vars:
        if v not in D:
            D[v] = np.nan

    # Replace vars over years
    D = harmonize_NHANES_columns(D).copy() 

    # Add missing columns, remove unused codes
    for v in vars:
        if v in vtypes and vtypes[v] == 'category':
            D.loc[:,v] = D.loc[:,v].astype('category')

        if v in V: 
            if 'codes' in V[v]:
                codes = [c for c in V[v]['codes'] if c['desc'] not in ['Refused', 'Don\'t know']]
                V[v]['codes'] = codes

    # Store codes
    V_D = dict([(k,v) for k,v in V.items() if k in vars])
    with open(path.join(out_path, cfg.output+'.json'), 'w') as f:
        json.dump(V_D, f, indent=4)

    # Drop rows with missing outcome vars
    n_pre = D.shape[0]
    D = D.dropna(subset=['BPXSY1', 'BPXDI1'])
    n_post = D.shape[0]

    # Drop columns with no observations
    c_miss = [c for c in D.columns if D[c].isna().sum() == D.shape[0]]
    D = D.drop(columns=c_miss)
    if len(c_miss)>0: 
        log_n_print(flog, 'Dropped columns with no observations: %s' % str(c_miss))

    # Define outcome
    D[C_OUT] = 1*np.maximum((D['BPXSY1']>=140),(D['BPXDI1']>=90))
    
    log_n_print(flog, 'Dropping %d observations without the outcome. %d remaining.' % (n_pre-n_post, n_post))

    # Create canonical train/test split
    D_tr, D_te = train_test_split(D, test_size=0.1, random_state=0, shuffle=True)

    # Save to pickle files
    D.to_pickle(path.join(out_path, cfg.output+'.pkl'))
    D_tr.to_pickle(path.join(out_path, cfg.output+'.train.pkl'))
    D_te.to_pickle(path.join(out_path, cfg.output+'.test.pkl'))

    # Save code file
    V['YEAR'] = {
        'SAS Label': 'Year(s) when the observation was collected',
        'codes': None
    }
    V['HYPERT'] = {
        'SAS Label': 'Whether a subject had hypertension (BPS>140 or BPDI>90)',
        'codes': [{
            "code": "0",
            "desc": "No",
        },{
            "code": "1",
            "desc": "Yes",
        }]
    }
    fcode = open(path.join(out_path, cfg.output+'.codes.txt'), 'w')
    for k in D.columns: 
        v = V.get(k, {'SAS Label': 'Unknown', 'codes': None})
        codestr = ''
        if not v['codes'] is None:
            codestr = ','.join(['\'%s\'=%s' % (c['code'], c['desc']) for c in v['codes'] ])
        s = '%s %s - %s\n' % (k, v['SAS Label'], codestr)
        fcode.write(s)
    fcode.close()


def harmonize_NHANES_columns(D):

    def replace_nans(col1, col2):
        """ Replaces missing values in col1 with values in col2 whether those are missing or not 
            Returns the resulting version of col1 
        """
        colo = col1.copy()
        Ina = col1.isna()
        colo[Ina] = col2[Ina]
        return colo

    # Blood pressure
    D['BPXSY1'] = replace_nans(D['BPXSY1'], D['BPXOSY1'])
    D['BPXDI1'] = replace_nans(D['BPXDI1'], D['BPXODI1'])

    # Fat consumption
    D.loc[:,'DRXTTFAT'] = replace_nans(D['DRXTTFAT'], D['DR1TTFAT'])
    D.loc[:,'DRXTSFAT'] = replace_nans(D['DRXTSFAT'], D['DR1TSFAT'])

    # Smoking
    D.loc[:,'SMD680'] = replace_nans(D['SMD680'], D['SMDANY'])
    D.loc[:,'SMD415'] = replace_nans(D['SMD415'], D['SMD460'])
    D.loc[:,'SMDANY'] = replace_nans(D['SMDANY'], D['SMD415A'])
    D.loc[:,'SMD415A'] = replace_nans(D['SMD415A'], D['SMD415'])

    # Occupation
    D.loc[:,'OCQ180'] = replace_nans(D['OCQ180'], D['OCD180'])
    D.loc[:,'OCQ380'] = replace_nans(D['OCQ380'], D['OCD383'])

    return D


if __name__ == "__main__":
    
    # Parse arguments
    parser = argparse.ArgumentParser(description='Create NHANES dataset')
    parser.add_argument('-c', '--config', type=str, dest='config', help='Path to config file', default='configs/NHANES_hypertension.yml')
    parser.add_argument('-v', '--verbose', type=str, dest='verbose', help='Verbose?', default=False)
    args = parser.parse_args()

    # Load config file
    cfg = load_config(args.config, as_namespace=True, deep=False)

    # Fit simulator
    create_dataset(cfg, verbose=args.verbose)
    
