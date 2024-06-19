import os
import numpy as np
import pandas as pd
import argparse
import yaml
import time
import pickle as pkl

def dict_to_namespace(d, deep=True):
    """ Converts a dictionary (recursively) to an argparse namespace
    """
    if type(d) is dict:
        for k,v in d.items():
            if deep: 
                d[k] = dict_to_namespace(v)
            else:
                d[k] = v
        return argparse.Namespace(**d)
    else:
        return d

def load_config(config_file, as_namespace=True, deep=True):
    """ Loads a Yaml configuration file and converts it to namespace form
    """
    with open(config_file, 'r') as file:
        if as_namespace:
            cfg = dict_to_namespace(yaml.safe_load(file), deep=deep)
        else: 
            cfg = yaml.safe_load(file)

    return cfg


def log_n_print(f, s):
    if not f is None:
        f.write(s+'\n')
    print(s)

def log(f, s):
    if not f is None:
        f.write(s+'\n')