#!/usr/bin/env python
# encoding: utf-8

import pandas as pd
import os, sys
from collections import defaultdict

path = "/data10/zhuman/build_dataset/"
sys.path.append(path)
import utils
os.chdir(path + "full_data")



data = pd.read_csv("full_query_count.txt", header=None, names=['query', 'count'], sep='\t')
data = utils.process_query(data)
data = data.groupby('query').sum()
data.reset_index(level=0, inplace=True)

data.to_csv("full_query_count_clean.txt", header=False, index=False, sep='\t')
data['query'].to_csv("full_query.txt", header=False, index=False, sep='\t')
