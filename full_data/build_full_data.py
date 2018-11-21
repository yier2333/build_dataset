# -*- coding: utf-8 -*-

import pandas as pd
import os, sys
from collections import defaultdict

path = "/data10/zhuman/build_dataset/full_data/"
os.chdir(path)

query_cat = 'query_cats_2018-11-14.txt'
query_count = 'full_query_count_clean.txt'
save_name = 'full_data.txt'

query_cat = pd.read_csv(query_cat, header=None, names=['query', 'new_category2_id'], sep='\t', dtype=str)
query_cat = query_cat[query_cat['new_category2_id'].isnull()==False]
query_count = pd.read_csv(query_count, header=None, names=['query', 'search_count'], sep='\t', dtype=str)

data = pd.merge(query_cat, query_count)
data = data[data['new_category2_id'].str.len()!=0]
data = data[data['new_category2_id'].isnull()==False]

'''统计每个query的catid个数，同时对id去重'''
def cal_count(ids_str):
    if ids_str=='':
        print
    ids_list = ids_str.split(',')
    ids_dict = defaultdict(int)
    for id in ids_list:
        ids_dict[id] += 1
    sorted_dict = [(k, ids_dict[k]) for k in sorted(ids_dict, key=ids_dict.get, reverse=True)]
    ids_res = ""
    count_res = ""
    for id, count in sorted_dict:
        ids_res += str(id) + ','
        count_res += str(count) + ','
    ids_res = ids_res.strip(',')
    count_res = count_res.strip(',')
    return ids_res, count_res

series = data['new_category2_id'].apply(cal_count)
data['new_category2_id'] = series.map(lambda x : x[0])
data['cat_count'] = series.map(lambda x : x[1])

'''添加对应的cat_name'''
phycat_newcat  = pd.read_csv('/data10/zhuman/build_dataset/click_data/phycat_newcat.txt',
                             sep='\t',
                             header=None,
                             names=['phy_category1_name','phy_category2_name','phy_category1_id','phy_category2_id',
                                    'new_category1_name','new_category2_name','new_category1_id','new_category2_id'],
                             dtype=str)
def map_id_name(ids):
    ids = ids.split(',')
    names = []
    for id in ids:
        values = phycat_newcat.loc[phycat_newcat['new_category2_id']==id, 'new_category2_name'].values
        if len(values) > 0:
            names.append(values[0])
    return ','.join(names)
data['new_category2_name'] = data['new_category2_id'].apply(map_id_name)

data = data[data['new_category2_name'].str.len()!=0]
data = data[['query', 'new_category2_id', 'new_category2_name', 'cat_count', 'search_count']]
data.to_csv(save_name, index=False, sep='\t')
