# -*- coding: utf-8 -*-

import pandas as pd
import os, sys
from collections import defaultdict

path = "/data10/zhuman/build_dataset/"
sys.path.append(path)
import utils
os.chdir(path + "click_data")

filename = "query_newcat.txt"
savename = "click_data.txt"

data = pd.read_csv(filename, header=None, names=['query', 'new_category2_id', 'search_count'], sep='\t', dtype=str)
data = utils.process_query(data)
data = data[data['new_category2_id'].str.len()!=0]
data = data[data['new_category2_id'].isnull()==False]

'''处理query后聚合相同query的new_category2_id/search_count'''
def combine(arr):
    return ','.join(arr)

def add(arr):
    return sum([int(i) for i in arr])

df1 = pd.pivot_table(data, values='new_category2_id', index='query', aggfunc=combine)
df2 = pd.pivot_table(data, values='search_count', index='query', aggfunc=add)
data = pd.concat((df1, df2), axis=1)
data.reset_index(level=0, inplace=True)


'''统计每个query的catid个数，同时对id去重'''
def cal_count(ids_str):
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
phycat_newcat  = pd.read_csv('phycat_newcat.txt',
                             sep='\t',
                             header=None,
                             names=['phy_category1_name','phy_category2_name','phy_category1_id','phy_category2_id',
                                    'new_category1_name','new_category2_name','new_category1_id','new_category2_id'],
                             dtype=str)

def map_id_name(ids):
    ids = ids.split(',')
    names = []
    for id in ids:
        name = phycat_newcat.loc[phycat_newcat['new_category2_id']==id, 'new_category2_name'].values[0]
        names.append(name)
    return ','.join(names)
data['new_category2_name'] = data['new_category2_id'].apply(map_id_name)

data = data[['query', 'new_category2_id', 'new_category2_name', 'cat_count', 'search_count']]
data.to_csv(savename, index=False, sep='\t')
