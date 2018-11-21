import pandas as pd
from collections import defaultdict

data = pd.read_csv('newcat_salecat.txt', sep='\t', header=None, dtype=str,
                   names=['new_category2_name', 'new_category2_id', 'sale_category2_name', 'sale_category2_id'])

'''统计每个sale的个数，同时去重'''
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


series = data['sale_category2_name'].apply(cal_count)
data['sale_category2_name'] = series.map(lambda x : x[0])
data['sale_count'] = series.map(lambda x : x[1])

series = data['sale_category2_id'].apply(cal_count)
data['sale_category2_id'] = series.map(lambda x : x[0])
data = data[data['new_category2_name'].isnull()==False]
#data.to_csv('newcat_salecat_no_filter.txt', index=None, sep='\t')

res = pd.DataFrame(columns=['new_category2_name', 'new_category2_id', 'sale_category2_name', 'sale_category2_id'])
for index, row in data.iterrows():
    sale_category2_name = row['sale_category2_name'].split(',')
    sale_category2_id = row['sale_category2_id'].split(',')
    sale_count = [int(i) for i in row['sale_count'].split(',')]
    max_count = max(sale_count)
    for i in range(len(sale_count)):
        res.loc[res.shape[0]] = [row['new_category2_name'], row['new_category2_id'], sale_category2_name[i], sale_category2_id[i]]

res.to_csv('newcat_salecat_no_filter.txt', header=None, index=None, sep='\t')

'''过滤掉salecat count较少的sale'''
res = pd.DataFrame(columns=['new_category2_name', 'new_category2_id', 'sale_category2_name', 'sale_category2_id'])
for index, row in data.iterrows():
    sale_category2_name = row['sale_category2_name'].split(',')
    sale_category2_id = row['sale_category2_id'].split(',')
    sale_count = [int(i) for i in row['sale_count'].split(',')]
    max_count = max(sale_count)
    for i in range(len(sale_count)):
        if (max_count > 20 and sale_count[i] < 5) == False:
            res.loc[res.shape[0]] = [row['new_category2_name'], row['new_category2_id'], sale_category2_name[i], sale_category2_id[i]]

res.to_csv('newcat_salecat_filter.txt', header=None, index=None, sep='\t')



