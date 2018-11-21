# -*- coding: utf-8 -*-

from hanziconv import HanziConv

def process_query(data):
    data = data[data['query'].isnull()==False]
    data['query'] = data['query'].str.lower()
    data['query'] = data['query'].apply(HanziConv.toSimplified)
    data['query'] = data['query'].replace('[^0-9a-z\u4e00-\u9faf]+', ' ', regex=True)
    data['query'] = data['query'].str.strip()
    data = data[data['query'].str.len()!=0]
    data = data[data['query'].replace(' ', '', regex=True).str.encode( 'UTF-8' ).str.isalnum()==False]
    #data = data[data['query'].replace(' ', '', regex=True).str.isdigit()==False]
    #data = data[data['query'].replace(' ', '', regex=True).str.encode( 'UTF-8' ).str.isalpha()==False]
    return data
