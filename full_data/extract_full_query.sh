#!/bin/bash

if [ $# != 2 ];
then
    echo "[BEGIN DATE] and [END DATE] needed."
        exit 1;
fi

BEGIN_DATE=$1
END_DATE=$2


hive -e "
select query,
       count(ds) 
from data_mining.yanxuan_search_click_ks
where length(exactlymatchclickitems)>0 or length(exactlymatchunclickitems)>0
and ds between '${BEGIN_DATE}' and '${END_DATE}'
group by query
" > full_query_count.txt

/home/di/anaconda3/bin/python process_query.py
