#!/bin/bash

hive -e "
select new_category2_name, new_category2_id, concat_ws(',', collect_list(sale_category2_name)), concat_ws(',', collect_list(sale_category2_id))
from(
    select new_category2_name, new_category2_id, sale_category2_name, sale_category2_id
    from data_mining.yanxuan_search_qi_phycat_newcat as a
    inner join
    (select
        phy_category2_name,
        cast(phy_category2_id as string),
        concat_ws(',', collect_list(class2)) as sale_category2_name,
        concat_ws(',', collect_list(cast(class2id as string))) as sale_category2_id
    from dm_growth.yx_item_skuinfo
    where phy_category2_name is not null
    and class2id!=0
    group by phy_category2_name, phy_category2_id) as b
    on a.phy_category2_name=b.phy_category2_name
    where new_category2_name is not null) as c
group by new_category2_name, new_category2_id
" > newcat_salecat.txt
