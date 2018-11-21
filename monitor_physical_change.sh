#!/bin/bash

hive -e "
select distinct 
    a.phy_category1_name, 
    a.phy_category2_name, 
    a.phy_category1_id, 
    a.phy_category2_id 
from dm_growth.yx_item_skuinfo a 
left outer join data_mining.yanxuan_search_qi_phycat_newcat b
on a.phy_category1_id=b.phy_category1_id
and a.phy_category1_name=b.phy_category1_name
and a.phy_category2_id=b.phy_category2_id
and a.phy_category2_name=b.phy_category2_name
where b.phy_category2_name is null 
and a.phy_category2_id!=1002860
and a.phy_category2_id!=1002269
">'phycat_increase.txt'

hive -e "
select distinct 
    b.phy_category1_name, 
    b.phy_category2_name, 
    b.phy_category1_id, 
    b.phy_category2_id 
from dm_growth.yx_item_skuinfo a 
right outer join data_mining.yanxuan_search_qi_phycat_newcat b
on a.phy_category1_id=b.phy_category1_id
and a.phy_category1_name=b.phy_category1_name
and a.phy_category2_id=b.phy_category2_id
and a.phy_category2_name=b.phy_category2_name
where a.phy_category2_name is null 
">'phycat_decrease.txt'

