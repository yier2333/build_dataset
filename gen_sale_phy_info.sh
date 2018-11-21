#!/bin/bash
hive -e 'select  distinct class1, class1id, class2, class2id from dm_growth.yx_item_skuinfo;' > sale_info.txt
 

hive -e 'select  distinct phy_category1_name, phy_category1_id, phy_category2_name, phy_category2_id from dm_growth.yx_item_skuinfo;' > phy_info.txt
