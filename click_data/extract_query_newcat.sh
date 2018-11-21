#!/bin/bash

# 手动更新phycat_newcat.txt文件后，更新hive映射表
hive -e "
load data local inpath '/data10/zhuman/build_dataset/click_data/phycat_newcat.txt' 
overwrite into table data_mining.yanxuan_search_qi_phycat_newcat;"

# 生成销售类目和物理类目的hive映射表
hive -e "
insert overwrite table data_mining.yanxuan_search_qi_salecat_phycat
select distinct 
				class1, 
				class2, 
				class1id, 
				class2id, 				
				phy_category1_name, 
				phy_category2_name, 
				phy_category1_id, 
				phy_category2_id 
from dm_growth.yx_item_skuinfo 
where class1id!=0 
and phy_category2_id is not null;"

# join上面两个表生成销售类目到新类目的hive映射表
hive -e "
insert overwrite table data_mining.yanxuan_search_qi_salecat_newcat
select 
		a.class1, 
        a.class2, 
        a.class1id, 
        a.class2id, 		
        b.new_category1_name, 
        b.new_category2_name, 
        b.new_category1_id, 
        b.new_category2_id 
from data_mining.yanxuan_search_qi_salecat_phycat a 
inner join data_mining.yanxuan_search_qi_phycat_newcat b 
on a.phy_category1_name=b.phy_category1_name 
and a.phy_category2_name=b.phy_category2_name;"

# 生成商品到物理类目新类目的映射关系表
hive -e "
insert overwrite table data_mining.yanxuan_search_qi_itemid_phycat_newcat
select distinct 
				cast(a.itemid as string),                
                a.phy_category1_name, 
				a.phy_category2_name,
				a.phy_category1_id, 
                a.phy_category2_id,               
				b.new_category1_name, 
				b.new_category2_name, 
				b.new_category1_id, 
				b.new_category2_id 
from dm_growth.yx_item_skuinfo a
inner join data_mining.yanxuan_search_qi_phycat_newcat b
on a.phy_category1_id=b.phy_category1_id
and a.phy_category2_id=b.phy_category2_id
where b.new_category2_name!='nan';"

# 生成query到新类目的映射关系表

if [ $# != 2 ];
then
    echo "[BEGIN DATE] and [END DATE] needed."
        exit 1;
fi

BEGIN_DATE=$1
END_DATE=$2

hive -e "
select query_cat.query, query_cat.cat_ids, query_count.count
from
	(select query, concat_ws(',', collect_list(new_category2_id)) as cat_ids
	from (
	        select query, itemid
	        from data_mining.yanxuan_search_click_ks a lateral VIEW explode(split(concat_ws(',', exactlymatchclickitems), ',')) my_view AS itemid
	        where length(concat(',', exactlymatchclickitems)) > 2
			and ds between '${BEGIN_DATE}' and '${END_DATE}' 
	    )query_itemid
	inner join (
	            select itemid, new_category2_id
	            from data_mining.yanxuan_search_qi_itemid_phycat_newcat
	            )itemid_newcat
	on query_itemid.itemid = itemid_newcat.itemid
	group by query)query_cat
inner join 
	(select query, count(ds) as count 
    from data_mining.yanxuan_search_click_ks 
	where ds between '${BEGIN_DATE}' and '${END_DATE}' 
    group by query)query_count
on  query_cat.query=query_count.query
" > query_newcat.txt

# 生成click点击数据
/home/di/anaconda3/bin/python build_click_data.py


# 上面生成的新类目到销售类目的映射关系没有过滤，下面进行过滤得到新的映射关系
# 先得到新类目到销售类目的映射关系，包括销售类目的个数
bash ./gen_newcat_salecat_count.sh

# 进行过滤得到netcat salecat过滤后的映射文件 newcat_salecat_filter.txt
/home/di/anaconda3/bin/python count_and_sort.py

# 将映射表上传到hive表中
hive -e "
load data local inpath '/data10/zhuman/build_dataset/click_data/newcat_salecat_filter.txt'
overwrite into table data_mining.yanxuan_search_qi_newcat_salecat_filter;
"


