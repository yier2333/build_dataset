#!/bin/bash

CUR_DATE=`date +%Y-%m-%d`

DECREASE_COUNT=`wc -l /data10/zhuman/build_dataset/phycat_decrease.txt | cut -d " " -f 1`
INCREASE_COUNT=`wc -l /data10/zhuman/build_dataset/phycat_increase.txt | cut -d " " -f 1`

#echo ${DECREASE_COUNT}
#echo ${INCREASE_COUNT}
ALTER_COUNT=$((DECREASE_COUNT + INCREASE_COUNT))
#echo ${ALTER_COUNT}

echo "phycat_alter_monitor ${ALTER_COUNT} != 0 ${CUR_DATE} Phycat had been changed, decrease count:${DECREASE_COUNT} increase count:${INCREASE_COUNT}"
