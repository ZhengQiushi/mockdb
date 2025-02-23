#!/bin/bash

# PD 地址
PD_ADDRESS="http://10.77.70.250:12379"

# 获取所有 region_id 的 API 地址
REGIONS_API="http://10.77.70.212:10080/tables/benchbase/usertable/regions"

# 使用 curl 获取 region_id 列表
REGION_IDS=$(curl -s $REGIONS_API | jq -r '.record_regions[].region_id')

# 检查是否成功获取 region_id
if [ -z "$REGION_IDS" ]; then
  echo "Failed to fetch region IDs or no regions found."
  exit 1
fi

# 循环删除每个 region_id 对应的 Operator
for idx in $REGION_IDS
do
  echo "Removing operator for region_id: $idx"
  tiup ctl:v8.5.0 pd -u $PD_ADDRESS operator remove $idx
done

echo "All operators removed."