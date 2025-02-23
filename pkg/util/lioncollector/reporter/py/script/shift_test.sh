#!/bin/bash

# 检查是否传入了参数
if [ -z "$1" ]; then
  echo "Usage: $0 <is_single=T/F>"
  exit 1
fi

# 解析参数
IS_SINGLE=$1

# PD 地址
PD_ADDRESS="http://10.77.70.250:12379"

# 获取 region_id 和 leader.store_id 的 API 地址
REGIONS_API="http://10.77.70.212:10080/tables/benchbase/usertable/regions"

# 全局 store_id 列表
GLOBAL_STORE_IDS=(1 4 5 6 7)

# 使用 curl 获取前十个 region_id 和 leader.store_id
REGIONS_DATA=$(curl -s $REGIONS_API)
REGION_LEADERS=$(echo "$REGIONS_DATA" | jq -r '.record_regions[0:10] | map({region_id: .region_id, store_id: .leader.store_id})')

# 检查是否成功获取数据
if [ -z "$REGION_LEADERS" ]; then
  echo "Failed to fetch region data or no regions found."
  exit 1
fi
# 初始化 Round-Robin 索引
rr_index=0

# 遍历前十个 region_id 和 leader.store_id
echo "$REGION_LEADERS" | jq -c '.[]' | while read -r region; do
  region_id=$(echo "$region" | jq -r '.region_id')
  current_store_id=$(echo "$region" | jq -r '.store_id')

  # 根据 is_single 参数选择目标 store_id
  if [ "$IS_SINGLE" == "T" ]; then
    target_store_id=${GLOBAL_STORE_IDS[0]}
  else
    target_store_id=${GLOBAL_STORE_IDS[$rr_index]}
    rr_index=$(( (rr_index + 1) % ${#GLOBAL_STORE_IDS[@]} ))
  fi

  # 如果当前 store_id 已经是目标 store_id，则跳过
  if [ "$current_store_id" == "$target_store_id" ]; then
    # echo "Region $region_id already has leader on store $target_store_id. Skipping."
    continue
  fi

  # 执行 Leader 切换
#   echo "Transferring leader of region $region_id from store $current_store_id to store $target_store_id"
    echo "tiup ctl:v8.5.0 pd -u "$PD_ADDRESS" operator add transfer-region $region_id $target_store_id $target_store_id"
    # tiup ctl:v8.5.0 pd -u "$PD_ADDRESS" operator add transfer-region $region_id $target_store_id $target_store_id
#   tiup ctl:v8.5.0 pd -u "$PD_ADDRESS" operator add transfer-leader $region_id $target_store_id
#   # 检查命令是否执行成功
#   if [ $? -eq 0 ]; then
#     echo "Successfully transferred leader of region $region_id to store $target_store_id"
#   else
#     echo "Failed to transfer leader of region $region_id to store $target_store_id"
#   fi

#   # 可选：添加延迟以避免过快请求
#   sleep 1
done

echo "Leader transfer completed."

