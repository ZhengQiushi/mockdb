#!/bin/bash

# 检查是否提供了时延参数
if [ -z "$1" ]; then
  echo "Usage: $0 <latency_in_ms>"
  exit 1
fi

LATENCY="$1ms"

# 定义集群中的机器IP列表
ALL_HOSTS=("10.77.70.250" "10.77.70.143" "10.77.70.212" "10.77.70.213" "10.77.70.214" "10.77.70.117" "10.77.70.206" "10.77.70.251" "10.77.70.208" "10.77.70.209" "10.77.70.205")

# 定义临近部署情况
declare -A NEARBY_MAP
NEARBY_MAP["10.77.70.143"]="10.77.70.206"
NEARBY_MAP["10.77.70.212"]="10.77.70.251"
NEARBY_MAP["10.77.70.213"]="10.77.70.208"
NEARBY_MAP["10.77.70.214"]="10.77.70.209"
NEARBY_MAP["10.77.70.117"]="10.77.70.205"

# SSH 用户名
SSH_USER="zqs"

# 函数：清除指定机器的时延配置
clear_latency() {
  local host=$1

  echo "Clearing latency configuration on $host"

  ssh ${SSH_USER}@$host "sudo /sbin/tc qdisc del dev eth0 root 2>/dev/null || true"
}

# 函数：为指定机器增加时延
add_latency() {
  local src_host=$1
  local dst_host=$2
  local latency=$3

  echo "Adding $latency latency from $src_host to $dst_host"

  ssh ${SSH_USER}@$src_host "sudo /sbin/tc qdisc add dev eth0 root handle 1: prio 2>/dev/null || true; sudo /sbin/tc qdisc add dev eth0 parent 1:1 handle 10: netem delay $latency; sudo /sbin/tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst $dst_host flowid 1:1"
}

# 函数：将十六进制 IP 转换为点分十进制
hex_to_ip() {
  local hex_ip=$1
  # 将十六进制分成两部分，每部分 2 字节
  local part1=$(echo $hex_ip | cut -c1-2)
  local part2=$(echo $hex_ip | cut -c3-4)
  local part3=$(echo $hex_ip | cut -c5-6)
  local part4=$(echo $hex_ip | cut -c7-8)

  # 将每部分转换为十进制
  local ip=$(printf "%d.%d.%d.%d" 0x$part1 0x$part2 0x$part3 0x$part4)
  echo $ip
}

# 函数：检查时延配置并解析 match 规则
check_latency() {
  local host=$1

  echo "Checking latency configuration on $host:"

  # 获取 tc filter 的输出，并去掉警告信息
  local tc_output=$(ssh ${SSH_USER}@$host "sudo /sbin/tc filter show dev eth0" 2>&1 | grep -v 'warning')
  
#   echo "$tc_output" 
  # 检查是否有 netem 规则
#   if echo "$tc_output"; then
#     echo "Latency is configured."
#   else
#     echo "No latency configured."
#   fi

  # 解析 match 规则
  echo "$tc_output" | grep 'match' | while read -r line; do
    # 提取 match 后的十六进制 IP
    local hex_ip=$(echo "$line" | grep -oP 'match \K[0-9a-fA-F]{8}')
    if [ -n "$hex_ip" ]; then
      # 将十六进制 IP 转换为点分十进制
      local ip=$(hex_to_ip $hex_ip)
      echo "  Match rule for destination IP: $ip"
    fi
  done
}

# 清除所有机器的时延配置
echo "Clearing existing latency configurations on all hosts..."
for host in "${ALL_HOSTS[@]}"; do
  clear_latency $host
done
echo "All existing latency configurations cleared."
echo "----------------------------------------"

# 为所有机器间增加时延，除了临近的 TiDB 和 TiKV
echo "Adding latency between hosts..."
for ((i = 0; i < ${#ALL_HOSTS[@]}; i++)); do
  for ((j = i + 1; j < ${#ALL_HOSTS[@]}; j++)); do
    src_host=${ALL_HOSTS[$i]}
    dst_host=${ALL_HOSTS[$j]}

    # 检查是否是临近的 TiDB 和 TiKV
    if [[ -n "${NEARBY_MAP[$src_host]}" && "${NEARBY_MAP[$src_host]}" == "$dst_host" ]]; then
      echo "Skipping latency between nearby TiDB and TiKV: $src_host -> $dst_host"
    else
      add_latency $src_host $dst_host $LATENCY
    fi
  done
done
echo "Latency configuration completed."
echo "----------------------------------------"

# 检查所有机器的时延配置
echo "Checking latency configurations on all hosts..."
for host in "${ALL_HOSTS[@]}"; do
  check_latency $host
done
echo "----------------------------------------"