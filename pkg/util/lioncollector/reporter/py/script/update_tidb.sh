#!/bin/bash

# 本地 tidb-server 文件路径
LOCAL_TIDB_SERVER="/home/zqs/tidb-8.5.0/bin/tidb-server"

# 远程机器的 tidb-server 目标路径
REMOTE_TIDB_SERVER_DIR="/data2/tidb-deploy/tidb-4000/bin"

# tidb_servers 列表
tidb_servers=(
  "10.77.70.143"
  "10.77.70.212"
  "10.77.70.213"
  "10.77.70.214"
  "10.77.70.117"
)

# 遍历每个主机
for host in "${tidb_servers[@]}"; do
  echo "Processing $host..."

#   # 备份远程机器上的 tidb-server
#   ssh $host "sudo mv $REMOTE_TIDB_SERVER_DIR/tidb-server $REMOTE_TIDB_SERVER_DIR/tidb-server.bak"

  # 使用 scp 将文件拷贝到远程机器的临时目录
  scp $LOCAL_TIDB_SERVER $host:/tmp/tidb-server

  # 使用 sudo 将文件从临时目录移动到目标目录
  ssh $host "sudo mv /tmp/tidb-server $REMOTE_TIDB_SERVER_DIR/tidb-server && sudo chmod +x $REMOTE_TIDB_SERVER_DIR/tidb-server"

  # 重启 tidb-4000 服务
  ssh $host "sudo systemctl restart tidb-4000"

  echo "Finished processing $host."
done

echo "All done."