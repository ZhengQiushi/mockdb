#!/bin/bash

# 采集性能数据
# perf record -F 99 -g --call-graph dwarf -- python /home/zqs/tidb-8.5.0/pkg/util/lioncollector/reporter/py/tests/core/analyze/test_graphpressure.py


# 生成火焰图
# perf script > out.perf
# /home/zqs/FlameGraph/stackcollapse-perf.pl out.perf > out.folded
# /home/zqs/FlameGraph/flamegraph.pl out.folded > flamegraph.svg

py-spy record -o flamegraph.svg -- python /home/zqs/tidb-8.5.0/pkg/util/lioncollector/reporter/py/tests/core/analyze/test_graphpressure.py

# 启动 HTTP 服务器
echo "Flame graph generated at flamegraph.svg"
echo "Open http://localhost:8000/flamegraph.svg in your browser"
python -m http.server 8000
