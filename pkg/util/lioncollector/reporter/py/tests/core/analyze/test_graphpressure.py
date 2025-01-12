import sys
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor
from collections import deque
import threading

# 确保能够导入核心模块
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from core.analyze.graph import Graph

# Constants
NUM_TRANSACTIONS = 100000  # 总事务数量
MAX_REGION_ID = 100000       # 最大region ID
MIN_REGIONS = 1            # 事务中最小region数量
MAX_REGIONS = 5            # 事务中最大region数量
MIN_WEIGHT = 1             # 最小事务权重
MAX_WEIGHT = 10            # 最大事务权重
NUM_THREADS = 10           # 线程数量
STATS_INTERVAL = 1         # 统计结果打印间隔（秒）

import time
def add_transaction_stub(regions, weight):
    time.sleep(0.0001)  # 模拟耗时1ms的事务


class TestGraphPerformance:
    def __init__(self):
        self.graph = Graph(weight=1, theta=1, top_hot_threshold=5)
        self.latencies = deque()  # 存储每个事务的延迟（毫秒）
        self.transaction_count = 0  # 已完成的事务数量
        self.start_time = time.time()  # 测试开始时间
        self.lock = threading.Lock()  # 用于保护共享数据的锁

    def add_transaction(self, regions, weight):
        start_time = time.time()
        self.graph.add_transaction(regions, weight)
        # add_transaction_stub(regions, weight)
        end_time = time.time()
        latency = (end_time - start_time) * 1000  # 转换为毫秒
        with self.lock:
            self.latencies.append(latency)
            self.transaction_count += 1

    def generate_transaction(self):
        num_regions = random.randint(MIN_REGIONS, MAX_REGIONS)
        regions = random.sample(range(1, MAX_REGION_ID + 1), num_regions)
        weight = random.randint(MIN_WEIGHT, MAX_WEIGHT)
        return regions, weight

    def print_stats(self):
        last_count = 0  # 上一个时间间隔内完成的事务数量
        last_latencies = deque()  # 上一个时间间隔内的延迟数据

        while True:
            time.sleep(STATS_INTERVAL)
            with self.lock:
                completed = self.transaction_count
                if completed >= NUM_TRANSACTIONS:
                    break

                # 计算当前时间间隔内完成的事务数量
                interval_count = completed - last_count
                last_count = completed

                # 计算当前时间间隔内的延迟数据
                interval_latencies = list(self.latencies)[-interval_count:] if interval_count > 0 else []
                last_latencies = deque(interval_latencies)

                # 计算吞吐量和平均延迟
                throughput = interval_count / STATS_INTERVAL  # 事务/秒
                avg_latency = sum(last_latencies) / len(last_latencies) if last_latencies else 0

                print(f"[Progress] Transactions: {completed}/{NUM_TRANSACTIONS}, "
                    f"Throughput (last {STATS_INTERVAL}s): {throughput:.2f} transactions/second, "
                    f"Average Latency (last {STATS_INTERVAL}s): {avg_latency:.2f} milliseconds")

    def run_performance_test(self):
        # 打印所有参数
        print("Performance Test Parameters:")
        print(f"  Total Transactions: {NUM_TRANSACTIONS}")
        print(f"  Max Region ID: {MAX_REGION_ID}")
        print(f"  Min Regions per Transaction: {MIN_REGIONS}")
        print(f"  Max Regions per Transaction: {MAX_REGIONS}")
        print(f"  Min Weight: {MIN_WEIGHT}")
        print(f"  Max Weight: {MAX_WEIGHT}")
        print(f"  Number of Threads: {NUM_THREADS}")
        print(f"  Stats Interval: {STATS_INTERVAL} seconds")
        print("Starting performance test...")

        # 启动统计打印线程
        stats_thread = threading.Thread(target=self.print_stats, daemon=True)
        stats_thread.start()

        # 使用线程池并发执行事务
        with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            futures = []
            for _ in range(NUM_TRANSACTIONS):
                regions, weight = self.generate_transaction()
                future = executor.submit(self.add_transaction, regions, weight)
                futures.append(future)
            # 等待所有事务完成
            for future in futures:
                future.result()

        # 测试结束，打印最终结果
        total_time = time.time() - self.start_time
        throughput = NUM_TRANSACTIONS / total_time  # 事务/秒
        avg_latency = sum(self.latencies) / len(self.latencies) if self.latencies else 0
        print("\nPerformance Test Completed:")
        print(f"  Total Transactions: {NUM_TRANSACTIONS}")
        print(f"  Total Time: {total_time:.2f} seconds")
        print(f"  Throughput: {throughput:.2f} transactions/second")
        print(f"  Average Latency: {avg_latency:.2f} milliseconds")

if __name__ == '__main__':
    tester = TestGraphPerformance()
    tester.run_performance_test()