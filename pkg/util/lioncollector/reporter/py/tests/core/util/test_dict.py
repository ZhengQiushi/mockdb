import sys
import os
import random
import time
from concurrent.futures import ProcessPoolExecutor
from collections import deque
import multiprocessing  # 使用 multiprocessing 的锁
from core.util.bucketDict import BucketedDict
from multiprocessing import Manager, current_process

# 确保能够导入 BucketedDict
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

# Constants
NUM_OPERATIONS = 30000  # 总操作数量
MAX_KEY = 10000         # 最大键值
MIN_KEY = 1             # 最小键值
NUM_PROCESSES = 10      # 进程数量
STATS_INTERVAL = 1      # 统计结果打印间隔（秒）

def simulate_cpu_computation():
    # 模拟一些计算密集型操作
    result = 0
    # for _ in range(1000):  # 调整循环次数以控制计算量
    #     result += random.random() * random.random()
    return result

# 使用 multiprocessing.Manager 创建共享对象
manager = multiprocessing.Manager()
map_stubs = [manager.dict() for _ in range(NUM_PROCESSES)]
  # 共享字典
latencies = manager.list()  # 共享列表，用于存储延迟
operation_count = manager.Value('i', 0)  # 共享计数器
lock = manager.Lock()  # 共享锁

def set_stub(key, val):
    simulate_cpu_computation()
    process_id = current_process()._identity[0] - 3  # Process IDs start from 1
    # print(process_id, "set_stub")
    map_stubs[process_id][key] = val

def get_stub(key):
    simulate_cpu_computation()
    process_id = current_process()._identity[0] - 3 # Process IDs start from 1
    # print(process_id, "get_stub")
    return map_stubs[process_id].get(key, None)  # 使用 get 方法避免 KeyError

def perform_operation():
    key = random.randint(MIN_KEY, MAX_KEY)
    operation = random.choice(["set", "get"])  # 随机选择 set 或 get 操作

    start_time = time.time()
    if operation == "set":
        set_stub(key, "value")
    elif operation == "get":
        get_stub(key)
    end_time = time.time()

    latency = (end_time - start_time) * 1000  # 转换为毫秒
    with lock:
        latencies.append(latency)
        operation_count.value += 1

def print_stats():
    last_count = 0  # 上一个时间间隔内完成的事务数量
    start_time = time.time()
    while True:
        time.sleep(STATS_INTERVAL)
        with lock:
            completed = operation_count.value
            if completed >= NUM_OPERATIONS:
                break
            total_time = time.time() - start_time

            # 计算当前时间间隔内完成的事务数量
            interval_count = completed - last_count
            last_count = completed
            
            throughput = interval_count / STATS_INTERVAL  # 操作/秒
            avg_latency = sum(latencies) / len(latencies) if latencies else 0


            print(f"[Progress] Operations: {completed}/{NUM_OPERATIONS}, "
                  f"Throughput: {throughput:.2f} operations/second, "
                  f"Average Latency: {avg_latency:.2f} milliseconds")

def run_performance_test():
    start_time = time.time()
    # 打印所有参数
    print("Performance Test Parameters:")
    print(f"  Total Operations: {NUM_OPERATIONS}")
    print(f"  Max Key: {MAX_KEY}")
    print(f"  Min Key: {MIN_KEY}")
    print(f"  Number of Processes: {NUM_PROCESSES}")
    print(f"  Stats Interval: {STATS_INTERVAL} seconds")
    print("Starting performance test...")

    # 启动统计打印进程
    stats_process = multiprocessing.Process(target=print_stats)
    stats_process.start()

    # 使用进程池并发执行操作
    with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:
        futures = [executor.submit(perform_operation) for _ in range(NUM_OPERATIONS)]
        for future in futures:
            future.result()

    # 等待统计进程结束
    stats_process.join()

    # 测试结束，打印最终结果
    total_time = time.time() - start_time
    throughput = NUM_OPERATIONS / total_time  # 操作/秒
    avg_latency = sum(latencies) / len(latencies) if latencies else 0
    print("\nPerformance Test Completed:")
    print(f"  Total Operations: {NUM_OPERATIONS}")
    print(f"  Total Time: {total_time:.2f} seconds")
    print(f"  Throughput: {throughput:.2f} operations/second")
    print(f"  Average Latency: {avg_latency:.2f} milliseconds")

if __name__ == '__main__':
    run_performance_test()