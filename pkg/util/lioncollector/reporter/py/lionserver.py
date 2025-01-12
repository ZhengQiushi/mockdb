import logging
import grpc
from concurrent import futures
import sql_info_pb2
import sql_info_pb2_grpc
from core.analyze.graph import Graph  # 导入Graph类
import threading
import time
import queue
import os

class SQLInfoServicer(sql_info_pb2_grpc.SQLInfoServiceServicer):
    def __init__(self, graph, queue_count=10, workers_per_queue=2):
        """
        初始化服务类。
        :param graph: Graph对象，用于存储和更新图结构
        :param queue_count: 队列的数量
        :param workers_per_queue: 每个队列对应的线程数
        """
        self.graph = graph
        self.queue_count = queue_count
        self.workers_per_queue = workers_per_queue
        self.task_queues = [queue.Queue() for _ in range(self.queue_count)]
        # 启动定时保存任务
        self.start_auto_save(interval=60, max_saves=10)  # 60秒保存一次，最多保存10个文件
        # 启动工作线程池
        self.start_worker_pool()

    def SendSQLInfo(self, request, context):
        """
        处理接收到的SQL信息，将任务放入对应的队列中。
        :param request: SQLInfoRequest对象，包含SQL信息
        :param context: gRPC上下文
        :return: SQLInfoResponse对象，表示处理结果
        """
        logging.info(f"Received SQL: {request.region_ids}")
        # 计算哈希值，这里使用region_ids的哈希值
        hash_value = hash(tuple(request.region_ids)) % self.queue_count
        # 将任务放入对应的队列
        self.task_queues[hash_value].put(request.region_ids)
        # 返回成功响应
        return sql_info_pb2.SQLInfoResponse(success=True)

    def start_auto_save(self, interval, max_saves=10):
        """
        启动定时保存任务。
        :param interval: 保存间隔时间（秒）
        :param max_saves: 最大保存文件数量
        """
        def save_graph_periodically():
            saves = []  # 用于记录保存的文件名
            while True:
                # 生成文件名，包含当前时间戳
                timestamp = int(time.time())
                filename = f"history/graph_{timestamp}.pkl"
                # 保存Graph对象
                self.graph.save(filename)
                logging.info(f"Graph saved to {filename}")
                # 将文件名加入保存列表
                saves.append(filename)
                # 如果超过最大保存数量，删除最旧的文件
                if len(saves) > max_saves:
                    oldest_file = saves.pop(0)
                    os.remove(oldest_file)
                    logging.info(f"Removed old file: {oldest_file}")
                # 等待指定间隔时间
                time.sleep(interval)

        # 启动一个后台线程执行定时保存任务
        threading.Thread(target=save_graph_periodically, daemon=True).start()

    def start_worker_pool(self):
        """
        启动工作线程池，从队列中取出任务并执行。
        """
        def worker(thread_id):
            queue_index = thread_id % self.queue_count
            while True:
                # 从指定的队列中取出任务
                region_ids = self.task_queues[queue_index].get()
                if region_ids is None:
                    self.task_queues[queue_index].task_done()
                    break
                # 执行任务
                self.graph.add_transaction(region_ids)
                # 标记任务完成
                self.task_queues[queue_index].task_done()

        # 启动工作线程
        for i in range(self.queue_count * self.workers_per_queue):
            threading.Thread(target=worker, args=(i,), daemon=True).start()

def serve(grpc_address, weight=10, theta=1, top_hot_threshold=0, queue_count=10, workers_per_queue=2):
    """
    启动gRPC服务器。
    :param grpc_address: gRPC服务器地址
    :param weight: 不同region之间的边权系数，默认为10
    :param theta: 相同region之间的边权系数，默认为1
    :param top_hot_threshold: 点权阈值，默认为0
    :param queue_count: 队列的数量
    :param workers_per_queue: 每个队列对应的线程数
    """
    graph = Graph(weight=weight, theta=theta, top_hot_threshold=top_hot_threshold)
    # 创建gRPC服务器
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # 注册服务
    sql_info_pb2_grpc.add_SQLInfoServiceServicer_to_server(
        SQLInfoServicer(graph, queue_count, workers_per_queue), server)
    # 启动服务器
    server.add_insecure_port(grpc_address)
    logging.info(f"Server started on {grpc_address}")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(level=logging.INFO)
    # 配置图参数
    weight = 10  # 不同region之间的边权系数
    theta = 1  # 相同region之间的边权系数
    top_hot_threshold = 5  # 点权阈值，根据需要调整
    # 启动gRPC服务器
    serve("[::]:50051", weight=weight, theta=theta, top_hot_threshold=top_hot_threshold, queue_count=10, workers_per_queue=2)