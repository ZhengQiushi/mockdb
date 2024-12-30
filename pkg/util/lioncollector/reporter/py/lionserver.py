import logging
import grpc
from concurrent import futures
import sql_info_pb2
import sql_info_pb2_grpc
from core.analyze.graph import Graph  # 导入Graph类
import threading
import time

class SQLInfoServicer(sql_info_pb2_grpc.SQLInfoServiceServicer):
    def __init__(self, graph):
        """
        初始化服务类。
        :param graph: Graph对象，用于存储和更新图结构
        """
        self.graph = graph
        # 启动定时保存任务
        self.start_auto_save(interval=60)  # 60秒保存一次

    def SendSQLInfo(self, request, context):
        """
        处理接收到的SQL信息，更新图结构。
        :param request: SQLInfoRequest对象，包含SQL信息
        :param context: gRPC上下文
        :return: SQLInfoResponse对象，表示处理结果
        """
        logging.info(f"Received SQL: {request}")
        self.graph.add_transaction(request.region_ids)
        # 返回成功响应
        return sql_info_pb2.SQLInfoResponse(success=True)

    def start_auto_save(self, interval):
        """
        启动定时保存任务。
        :param interval: 保存间隔时间（秒）
        """
        def save_graph_periodically():
            while True:
                # 生成文件名，包含当前时间戳
                filename = f"history/graph_{int(time.time())}.pkl"
                # 保存Graph对象
                self.graph.save(filename)
                logging.info(f"Graph saved to {filename}")
                # 等待指定间隔时间
                time.sleep(interval)

        # 启动一个后台线程执行定时保存任务
        threading.Thread(target=save_graph_periodically, daemon=True).start()

def serve(grpc_address, weight=10, theta=1, top_hot_threshold=0):
    """
    启动gRPC服务器。
    :param grpc_address: gRPC服务器地址
    :param weight: 不同region之间的边权系数，默认为10
    :param theta: 相同region之间的边权系数，默认为1
    :param top_hot_threshold: 点权阈值，默认为0
    """
    graph = Graph(weight=weight, theta=theta, top_hot_threshold=top_hot_threshold)
    # 创建gRPC服务器
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # 注册服务
    sql_info_pb2_grpc.add_SQLInfoServiceServicer_to_server(SQLInfoServicer(graph), server)
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
    serve("[::]:50051", weight=weight, theta=theta, top_hot_threshold=top_hot_threshold)