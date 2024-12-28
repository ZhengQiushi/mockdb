import logging
import grpc
from concurrent import futures
import sql_info_pb2
import sql_info_pb2_grpc

class SQLInfoServicer(sql_info_pb2_grpc.SQLInfoServiceServicer):
    def SendSQLInfo(self, request, context):
        # 打印接收到的 SQL 信息
        logging.info(f"Received SQL: {request}")
        # 处理 SQL 信息逻辑

        # 这里只是简单地返回成功
        return sql_info_pb2.SQLInfoResponse(success=True)

def serve(grpc_address):
    # 创建 gRPC 服务器
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # 注册服务
    sql_info_pb2_grpc.add_SQLInfoServiceServicer_to_server(SQLInfoServicer(), server)

    # 启动服务器
    server.add_insecure_port(grpc_address)
    logging.info(f"Server started on {grpc_address}")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(level=logging.INFO)

    # 启动 gRPC 服务器
    serve("[::]:50051")