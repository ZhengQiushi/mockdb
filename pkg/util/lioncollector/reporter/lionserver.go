package lionreporter

import (
	"context"
	"log"
	"net"

	pb "github.com/pingcap/tidb/pkg/util/lioncollector/reporter/pb"

	"google.golang.org/grpc"
)

type SQLInfoServer struct {
	pb.UnimplementedSQLInfoServiceServer
}

func (s *SQLInfoServer) SendSQLInfo(ctx context.Context, req *pb.SQLInfoRequest) (*pb.SQLInfoResponse, error) {
	// 打印接收到的 SQL 信息
	log.Printf("Received SQL: %v\n", req)
	// 处理 SQL 信息逻辑

	// 这里只是简单地返回成功
	return &pb.SQLInfoResponse{Success: true}, nil
}

func (s *SQLInfoServer) Setup(grpcAddress string) error {
	// 创建监听器
	listen, err := net.Listen("tcp", grpcAddress)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	// 创建 gRPC 服务器
	server := grpc.NewServer()

	// 注册服务
	pb.RegisterSQLInfoServiceServer(server, &SQLInfoServer{})

	// 启动服务器
	log.Printf("Server started on :%v", grpcAddress)
	if err := server.Serve(listen); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
	return nil
}
