package lioncollector

import (
	lionreporter "github.com/pingcap/tidb/pkg/util/lioncollector/reporter"
)

var (
	globalLionCollector lionreporter.Collector
)

func Setup() {
	// 设置 gRPC 地址和参数
	grpcAddress := "10.77.110.148:50051"
	// 初始化 Collector
	globalLionCollector.Setup(grpcAddress, 10, 2)
}

func Close() {
	globalLionCollector.Close()
}

func RegisterSQLInfo(info lionreporter.SQLInfo) {
	globalLionCollector.RegisterSQLInfo(info)
}
