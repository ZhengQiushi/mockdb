package lioncollector

import (
	"time"

	lionreporter "github.com/pingcap/tidb/pkg/util/lioncollector/reporter"
)

var (
	globalLionCollector lionreporter.Collector
)

func Setup() {
	// 设置 gRPC 地址和参数
	grpcAddress := "localhost:50051"
	interval := 1 * time.Second
	batchSize := 10
	// 初始化 Collector
	globalLionCollector.Setup(grpcAddress, interval, batchSize)
}

func Close() {
	globalLionCollector.Close()
}

func RegisterSQLInfo(info lionreporter.SQLInfo) {
	globalLionCollector.RegisterSQLInfo(info)
}
