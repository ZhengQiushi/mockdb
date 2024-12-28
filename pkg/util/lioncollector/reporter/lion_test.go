package lionreporter

import (
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	// 替换为你的模块名称和路径
)

// 测试 Collector 和 Server 的集成
func TestCollectorAndServer(t *testing.T) {
	// 设置 gRPC 地址和参数
	grpcAddress := "localhost:50051"
	interval := 1 * time.Second
	batchSize := 10

	// 初始化 Collector
	collector, err := Setup(grpcAddress, interval, batchSize)
	if err != nil {
		log.Fatalf("Failed to setup collector: %v", err)
	}
	defer collector.Close() // 确保连接在程序结束时关闭

	s := &SQLInfoServer{}
	go func() {
		if err := s.Setup(grpcAddress); err != nil {
			log.Fatalf("Failed to setup server: %v", err)
		}
	}()

	// 注册一些 SQL 信息
	collector.RegisterSQLInfo(
		SQLInfo{
			SQLText:   "SELECT * FROM users",
			TxnID:     1,
			RegionIDs: []int32{1, 2, 3},
		})
	collector.RegisterSQLInfo(
		SQLInfo{
			SQLText:   "INSERT INTO orders VALUES (1, 'item')",
			TxnID:     2,
			RegionIDs: []int32{4, 5},
		})

	// 等待一段时间以确保客户端能够接收到数据
	time.Sleep(5 * time.Second)
}

func TestCollectorAndNoServer(t *testing.T) {
	// 设置 gRPC 地址和参数
	grpcAddress := "localhost:50051"
	interval := 1 * time.Second
	batchSize := 10

	// 初始化 Collector
	collector, err := Setup(grpcAddress, interval, batchSize)
	if err != nil {
		log.Fatalf("Failed to setup collector: %v", err)
	}
	defer collector.Close() // 确保连接在程序结束时关闭

	// s := &SQLInfoServer{}
	// go func() {
	// 	if err := s.Setup(grpcAddress); err != nil {
	// 		log.Fatalf("Failed to setup server: %v", err)
	// 	}
	// }()

	// 注册一些 SQL 信息
	collector.RegisterSQLInfo(
		SQLInfo{
			SQLText:   "SELECT * FROM users",
			TxnID:     1,
			RegionIDs: []int32{1, 2, 3},
		})
	collector.RegisterSQLInfo(
		SQLInfo{
			SQLText:   "INSERT INTO orders VALUES (1, 'item')",
			TxnID:     2,
			RegionIDs: []int32{4, 5},
		})

	// 等待一段时间以确保客户端能够接收到数据
	time.Sleep(5 * time.Second)
}

func TestNoCollectorAndServer(t *testing.T) {
	// 设置 gRPC 地址和参数
	grpcAddress := "localhost:50051"
	// interval := 1 * time.Second
	// batchSize := 10

	// // 初始化 Collector
	// collector, grpcConn, err := Setup(grpcAddress, interval, batchSize)
	// if err != nil {
	// 	log.Fatalf("Failed to setup collector: %v", err)
	// }
	// defer grpcConn.Close() // 确保连接在程序结束时关闭

	s := &SQLInfoServer{}
	go func() {
		if err := s.Setup(grpcAddress); err != nil {
			log.Fatalf("Failed to setup server: %v", err)
		}
	}()

	// // 注册一些 SQL 信息
	// collector.RegisterSQLInfo("SELECT * FROM users", 1, []int32{1, 2, 3})
	// collector.RegisterSQLInfo("INSERT INTO orders VALUES (1, 'item')", 2, []int32{4, 5})

	// 等待一段时间以确保客户端能够接收到数据
	time.Sleep(500 * time.Second)
}

// 测试 Collector 和 Client 的集成（压测）
func TestCollectorAndClientWithConcurrency(t *testing.T) {
	// 设置 gRPC 地址和参数
	grpcAddress := "localhost:50051"
	interval := 1 * time.Second
	batchSize := 10

	// 初始化 Collector
	collector, err := Setup(grpcAddress, interval, batchSize)
	if err != nil {
		log.Fatalf("Failed to setup collector: %v", err)
	}
	defer collector.Close() // 确保连接在程序结束时关闭

	// 启动 gRPC 服务器
	s := &SQLInfoServer{}
	go func() {
		if err := s.Setup(grpcAddress); err != nil {
			log.Fatalf("Failed to setup server: %v", err)
		}
	}()

	// 统计延时
	var totalLatency int64
	var totalCalls int64

	// 并发数
	concurrency := 200
	// 压测持续时间
	testDuration := 60 * time.Second

	// 创建 WaitGroup
	var wg sync.WaitGroup
	wg.Add(concurrency)

	// 压测开始时间
	startTime := time.Now()

	// 启动并发任务
	for i := 0; i < concurrency; i++ {
		go func(workerID int32) {
			defer wg.Done()
			sqlText := "SELECT * FROM table"
			txnID := int(workerID)
			regionIDs := []int32{workerID, workerID + 1, workerID + 2}

			for {
				// 检查是否超过压测持续时间
				if time.Since(startTime) > testDuration {
					break
				}

				// 记录开始时间
				start := time.Now()

				// 注册 SQL 信息
				collector.RegisterSQLInfo(SQLInfo{
					SQLText:   sqlText,
					TxnID:     txnID,
					RegionIDs: regionIDs,
				})

				// 记录结束时间并计算延时
				latency := time.Since(start).Nanoseconds()

				// 统计延时
				atomic.AddInt64(&totalLatency, latency)
				atomic.AddInt64(&totalCalls, 1)

				// 模拟每次调用的间隔
				time.Sleep(10 * time.Millisecond)
			}
		}(int32(i))
	}

	// 等待所有 Goroutines 完成
	wg.Wait()

	// 计算平均延时
	averageLatency := float64(totalLatency) / float64(totalCalls) / 1e6 // 转换为毫秒

	log.Printf("Total calls: %d, throughput: %.2f txn/s, Average latency: %.2f ms\n", totalCalls, float32(totalCalls*1.0/int64(testDuration.Seconds())), averageLatency)

	// 等待一段时间以确保客户端能够接收到所有数据
	time.Sleep(5 * time.Second)
}
