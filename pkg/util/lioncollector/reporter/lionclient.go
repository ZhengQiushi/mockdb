package lionreporter

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	pb "github.com/pingcap/tidb/pkg/util/lioncollector/reporter/pb"
	"golang.org/x/exp/rand"

	"google.golang.org/grpc"
)

// SQLInfo represents the structure of collected SQL data.
type SQLInfo struct {
	SQLText     string
	TxnID       int
	Keys        []int32
	RegionIDs   []int32
	EnqueueTime time.Time // 记录任务进入队列的时间
}

func (info *SQLInfo) AddKey(key int32) {
	info.Keys = append(info.Keys, key)
	info.RegionIDs = append(info.RegionIDs, key/100000)
}

func (info *SQLInfo) SetTxnID(txnId int32) {
	info.TxnID = int(txnId)
}

func (info *SQLInfo) SetSQLText(sqlText string) {
	info.SQLText = sqlText
}

// Cache is a thread-safe in-memory queue for SQL data.
type Cache struct {
	queues []chan SQLInfo
	mu     sync.Mutex
}

func NewCache(queueCount int) *Cache {
	queues := make([]chan SQLInfo, queueCount)
	for i := range queues {
		queues[i] = make(chan SQLInfo, 1000) // 设置缓冲区大小
	}
	return &Cache{
		queues: queues,
	}
}

// WriteToCache appends SQLInfo to the cache.
func (c *Cache) WriteToCache(info SQLInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(info.RegionIDs) == 0 {
		return
	}
	randomIndex := rand.Intn(len(c.queues)) // 生成一个 0 到 len(c.queues)-1 的随机数
	info.EnqueueTime = time.Now()           // 记录任务进入队列的时间
	c.queues[randomIndex] <- info
}

// CacheReceiver handles incoming SQLInfo and writes it to the cache.
type CacheReceiver struct {
	cache *Cache
}

func NewCacheReceiver(cache *Cache) *CacheReceiver {
	return &CacheReceiver{
		cache: cache,
	}
}

func (cr *CacheReceiver) RegisterSQLInfo(info SQLInfo) {
	cr.cache.WriteToCache(info)
}

// RpcSender retrieves data from the cache and sends it via gRPC.
type RpcSender struct {
	cache           *Cache
	grpcClient      pb.SQLInfoServiceClient
	grpcAddress     string
	queueCount      int
	workersPerQueue int
	delayMetrics    []*DelayMetrics // 每个队列的延迟统计
}

func NewRpcSender(cache *Cache, grpcAddress string, queueCount, workersPerQueue int) *RpcSender {
	conn, err := grpc.Dial(grpcAddress, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to dial gRPC server: %v", err)
	}
	delayMetrics := make([]*DelayMetrics, queueCount)
	for i := range delayMetrics {
		delayMetrics[i] = NewDelayMetrics()
	}
	return &RpcSender{
		cache:           cache,
		grpcClient:      pb.NewSQLInfoServiceClient(conn),
		grpcAddress:     grpcAddress,
		queueCount:      queueCount,
		workersPerQueue: workersPerQueue,
		delayMetrics:    delayMetrics,
	}
}

func (rs *RpcSender) Start(ctx context.Context) {
	for q := 0; q < rs.queueCount; q++ {
		for w := 0; w < rs.workersPerQueue; w++ {
			go rs.worker(ctx, q)
		}
	}
	// 启动定时统计任务
	go rs.startMetricsReporter(ctx)
}

func (rs *RpcSender) worker(ctx context.Context, queueIndex int) {
	for {
		select {
		case <-ctx.Done():
			return
		case info, ok := <-rs.cache.queues[queueIndex]:
			if !ok {
				continue
			}
			// 计算延迟时间
			delay := time.Since(info.EnqueueTime)
			rs.delayMetrics[queueIndex].Record(delay)
			rs.sendToRemote(ctx, info)
		}
	}
}

func (rs *RpcSender) sendToRemote(ctx context.Context, info SQLInfo) {
	request := &pb.SQLInfoRequest{
		SqlText:   info.SQLText,
		TxnId:     int32(info.TxnID),
		Keys:      info.Keys,
		RegionIds: info.RegionIDs,
	}
	_, err := rs.grpcClient.SendSQLInfo(ctx, request)
	if err != nil {
		log.Printf("Failed to send SQLInfo: %+v, error: %v\n", info, err)
	}
}

// 启动定时统计任务
func (rs *RpcSender) startMetricsReporter(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rs.reportMetrics()
		}
	}
}

// 打印统计信息
func (rs *RpcSender) reportMetrics() {
	log.Printf("lion client reportMetrics:")
	for q := 0; q < rs.queueCount; q++ {
		backlog := len(rs.cache.queues[q])
		p80, p95, p99 := rs.delayMetrics[q].GetPercentiles()
		log.Printf("Queue %d: Backlog=%d, P80=%v, P95=%v, P99=%v\n", q, backlog, p80, p95, p99)
		rs.delayMetrics[q].Reset() // 重置统计
	}
}

// DelayMetrics 用于统计延迟分位数
type DelayMetrics struct {
	mu     sync.Mutex
	delays []time.Duration
}

func NewDelayMetrics() *DelayMetrics {
	return &DelayMetrics{
		delays: make([]time.Duration, 0),
	}
}

func (dm *DelayMetrics) Record(delay time.Duration) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.delays = append(dm.delays, delay)
}

func (dm *DelayMetrics) GetPercentiles() (p80, p95, p99 time.Duration) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if len(dm.delays) == 0 {
		return 0, 0, 0
	}
	// 排序并计算分位数
	sortDurations(dm.delays)
	p80 = dm.delays[int(float64(len(dm.delays))*0.8)]
	p95 = dm.delays[int(float64(len(dm.delays))*0.95)]
	p99 = dm.delays[int(float64(len(dm.delays))*0.99)]
	return
}

func (dm *DelayMetrics) Reset() {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.delays = dm.delays[:0] // 清空延迟数据
}

// 辅助函数：对延迟数据进行排序
func sortDurations(durations []time.Duration) {
	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})
}

// Collector encapsulates CacheReceiver and RpcSender.
type Collector struct {
	cache    *Cache
	receiver *CacheReceiver
	sender   *RpcSender
	ctx      context.Context
	cancel   context.CancelFunc
}

func (c *Collector) Setup(grpcAddress string, queueCount, workersPerQueue int) error {
	c.cache = NewCache(queueCount)
	c.receiver = NewCacheReceiver(c.cache)

	ctx, cancel := context.WithCancel(context.Background())
	c.ctx = ctx
	c.cancel = cancel

	c.sender = NewRpcSender(c.cache, grpcAddress, queueCount, workersPerQueue)
	go c.sender.Start(ctx)

	log.Printf("Successfully initialized NewCollector: %v\n", grpcAddress)
	return nil
}

// Setup method to initialize the gRPC connection and return the collector
func Setup(grpcAddress string, queueCount, workersPerQueue int) (*Collector, error) {
	collector := &Collector{}
	err := collector.Setup(grpcAddress, queueCount, workersPerQueue)
	return collector, err
}

func (c *Collector) RegisterSQLInfo(info SQLInfo) {
	if c.receiver != nil {
		c.receiver.RegisterSQLInfo(info)
	}
}

func (c *Collector) Close() {
	c.cancel()
	for _, q := range c.cache.queues {
		close(q)
	}
}
