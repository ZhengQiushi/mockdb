package lionreporter

import (
	"context"
	"log"
	"sync"
	"time"

	pb "github.com/pingcap/tidb/pkg/util/lioncollector/reporter/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

// SQLInfo represents the structure of collected SQL data.
type SQLInfo struct {
	SQLText   string
	TxnID     int
	Keys      []int32
	RegionIDs []int32
}

func (info *SQLInfo) AddKey(key int32) {
	info.Keys = append(info.Keys, key)
	info.RegionIDs = append(info.RegionIDs, key/1000)
}

func (info *SQLInfo) SetTxnID(txnId int32) {
	info.TxnID = int(txnId)
}

func (info *SQLInfo) SetSQLText(sqlText string) {
	info.SQLText = sqlText
}

// Cache is a thread-safe in-memory queue for SQL data.
type Cache struct {
	data []SQLInfo
	mu   sync.RWMutex
}

func NewCache() *Cache {
	return &Cache{
		data: make([]SQLInfo, 0),
	}
}

// WriteToCache appends SQLInfo to the cache.
func (c *Cache) WriteToCache(info SQLInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data = append(c.data, info)
}

// ReadFromCache retrieves a batch of SQLInfo from the cache.
func (c *Cache) ReadFromCache(batchSize int) []SQLInfo {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.data) == 0 {
		return nil
	}

	if len(c.data) <= batchSize {
		batch := c.data
		c.data = nil
		return batch
	}

	batch := c.data[:batchSize]
	c.data = c.data[batchSize:]
	return batch
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
	cache       *Cache
	grpcConn    *grpc.ClientConn
	grpcClient  pb.SQLInfoServiceClient
	grpcAddress string
	mu          sync.Mutex
}

func NewRpcSender(cache *Cache, grpcConn *grpc.ClientConn, grpcAddress string) *RpcSender {
	return &RpcSender{
		cache:       cache,
		grpcConn:    grpcConn,
		grpcClient:  pb.NewSQLInfoServiceClient(grpcConn),
		grpcAddress: grpcAddress,
	}
}

// reconnect attempts to re-establish the gRPC connection.
func (rs *RpcSender) reconnect() error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.grpcConn != nil {
		rs.grpcConn.Close()
	}

	conn, err := grpc.Dial(rs.grpcAddress, grpc.WithInsecure())
	if err != nil {
		return err
	}

	rs.grpcConn = conn
	rs.grpcClient = pb.NewSQLInfoServiceClient(conn)
	return nil
}

// checkConnection checks if the gRPC connection is ready.
func (rs *RpcSender) checkConnection() bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.grpcConn == nil {
		return false
	}

	state := rs.grpcConn.GetState()
	return state == connectivity.Ready || state == connectivity.Idle
}

func (rs *RpcSender) Start(ctx context.Context, interval time.Duration, batchSize int) {
	retryInterval := time.Second // Initial retry interval
	for {
		select {
		case <-ctx.Done():
			log.Println("RpcSender stopped")
			return
		default:
			if !rs.checkConnection() {
				log.Println("gRPC connection is down, attempting to reconnect...")
				if err := rs.reconnect(); err != nil {
					log.Printf("Failed to reconnect: %v, retrying in %v...\n", err, retryInterval)
					time.Sleep(retryInterval)
					if retryInterval > 30*time.Second {
						retryInterval = 30 * time.Second // Cap the retry interval
					}
					continue
				}
				retryInterval = time.Second // Reset retry interval on successful reconnect
				log.Println("Successfully reconnected to gRPC server")
			}

			batch := rs.cache.ReadFromCache(batchSize)
			if len(batch) > 0 {
				rs.SendToRemote(batch)
			}
			time.Sleep(interval)
		}
	}
}

func (rs *RpcSender) SendToRemote(sqlInfos []SQLInfo) {
	for _, info := range sqlInfos {
		request := &pb.SQLInfoRequest{
			SqlText:   info.SQLText,
			TxnId:     int32(info.TxnID),
			Keys:      info.Keys,
			RegionIds: info.RegionIDs,
		}

		_, err := rs.grpcClient.SendSQLInfo(context.Background(), request)
		if err != nil {
			log.Printf("Failed to send SQLInfo: %+v, error: %v\n", info, err)
			continue
		}

		log.Printf("Successfully sent SQLInfo: %+v\n", info)
	}
}

// Collector encapsulates CacheReceiver and RpcSender.
type Collector struct {
	cache    *Cache
	receiver *CacheReceiver
	sender   *RpcSender
	grpcConn *grpc.ClientConn
}

func (c *Collector) Setup(grpcAddress string, interval time.Duration, batchSize int) error {

	c.cache = NewCache()
	c.receiver = NewCacheReceiver(c.cache)

	grpcConn, err := grpc.Dial(grpcAddress, grpc.WithInsecure())
	if err != nil {
		return err
	}

	c.sender = NewRpcSender(c.cache, grpcConn, grpcAddress)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		c.sender.Start(ctx, interval, batchSize)
	}()
	log.Printf("Successfully initialized NewCollector: %v\n", grpcAddress)

	c.grpcConn = grpcConn
	return nil
}

// Setup method to initialize the gRPC connection and return the collector
func Setup(grpcAddress string, interval time.Duration, batchSize int) (*Collector, error) {
	collector := &Collector{}
	err := collector.Setup(grpcAddress, interval, batchSize)
	return collector, err
}

func (c *Collector) RegisterSQLInfo(info SQLInfo) {
	if c.receiver != nil {
		c.receiver.RegisterSQLInfo(info)
	}
}

func (c *Collector) Close() {
	c.grpcConn.Close()
}
