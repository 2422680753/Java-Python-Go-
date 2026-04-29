package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

var (
	logger           *zap.Logger
	redisClient      *redis.Client
	config           *Config
	stats            *ConsumerStats
	statsMutex       sync.RWMutex
	startTime        time.Time
	userActivities   map[string]*UserActivity
	activityMutex    sync.RWMutex
	scalingConfig    *ScalingConfig
	scalingConfigMutex sync.RWMutex
)

type Config struct {
	ServerPort         string
	RedisAddr          string
	RedisPass          string
	RedisDB            int
	KafkaBrokers       []string
	Topics             []string
	GroupID            string
	ConsumerConcurrency int
	MaxPollRecords     int
	FetchMinBytes      int
	FetchMaxWait       int
}

type ScalingConfig struct {
	ScaleUpLagThreshold   int64
	ScaleDownLagThreshold int64
	MinConsumers          int
	MaxConsumers          int
	CurrentConsumers      int
	CooldownPeriodMs      int64
	LastScaleUpTime       int64
	LastScaleDownTime     int64
}

type TransactionEvent struct {
	TransactionID   string    `json:"transactionId"`
	UserID          string    `json:"userId"`
	AccountID       string    `json:"accountId"`
	Amount          float64   `json:"amount"`
	TransactionType string    `json:"transactionType"`
	Recipient       string    `json:"recipient"`
	DeviceID        string    `json:"deviceId"`
	IPAddress       string    `json:"ipAddress"`
	Location        string    `json:"location"`
	TransactionTime time.Time `json:"transactionTime"`
}

type UserActivity struct {
	UserID           string
	TransactionCount int64
	TotalAmount      float64
	LastTransaction  time.Time
	UniqueDevices    map[string]bool
	UniqueIPs        map[string]bool
	UniqueLocations  map[string]bool
	TransactionTypes map[string]int64
}

type ConsumerStats struct {
	MessagesProcessed   int64
	MessagesFailed      int64
	ActiveConsumers     int
	LastMessageTime     time.Time
	Uptime              time.Duration
	TotalLag            int64
	BatchesProcessed    int64
	ProcessingLatencyMs float64
}

type BatchProcessor struct {
	batchSize     int
	maxWait       time.Duration
	messages      []kafka.Message
	mu            sync.Mutex
	flushTimer    *time.Timer
	processFunc   func([]kafka.Message)
}

func init() {
	startTime = time.Now()
	stats = &ConsumerStats{}
	userActivities = make(map[string]*UserActivity)
	scalingConfig = &ScalingConfig{
		ScaleUpLagThreshold:   5000,
		ScaleDownLagThreshold: 500,
		MinConsumers:          2,
		MaxConsumers:          16,
		CurrentConsumers:      4,
		CooldownPeriodMs:      60000,
	}

	var err error
	config := zap.NewProductionConfig()
	config.EncoderConfig.TimeKey = "timestamp"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	logger, err = config.Build()
	if err != nil {
		log.Fatalf("无法初始化日志: %v", err)
	}
}

func loadConfig() *Config {
	return &Config{
		ServerPort:         getEnv("SERVER_PORT", "8085"),
		RedisAddr:          getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPass:          getEnv("REDIS_PASS", ""),
		RedisDB:            getEnvAsInt("REDIS_DB", 5),
		KafkaBrokers:       getEnvAsSlice("KAFKA_BROKERS", []string{"localhost:9092"}),
		Topics:             getEnvAsSlice("KAFKA_TOPICS", []string{"transactions", "final-decisions"}),
		GroupID:            getEnv("KAFKA_GROUP_ID", "stream-processing"),
		ConsumerConcurrency: getEnvAsInt("CONSUMER_CONCURRENCY", 4),
		MaxPollRecords:     getEnvAsInt("MAX_POLL_RECORDS", 500),
		FetchMinBytes:      getEnvAsInt("FETCH_MIN_BYTES", 10240),
		FetchMaxWait:       getEnvAsInt("FETCH_MAX_WAIT", 500),
	}
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	if value, exists := os.LookupEnv(key); exists {
		var intValue int
		fmt.Sscanf(value, "%d", &intValue)
		return intValue
	}
	return defaultValue
}

func getEnvAsSlice(key string, defaultValue []string) []string {
	if value, exists := os.LookupEnv(key); exists {
		var result []string
		json.Unmarshal([]byte(value), &result)
		return result
	}
	return defaultValue
}

func initRedis() {
	redisClient = redis.NewClient(&redis.Options{
		Addr:     config.RedisAddr,
		Password: config.RedisPass,
		DB:       config.RedisDB,
		PoolSize: 32,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		logger.Fatal("无法连接到Redis", zap.Error(err))
	}
	logger.Info("Redis连接成功")
}

func createConsumer(topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  config.KafkaBrokers,
		GroupID:  config.GroupID,
		Topic:    topic,
		MinBytes: config.FetchMinBytes,
		MaxBytes: 50 * 1024 * 1024,
		MaxWait:  time.Millisecond * time.Duration(config.FetchMaxWait),
		QueueCapacity: config.MaxPollRecords * 2,
	})
}

func NewBatchProcessor(batchSize int, maxWait time.Duration, processFunc func([]kafka.Message)) *BatchProcessor {
	bp := &BatchProcessor{
		batchSize:   batchSize,
		maxWait:     maxWait,
		messages:    make([]kafka.Message, 0, batchSize),
		processFunc: processFunc,
	}
	bp.flushTimer = time.AfterFunc(maxWait, bp.flush)
	return bp
}

func (bp *BatchProcessor) Add(msg kafka.Message) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	bp.messages = append(bp.messages, msg)

	if len(bp.messages) >= bp.batchSize {
		bp.flushLocked()
	} else if len(bp.messages) == 1 {
		bp.flushTimer.Reset(bp.maxWait)
	}
}

func (bp *BatchProcessor) flush() {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.flushLocked()
}

func (bp *BatchProcessor) flushLocked() {
	if len(bp.messages) == 0 {
		return
	}

	messagesToProcess := make([]kafka.Message, len(bp.messages))
	copy(messagesToProcess, bp.messages)
	bp.messages = bp.messages[:0]

	go bp.processFunc(messagesToProcess)
}

func (bp *BatchProcessor) Close() {
	bp.flushTimer.Stop()
	bp.flush()
}

func processBatchMessages(messages []kafka.Message) {
	if len(messages) == 0 {
		return
	}

	startTime := time.Now()
	ctx := context.Background()
	
	logger.Debug("开始批量处理消息", zap.Int("count", len(messages)))

	events := make([]*TransactionEvent, 0, len(messages))
	userUpdates := make(map[string]*UserActivity)

	for _, msg := range messages {
		var event TransactionEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			logger.Error("解析交易事件失败", zap.Error(err))
			atomic.AddInt64(&stats.MessagesFailed, 1)
			continue
		}
		events = append(events, &event)

		userID := event.UserID
		activity, exists := userUpdates[userID]
		if !exists {
			activityMutex.RLock()
			existingActivity, cacheExists := userActivities[userID]
			activityMutex.RUnlock()

			if cacheExists {
				activity = existingActivity
			} else {
				activity = &UserActivity{
					UserID:           userID,
					UniqueDevices:    make(map[string]bool),
					UniqueIPs:        make(map[string]bool),
					UniqueLocations:  make(map[string]bool),
					TransactionTypes: make(map[string]int64),
				}
			}
			userUpdates[userID] = activity
		}

		activity.TransactionCount++
		activity.TotalAmount += event.Amount
		activity.LastTransaction = event.TransactionTime
		activity.TransactionTypes[event.TransactionType]++

		if event.DeviceID != "" {
			activity.UniqueDevices[event.DeviceID] = true
		}
		if event.IPAddress != "" {
			activity.UniqueIPs[event.IPAddress] = true
		}
		if event.Location != "" {
			activity.UniqueLocations[event.Location] = true
		}
	}

	pipe := redisClient.Pipeline()
	for userID, activity := range userUpdates {
		userKey := fmt.Sprintf("stream:user:%s", userID)
		
		pipe.HSet(ctx, userKey, map[string]interface{}{
			"transaction_count": activity.TransactionCount,
			"total_amount":      activity.TotalAmount,
			"last_transaction":  activity.LastTransaction.Format(time.RFC3339),
			"unique_devices":    len(activity.UniqueDevices),
			"unique_ips":        len(activity.UniqueIPs),
			"unique_locations":  len(activity.UniqueLocations),
		})
		pipe.Expire(ctx, userKey, 24*time.Hour)

		detectAnomaliesBatch(ctx, activity)

		activityMutex.Lock()
		userActivities[userID] = activity
		activityMutex.Unlock()
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		logger.Error("批量更新Redis失败", zap.Error(err))
	}

	atomic.AddInt64(&stats.MessagesProcessed, int64(len(events)))
	atomic.AddInt64(&stats.BatchesProcessed, 1)

	statsMutex.Lock()
	stats.LastMessageTime = time.Now()
	statsMutex.Unlock()

	latency := float64(time.Since(startTime).Milliseconds()) / float64(len(events))
	statsMutex.Lock()
	stats.ProcessingLatencyMs = (stats.ProcessingLatencyMs*float64(stats.BatchesProcessed-1) + latency) / float64(stats.BatchesProcessed)
	statsMutex.Unlock()

	logger.Debug("批量处理完成", 
		zap.Int("count", len(events)), 
		zap.Duration("duration", time.Since(startTime)),
		zap.Float64("avgLatencyMs", latency),
	)
}

func detectAnomaliesBatch(ctx context.Context, activity *UserActivity) {
	anomalies := make([]string, 0)

	if activity.TotalAmount > 100000 {
		anomalies = append(anomalies, "大额交易")
	}

	if len(activity.UniqueDevices) > 3 {
		anomalies = append(anomalies, "多设备登录")
	}

	if len(activity.UniqueLocations) > 2 {
		anomalies = append(anomalies, "多位置交易")
	}

	oneHourAgo := time.Now().Add(-1 * time.Hour)
	if activity.LastTransaction.After(oneHourAgo) && activity.TransactionCount > 10 {
		anomalies = append(anomalies, "高频交易")
	}

	if len(anomalies) > 0 {
		anomalyKey := fmt.Sprintf("anomaly:batch:%s:%d", activity.UserID, time.Now().Unix())
		anomalyData := map[string]interface{}{
			"user_id":          activity.UserID,
			"anomalies":        anomalies,
			"transaction_count": activity.TransactionCount,
			"total_amount":     activity.TotalAmount,
			"unique_devices":   len(activity.UniqueDevices),
			"timestamp":        time.Now().Format(time.RFC3339),
		}

		jsonData, _ := json.Marshal(anomalyData)
		redisClient.Set(ctx, anomalyKey, jsonData, 7*24*time.Hour)

		logger.Warn("检测到异常行为",
			zap.String("userId", activity.UserID),
			zap.Strings("anomalies", anomalies),
		)
	}
}

func updateUserCache(ctx context.Context, activity *UserActivity) {
	userKey := fmt.Sprintf("stream:user:%s", activity.UserID)

	pipe := redisClient.Pipeline()
	pipe.HSet(ctx, userKey, map[string]interface{}{
		"transaction_count": activity.TransactionCount,
		"total_amount":      activity.TotalAmount,
		"last_transaction":  activity.LastTransaction.Format(time.RFC3339),
		"unique_devices":    len(activity.UniqueDevices),
		"unique_ips":        len(activity.UniqueIPs),
		"unique_locations":  len(activity.UniqueLocations),
	})
	pipe.Expire(ctx, userKey, 24*time.Hour)

	_, err := pipe.Exec(ctx)
	if err != nil {
		logger.Error("更新用户缓存失败", zap.Error(err))
	}
}

func detectAnomalies(ctx context.Context, event *TransactionEvent, activity *UserActivity) {
	anomalies := make([]string, 0)

	if event.Amount > 100000 {
		anomalies = append(anomalies, "大额交易")
	}

	if len(activity.UniqueDevices) > 3 {
		anomalies = append(anomalies, "多设备登录")
	}

	if len(activity.UniqueLocations) > 2 {
		anomalies = append(anomalies, "多位置交易")
	}

	oneHourAgo := time.Now().Add(-1 * time.Hour)
	if activity.LastTransaction.After(oneHourAgo) && activity.TransactionCount > 10 {
		anomalies = append(anomalies, "高频交易")
	}

	if len(anomalies) > 0 {
		anomalyKey := fmt.Sprintf("anomaly:%s:%s", event.UserID, event.TransactionID)
		anomalyData := map[string]interface{}{
			"user_id":          event.UserID,
			"transaction_id":   event.TransactionID,
			"anomalies":        anomalies,
			"amount":           event.Amount,
			"transaction_count": activity.TransactionCount,
			"timestamp":        time.Now().Format(time.RFC3339),
		}

		jsonData, _ := json.Marshal(anomalyData)
		redisClient.Set(ctx, anomalyKey, jsonData, 7*24*time.Hour)

		logger.Warn("检测到异常行为",
			zap.String("userId", event.UserID),
			zap.String("transactionId", event.TransactionID),
			zap.Strings("anomalies", anomalies),
		)
	}
}

func consumeTopicWithBatch(ctx context.Context, topic string, batchProcessor *BatchProcessor) error {
	reader := createConsumer(topic)
	defer reader.Close()

	logger.Info("开始消费主题", zap.String("topic", topic))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msg, err := reader.FetchMessage(ctx)
			if err != nil {
				if err == context.Canceled {
					return nil
				}
				atomic.AddInt64(&stats.MessagesFailed, 1)
				logger.Error("获取消息失败", zap.Error(err))
				continue
			}

			batchProcessor.Add(msg)

			if err := reader.CommitMessages(ctx, msg); err != nil {
				logger.Error("提交偏移量失败", zap.Error(err))
			}
		}
	}
}

func consumeTopic(ctx context.Context, topic string) error {
	reader := createConsumer(topic)
	defer reader.Close()

	logger.Info("开始消费主题", zap.String("topic", topic))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msg, err := reader.FetchMessage(ctx)
			if err != nil {
				if err == context.Canceled {
					return nil
				}
				atomic.AddInt64(&stats.MessagesFailed, 1)
				logger.Error("获取消息失败", zap.Error(err))
				continue
			}

			if topic == "transactions" {
				var event TransactionEvent
				if err := json.Unmarshal(msg.Value, &event); err != nil {
					logger.Error("解析交易事件失败", zap.Error(err))
					atomic.AddInt64(&stats.MessagesFailed, 1)
					continue
				}
				processTransactionEvent(ctx, &event)
			} else {
				logger.Debug("处理其他主题消息", zap.String("topic", topic), zap.String("key", string(msg.Key)))
			}

			atomic.AddInt64(&stats.MessagesProcessed, 1)
			statsMutex.Lock()
			stats.LastMessageTime = time.Now()
			statsMutex.Unlock()

			if err := reader.CommitMessages(ctx, msg); err != nil {
				logger.Error("提交偏移量失败", zap.Error(err))
			}
		}
	}
}

func processTransactionEvent(ctx context.Context, event *TransactionEvent) {
	logger.Debug("处理交易事件", zap.String("transactionId", event.TransactionID))

	activityMutex.Lock()
	activity, exists := userActivities[event.UserID]
	if !exists {
		activity = &UserActivity{
			UserID:           event.UserID,
			UniqueDevices:    make(map[string]bool),
			UniqueIPs:        make(map[string]bool),
			UniqueLocations:  make(map[string]bool),
			TransactionTypes: make(map[string]int64),
		}
		userActivities[event.UserID] = activity
	}

	activity.TransactionCount++
	activity.TotalAmount += event.Amount
	activity.LastTransaction = event.TransactionTime
	activity.TransactionTypes[event.TransactionType]++

	if event.DeviceID != "" {
		activity.UniqueDevices[event.DeviceID] = true
	}
	if event.IPAddress != "" {
		activity.UniqueIPs[event.IPAddress] = true
	}
	if event.Location != "" {
		activity.UniqueLocations[event.Location] = true
	}
	activityMutex.Unlock()

	updateUserCache(ctx, activity)

	detectAnomalies(ctx, event, activity)
}

func setupRoutes(r *gin.Engine) {
	r.GET("/api/stats", handleGetStats)
	r.GET("/api/users/:id/activity", handleGetUserActivity)
	r.GET("/api/anomalies", handleGetAnomalies)
	r.GET("/api/consumer/status", handleGetConsumerStatus)
	r.POST("/api/scaling/config", handleUpdateScalingConfig)
	r.GET("/api/scaling/status", handleGetScalingStatus)
	r.GET("/health", handleHealth)
}

func handleGetStats(c *gin.Context) {
	statsMutex.RLock()
	defer statsMutex.RUnlock()

	stats.Uptime = time.Since(startTime)

	c.JSON(200, stats)
}

func handleGetUserActivity(c *gin.Context) {
	userID := c.Param("id")

	activityMutex.RLock()
	activity, exists := userActivities[userID]
	activityMutex.RUnlock()

	if !exists {
		userKey := fmt.Sprintf("stream:user:%s", userID)
		data, err := redisClient.HGetAll(c.Request.Context(), userKey).Result()
		if err != nil || len(data) == 0 {
			c.JSON(404, gin.H{"error": "用户活动记录未找到"})
			return
		}
		c.JSON(200, data)
		return
	}

	c.JSON(200, gin.H{
		"user_id":           activity.UserID,
		"transaction_count": activity.TransactionCount,
		"total_amount":      activity.TotalAmount,
		"last_transaction":  activity.LastTransaction,
		"unique_devices":    len(activity.UniqueDevices),
		"unique_ips":        len(activity.UniqueIPs),
		"unique_locations":  len(activity.UniqueLocations),
		"transaction_types": activity.TransactionTypes,
	})
}

func handleGetAnomalies(c *gin.Context) {
	ctx := c.Request.Context()

	keys, err := redisClient.Keys(ctx, "anomaly:*").Result()
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	anomalies := make([]map[string]interface{}, 0)
	for _, key := range keys {
		data, err := redisClient.Get(ctx, key).Result()
		if err == nil {
			var anomaly map[string]interface{}
			if json.Unmarshal([]byte(data), &anomaly) == nil {
				anomalies = append(anomalies, anomaly)
			}
		}
	}

	c.JSON(200, anomalies)
}

func handleGetConsumerStatus(c *gin.Context) {
	statsMutex.RLock()
	defer statsMutex.RUnlock()

	status := map[string]interface{}{
		"messagesProcessed":    stats.MessagesProcessed,
		"messagesFailed":       stats.MessagesFailed,
		"activeConsumers":      stats.ActiveConsumers,
		"lastMessageTime":      stats.LastMessageTime,
		"uptime":               time.Since(startTime).String(),
		"batchesProcessed":     stats.BatchesProcessed,
		"averageLatencyMs":     stats.ProcessingLatencyMs,
	}

	c.JSON(200, status)
}

func handleGetScalingStatus(c *gin.Context) {
	scalingConfigMutex.RLock()
	defer scalingConfigMutex.RUnlock()

	status := map[string]interface{}{
		"scaleUpLagThreshold":   scalingConfig.ScaleUpLagThreshold,
		"scaleDownLagThreshold": scalingConfig.ScaleDownLagThreshold,
		"minConsumers":          scalingConfig.MinConsumers,
		"maxConsumers":          scalingConfig.MaxConsumers,
		"currentConsumers":      scalingConfig.CurrentConsumers,
		"cooldownPeriodMs":      scalingConfig.CooldownPeriodMs,
	}

	c.JSON(200, status)
}

func handleUpdateScalingConfig(c *gin.Context) {
	var req map[string]interface{}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	scalingConfigMutex.Lock()
	defer scalingConfigMutex.Unlock()

	if v, ok := req["scaleUpLagThreshold"].(float64); ok {
		scalingConfig.ScaleUpLagThreshold = int64(v)
	}
	if v, ok := req["scaleDownLagThreshold"].(float64); ok {
		scalingConfig.ScaleDownLagThreshold = int64(v)
	}
	if v, ok := req["minConsumers"].(float64); ok {
		scalingConfig.MinConsumers = int(v)
	}
	if v, ok := req["maxConsumers"].(float64); ok {
		scalingConfig.MaxConsumers = int(v)
	}
	if v, ok := req["cooldownPeriodMs"].(float64); ok {
		scalingConfig.CooldownPeriodMs = int64(v)
	}

	c.JSON(200, gin.H{"success": true, "config": scalingConfig})
}

func handleHealth(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	redisStatus := "UP"
	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		redisStatus = "DOWN"
	}

	c.JSON(200, gin.H{
		"status":        "UP",
		"redis":         redisStatus,
		"uptime":        time.Since(startTime).String(),
		"activeConsumers": stats.ActiveConsumers,
	})
}

func main() {
	defer logger.Sync()

	config = loadConfig()
	initRedis()

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	setupRoutes(r)

	server := &struct {
		Addr    string
		Handler *gin.Engine
	}{
		Addr:    ":" + config.ServerPort,
		Handler: r,
	}

	go func() {
		logger.Info("流处理服务启动", zap.String("port", config.ServerPort))
		if err := http.ListenAndServe(server.Addr, server.Handler); err != nil {
			logger.Fatal("服务启动失败", zap.Error(err))
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	batchProcessor := NewBatchProcessor(
		config.MaxPollRecords,
		100*time.Millisecond,
		processBatchMessages,
	)
	defer batchProcessor.Close()

	for _, topic := range config.Topics {
		topic := topic
		g.Go(func() error {
			statsMutex.Lock()
			stats.ActiveConsumers++
			statsMutex.Unlock()

			defer func() {
				statsMutex.Lock()
				stats.ActiveConsumers--
				statsMutex.Unlock()
			}()

			return consumeTopicWithBatch(ctx, topic, batchProcessor)
		})
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-quit:
		logger.Info("收到停止信号，正在关闭服务...")
		cancel()
	case <-ctx.Done():
	}

	if err := g.Wait(); err != nil && err != context.Canceled {
		logger.Error("消费者组错误", zap.Error(err))
	}

	redisClient.Close()

	logger.Info("流处理服务已关闭")
}
