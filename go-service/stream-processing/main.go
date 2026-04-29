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
	logger      *zap.Logger
	redisClient *redis.Client
	config      *Config
)

type Config struct {
	ServerPort   string
	RedisAddr    string
	RedisPass    string
	RedisDB      int
	KafkaBrokers []string
	Topics       []string
	GroupID      string
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
	UserID             string
	TransactionCount   int
	TotalAmount        float64
	LastTransaction    time.Time
	UniqueDevices      map[string]bool
	UniqueIPs          map[string]bool
	UniqueLocations    map[string]bool
	TransactionTypes   map[string]int
}

type StreamStats struct {
	MessagesProcessed  int64
	ErrorsEncountered  int64
	ActiveConsumers    int
	LastMessageTime    time.Time
	Uptime             time.Duration
}

var (
	stats          *StreamStats
	statsMutex     sync.RWMutex
	startTime      time.Time
	userActivities map[string]*UserActivity
	activityMutex  sync.RWMutex
)

func init() {
	startTime = time.Now()
	stats = &StreamStats{}
	userActivities = make(map[string]*UserActivity)
	
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
		ServerPort:   getEnv("SERVER_PORT", "8085"),
		RedisAddr:    getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPass:    getEnv("REDIS_PASS", ""),
		RedisDB:      getEnvAsInt("REDIS_DB", 5),
		KafkaBrokers: getEnvAsSlice("KAFKA_BROKERS", []string{"localhost:9092"}),
		Topics:       getEnvAsSlice("KAFKA_TOPICS", []string{"transactions", "final-decisions"}),
		GroupID:      getEnv("KAFKA_GROUP_ID", "stream-processing"),
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
		MinBytes: 10e3,
		MaxBytes: 10e6,
		MaxWait:  1 * time.Second,
	})
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
			TransactionTypes: make(map[string]int),
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
	
	statsMutex.Lock()
	stats.MessagesProcessed++
	stats.LastMessageTime = time.Now()
	statsMutex.Unlock()
}

func updateUserCache(ctx context.Context, activity *UserActivity) {
	userKey := fmt.Sprintf("stream:user:%s", activity.UserID)
	
	pipe := redisClient.Pipeline()
	pipe.HSet(ctx, userKey, "transaction_count", activity.TransactionCount)
	pipe.HSet(ctx, userKey, "total_amount", activity.TotalAmount)
	pipe.HSet(ctx, userKey, "last_transaction", activity.LastTransaction.Format(time.RFC3339))
	pipe.HSet(ctx, userKey, "unique_devices", len(activity.UniqueDevices))
	pipe.HSet(ctx, userKey, "unique_ips", len(activity.UniqueIPs))
	pipe.HSet(ctx, userKey, "unique_locations", len(activity.UniqueLocations))
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
				statsMutex.Lock()
				stats.ErrorsEncountered++
				statsMutex.Unlock()
				logger.Error("获取消息失败", zap.Error(err))
				continue
			}
			
			if topic == "transactions" {
				var event TransactionEvent
				if err := json.Unmarshal(msg.Value, &event); err != nil {
					logger.Error("解析交易事件失败", zap.Error(err))
					continue
				}
				processTransactionEvent(ctx, &event)
			} else {
				logger.Debug("处理其他主题消息", zap.String("topic", topic), zap.String("key", string(msg.Key)))
			}
			
			if err := reader.CommitMessages(ctx, msg); err != nil {
				logger.Error("提交偏移量失败", zap.Error(err))
			}
		}
	}
}

func setupRoutes(r *gin.Engine) {
	r.GET("/api/stats", handleGetStats)
	r.GET("/api/users/:id/activity", handleGetUserActivity)
	r.GET("/api/anomalies", handleGetAnomalies)
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

func handleHealth(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	redisStatus := "UP"
	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		redisStatus = "DOWN"
	}
	
	c.JSON(200, gin.H{
		"status":   "UP",
		"redis":    redisStatus,
		"uptime":   time.Since(startTime).String(),
		"consumers": stats.ActiveConsumers,
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
			
			return consumeTopic(ctx, topic)
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
