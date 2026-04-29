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
)

var (
	logger      *zap.Logger
	redisClient *redis.Client
	kafkaWriter *kafka.Writer
	config      *Config
)

type Config struct {
	ServerPort   string
	RedisAddr    string
	RedisPass    string
	RedisDB      int
	KafkaBrokers []string
}

type Transaction struct {
	TransactionID   string    `json:"transactionId"`
	UserID          string    `json:"userId"`
	AccountID       string    `json:"accountId"`
	Amount          float64   `json:"amount"`
	TransactionType string    `json:"transactionType"`
	Recipient       string    `json:"recipient"`
	DeviceID        string    `json:"deviceId,omitempty"`
	IPAddress       string    `json:"ipAddress,omitempty"`
	Location        string    `json:"location,omitempty"`
	TransactionTime time.Time `json:"transactionTime"`
	CreateTime      time.Time `json:"createTime"`
}

type ProcessedTransaction struct {
	Transaction    *Transaction
	ProcessTime    time.Time
	ProcessingTime int64
	Status         string
}

type Statistics struct {
	TotalTransactions     int64         `json:"totalTransactions"`
	ProcessedTransactions int64         `json:"processedTransactions"`
	AvgProcessingTime     int64         `json:"avgProcessingTime"`
	Throughput            float64       `json:"throughput"`
	Uptime                time.Duration `json:"uptime"`
}

var (
	stats          *Statistics
	statsMutex     sync.RWMutex
	startTime      time.Time
	processingTimes []int64
)

func init() {
	startTime = time.Now()
	stats = &Statistics{}
	processingTimes = make([]int64, 0, 10000)
	
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
		ServerPort:   getEnv("SERVER_PORT", "8084"),
		RedisAddr:    getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPass:    getEnv("REDIS_PASS", ""),
		RedisDB:      getEnvAsInt("REDIS_DB", 4),
		KafkaBrokers: getEnvAsSlice("KAFKA_BROKERS", []string{"localhost:9092"}),
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

func initKafka() {
	kafkaWriter = &kafka.Writer{
		Addr:         kafka.TCP(config.KafkaBrokers...),
		Topic:        "processed-transactions",
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        true,
	}
	logger.Info("Kafka生产者初始化成功")
}

func processTransaction(ctx context.Context, txn *Transaction) *ProcessedTransaction {
	start := time.Now()
	
	logger.Debug("开始处理交易", zap.String("transactionId", txn.TransactionID))
	
	userKey := fmt.Sprintf("user:%s:stats", txn.UserID)
	txnCount, _ := redisClient.HIncrBy(ctx, userKey, "transaction_count", 1).Result()
	
	dailyKey := fmt.Sprintf("user:%s:daily:%s", txn.UserID, time.Now().Format("2006-01-02"))
	dailyAmount, _ := redisClient.IncrByFloat(ctx, dailyKey, txn.Amount).Result()
	redisClient.Expire(ctx, dailyKey, 24*time.Hour)
	
	if txn.DeviceID != "" {
		deviceKey := fmt.Sprintf("user:%s:devices", txn.UserID)
		redisClient.SAdd(ctx, deviceKey, txn.DeviceID)
		redisClient.Expire(ctx, deviceKey, 90*24*time.Hour)
	}
	
	if txn.Location != "" {
		locationKey := fmt.Sprintf("user:%s:locations", txn.UserID)
		redisClient.SAdd(ctx, locationKey, txn.Location)
		redisClient.Expire(ctx, locationKey, 90*24*time.Hour)
	}
	
	processingTime := time.Since(start).Microseconds()
	
	statsMutex.Lock()
	stats.TotalTransactions++
	stats.ProcessedTransactions++
	processingTimes = append(processingTimes, processingTime)
	
	if len(processingTimes) > 1000 {
		processingTimes = processingTimes[len(processingTimes)-1000:]
	}
	
	var sum int64
	for _, t := range processingTimes {
		sum += t
	}
	stats.AvgProcessingTime = sum / int64(len(processingTimes))
	stats.Throughput = float64(stats.ProcessedTransactions) / time.Since(startTime).Seconds()
	stats.Uptime = time.Since(startTime)
	statsMutex.Unlock()
	
	processedTxn := &ProcessedTransaction{
		Transaction:    txn,
		ProcessTime:    time.Now(),
		ProcessingTime: processingTime,
		Status:         "PROCESSED",
	}
	
	logger.Info("交易处理完成", 
		zap.String("transactionId", txn.TransactionID),
		zap.Int64("processingTimeUs", processingTime),
		zap.Int64("userTxnCount", txnCount),
		zap.Float64("dailyAmount", dailyAmount),
	)
	
	return processedTxn
}

func sendToKafka(ctx context.Context, processedTxn *ProcessedTransaction) {
	if kafkaWriter == nil {
		return
	}
	
	data, err := json.Marshal(processedTxn)
	if err != nil {
		logger.Error("序列化处理结果失败", zap.Error(err))
		return
	}
	
	err = kafkaWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(processedTxn.Transaction.TransactionID),
		Value: data,
	})
	
	if err != nil {
		logger.Error("发送消息到Kafka失败", zap.Error(err))
	}
}

func setupRoutes(r *gin.Engine) {
	r.POST("/api/transactions/process", handleProcessTransaction)
	r.GET("/api/transactions/:id", handleGetTransaction)
	r.GET("/api/stats", handleGetStats)
	r.GET("/health", handleHealth)
}

func handleProcessTransaction(c *gin.Context) {
	var txn Transaction
	if err := c.ShouldBindJSON(&txn); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	
	if txn.TransactionID == "" {
		txn.TransactionID = fmt.Sprintf("txn_%d", time.Now().UnixNano())
	}
	if txn.TransactionTime.IsZero() {
		txn.TransactionTime = time.Now()
	}
	if txn.CreateTime.IsZero() {
		txn.CreateTime = time.Now()
	}
	
	processedTxn := processTransaction(c.Request.Context(), &txn)
	
	cacheKey := fmt.Sprintf("processed:%s", txn.TransactionID)
	cacheData, _ := json.Marshal(processedTxn)
	redisClient.Set(c.Request.Context(), cacheKey, cacheData, 24*time.Hour)
	
	sendToKafka(c.Request.Context(), processedTxn)
	
	c.JSON(http.StatusOK, processedTxn)
}

func handleGetTransaction(c *gin.Context) {
	transactionID := c.Param("id")
	cacheKey := fmt.Sprintf("processed:%s", transactionID)
	
	data, err := redisClient.Get(c.Request.Context(), cacheKey).Result()
	if err == redis.Nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "交易记录未找到"})
		return
	} else if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	var processedTxn ProcessedTransaction
	json.Unmarshal([]byte(data), &processedTxn)
	
	c.JSON(http.StatusOK, processedTxn)
}

func handleGetStats(c *gin.Context) {
	statsMutex.RLock()
	defer statsMutex.RUnlock()
	
	c.JSON(http.StatusOK, stats)
}

func handleHealth(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	redisStatus := "UP"
	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		redisStatus = "DOWN"
	}
	
	c.JSON(http.StatusOK, gin.H{
		"status": "UP",
		"redis":  redisStatus,
		"uptime": time.Since(startTime).String(),
	})
}

func main() {
	defer logger.Sync()
	
	config = loadConfig()
	initRedis()
	initKafka()
	
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	
	setupRoutes(r)
	
	server := &http.Server{
		Addr:    ":" + config.ServerPort,
		Handler: r,
	}
	
	go func() {
		logger.Info("数据处理服务启动", zap.String("port", config.ServerPort))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("服务启动失败", zap.Error(err))
		}
	}()
	
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	
	logger.Info("正在关闭服务...")
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	if err := server.Shutdown(ctx); err != nil {
		logger.Fatal("服务强制关闭", zap.Error(err))
	}
	
	if kafkaWriter != nil {
		kafkaWriter.Close()
	}
	
	redisClient.Close()
	
	logger.Info("服务已关闭")
}
