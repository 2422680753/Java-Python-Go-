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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	logger       *zap.Logger
	redisClient  *redis.Client
	config       *Config
	cacheStats   *CacheStatistics
	statsMutex   sync.RWMutex
	startTime    time.Time
)

type Config struct {
	ServerPort      string
	RedisAddr       string
	RedisPass       string
	RedisDB         int
	DefaultTTL      time.Duration
	MaxCacheSize    int64
	CleanupInterval time.Duration
}

type CacheStatistics struct {
	TotalHits        int64
	TotalMisses      int64
	TotalSets        int64
	TotalDeletes     int64
	CacheSize        int64
	KeyCount         int64
	Uptime           time.Duration
	HitRate          float64
}

type BlacklistEntry struct {
	ID          string    `json:"id"`
	Type        string    `json:"type"`
	Reason      string    `json:"reason"`
	ExpireAt    time.Time `json:"expireAt"`
	CreatedAt   time.Time `json:"createdAt"`
	CreatedBy   string    `json:"createdBy"`
}

type WhitelistEntry struct {
	ID          string    `json:"id"`
	Type        string    `json:"type"`
	Reason      string    `json:"reason"`
	ExpireAt    time.Time `json:"expireAt"`
	CreatedAt   time.Time `json:"createdAt"`
	CreatedBy   string    `json:"createdBy"`
}

type CacheRequest struct {
	Key        string        `json:"key" binding:"required"`
	Value      interface{}   `json:"value" binding:"required"`
	TTL        time.Duration `json:"ttl"`
}

func init() {
	startTime = time.Now()
	cacheStats = &CacheStatistics{}
	
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
	ttl, _ := time.ParseDuration(getEnv("DEFAULT_TTL", "1h"))
	cleanup, _ := time.ParseDuration(getEnv("CLEANUP_INTERVAL", "10m"))
	
	return &Config{
		ServerPort:      getEnv("SERVER_PORT", "8086"),
		RedisAddr:       getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPass:       getEnv("REDIS_PASS", ""),
		RedisDB:         getEnvAsInt("REDIS_DB", 6),
		DefaultTTL:      ttl,
		MaxCacheSize:    int64(getEnvAsInt("MAX_CACHE_SIZE", 100000)),
		CleanupInterval: cleanup,
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

func initRedis() {
	redisClient = redis.NewClient(&redis.Options{
		Addr:     config.RedisAddr,
		Password: config.RedisPass,
		DB:       config.RedisDB,
		PoolSize: 20,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		logger.Fatal("无法连接到Redis", zap.Error(err))
	}
	logger.Info("Redis连接成功")
}

func setupRoutes(r *gin.Engine) {
	cache := r.Group("/api/cache")
	{
		cache.GET("/:key", handleCacheGet)
		cache.POST("", handleCacheSet)
		cache.DELETE("/:key", handleCacheDelete)
		cache.DELETE("/pattern/:pattern", handleCacheDeletePattern)
		cache.GET("/exists/:key", handleCacheExists)
		cache.POST("/expire/:key", handleCacheExpire)
	}
	
	blacklist := r.Group("/api/blacklist")
	{
		blacklist.POST("", handleAddToBlacklist)
		blacklist.DELETE("/:type/:id", handleRemoveFromBlacklist)
		blacklist.GET("/:type/:id", handleCheckBlacklist)
		blacklist.GET("", handleListBlacklist)
	}
	
	whitelist := r.Group("/api/whitelist")
	{
		whitelist.POST("", handleAddToWhitelist)
		whitelist.DELETE("/:type/:id", handleRemoveFromWhitelist)
		whitelist.GET("/:type/:id", handleCheckWhitelist)
		whitelist.GET("", handleListWhitelist)
	}
	
	r.GET("/api/stats", handleGetStats)
	r.GET("/health", handleHealth)
}

func handleCacheGet(c *gin.Context) {
	key := c.Param("key")
	
	ctx := context.Background()
	value, err := redisClient.Get(ctx, key).Result()
	
	statsMutex.Lock()
	defer statsMutex.Unlock()
	
	if err == redis.Nil {
		cacheStats.TotalMisses++
		c.JSON(http.StatusNotFound, gin.H{
			"error": "key not found",
			"key":   key,
		})
		return
	} else if err != nil {
		logger.Error("获取缓存失败", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	
	cacheStats.TotalHits++
	
	var result interface{}
	if json.Unmarshal([]byte(value), &result) == nil {
		c.JSON(http.StatusOK, gin.H{
			"key":   key,
			"value": result,
		})
	} else {
		c.JSON(http.StatusOK, gin.H{
			"key":   key,
			"value": value,
		})
	}
}

func handleCacheSet(c *gin.Context) {
	var req CacheRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	
	ctx := context.Background()
	ttl := config.DefaultTTL
	if req.TTL > 0 {
		ttl = req.TTL
	}
	
	var valueStr string
	if str, ok := req.Value.(string); ok {
		valueStr = str
	} else {
		jsonBytes, err := json.Marshal(req.Value)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid value"})
			return
		}
		valueStr = string(jsonBytes)
	}
	
	err := redisClient.Set(ctx, req.Key, valueStr, ttl).Err()
	if err != nil {
		logger.Error("设置缓存失败", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	statsMutex.Lock()
	cacheStats.TotalSets++
	statsMutex.Unlock()
	
	c.JSON(http.StatusOK, gin.H{
		"status": "OK",
		"key":    req.Key,
		"ttl":    ttl.String(),
	})
}

func handleCacheDelete(c *gin.Context) {
	key := c.Param("key")
	
	ctx := context.Background()
	result, err := redisClient.Del(ctx, key).Result()
	if err != nil {
		logger.Error("删除缓存失败", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	statsMutex.Lock()
	if result > 0 {
		cacheStats.TotalDeletes++
	}
	statsMutex.Unlock()
	
	c.JSON(http.StatusOK, gin.H{
		"status":  "OK",
		"deleted": result,
	})
}

func handleCacheDeletePattern(c *gin.Context) {
	pattern := c.Param("pattern")
	
	ctx := context.Background()
	keys, err := redisClient.Keys(ctx, pattern).Result()
	if err != nil {
		logger.Error("查找keys失败", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	if len(keys) == 0 {
		c.JSON(http.StatusOK, gin.H{
			"status":  "OK",
			"deleted": 0,
		})
		return
	}
	
	result, err := redisClient.Del(ctx, keys...).Result()
	if err != nil {
		logger.Error("批量删除缓存失败", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	statsMutex.Lock()
	cacheStats.TotalDeletes += result
	statsMutex.Unlock()
	
	c.JSON(http.StatusOK, gin.H{
		"status":  "OK",
		"deleted": result,
	})
}

func handleCacheExists(c *gin.Context) {
	key := c.Param("key")
	
	ctx := context.Background()
	exists, err := redisClient.Exists(ctx, key).Result()
	if err != nil {
		logger.Error("检查缓存存在失败", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	c.JSON(http.StatusOK, gin.H{
		"key":    key,
		"exists": exists > 0,
	})
}

func handleCacheExpire(c *gin.Context) {
	key := c.Param("key")
	
	var req struct {
		TTL time.Duration `json:"ttl" binding:"required"`
	}
	
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	
	ctx := context.Background()
	err := redisClient.Expire(ctx, key, req.TTL).Err()
	if err != nil {
		logger.Error("设置过期时间失败", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	c.JSON(http.StatusOK, gin.H{
		"status": "OK",
		"key":    key,
		"ttl":    req.TTL.String(),
	})
}

func handleAddToBlacklist(c *gin.Context) {
	var entry BlacklistEntry
	if err := c.ShouldBindJSON(&entry); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	
	if entry.ID == "" || entry.Type == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id and type are required"})
		return
	}
	
	entry.CreatedAt = time.Now()
	if entry.ExpireAt.IsZero() {
		entry.ExpireAt = time.Now().Add(30 * 24 * time.Hour)
	}
	
	key := fmt.Sprintf("blacklist:%s:%s", entry.Type, entry.ID)
	data, _ := json.Marshal(entry)
	
	ttl := time.Until(entry.ExpireAt)
	if ttl < 0 {
		ttl = 0
	}
	
	ctx := context.Background()
	err := redisClient.Set(ctx, key, data, ttl).Err()
	if err != nil {
		logger.Error("添加黑名单失败", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	logger.Info("添加到黑名单",
		zap.String("type", entry.Type),
		zap.String("id", entry.ID),
		zap.String("reason", entry.Reason),
	)
	
	c.JSON(http.StatusOK, gin.H{
		"status":  "OK",
		"message": "added to blacklist",
		"entry":   entry,
	})
}

func handleRemoveFromBlacklist(c *gin.Context) {
	entryType := c.Param("type")
	id := c.Param("id")
	
	key := fmt.Sprintf("blacklist:%s:%s", entryType, id)
	
	ctx := context.Background()
	result, err := redisClient.Del(ctx, key).Result()
	if err != nil {
		logger.Error("删除黑名单失败", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	logger.Info("从黑名单移除",
		zap.String("type", entryType),
		zap.String("id", id),
	)
	
	c.JSON(http.StatusOK, gin.H{
		"status":  "OK",
		"deleted": result,
	})
}

func handleCheckBlacklist(c *gin.Context) {
	entryType := c.Param("type")
	id := c.Param("id")
	
	key := fmt.Sprintf("blacklist:%s:%s", entryType, id)
	
	ctx := context.Background()
	data, err := redisClient.Get(ctx, key).Result()
	if err == redis.Nil {
		c.JSON(http.StatusOK, gin.H{
			"in_blacklist": false,
		})
		return
	} else if err != nil {
		logger.Error("检查黑名单失败", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	var entry BlacklistEntry
	json.Unmarshal([]byte(data), &entry)
	
	c.JSON(http.StatusOK, gin.H{
		"in_blacklist": true,
		"entry":        entry,
	})
}

func handleListBlacklist(c *gin.Context) {
	ctx := context.Background()
	keys, err := redisClient.Keys(ctx, "blacklist:*").Result()
	if err != nil {
		logger.Error("获取黑名单列表失败", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	entries := make([]BlacklistEntry, 0, len(keys))
	for _, key := range keys {
		data, err := redisClient.Get(ctx, key).Result()
		if err != nil {
			continue
		}
		
		var entry BlacklistEntry
		if json.Unmarshal([]byte(data), &entry) == nil {
			entries = append(entries, entry)
		}
	}
	
	c.JSON(http.StatusOK, gin.H{
		"total":   len(entries),
		"entries": entries,
	})
}

func handleAddToWhitelist(c *gin.Context) {
	var entry WhitelistEntry
	if err := c.ShouldBindJSON(&entry); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	
	if entry.ID == "" || entry.Type == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id and type are required"})
		return
	}
	
	entry.CreatedAt = time.Now()
	if entry.ExpireAt.IsZero() {
		entry.ExpireAt = time.Now().Add(90 * 24 * time.Hour)
	}
	
	key := fmt.Sprintf("whitelist:%s:%s", entry.Type, entry.ID)
	data, _ := json.Marshal(entry)
	
	ttl := time.Until(entry.ExpireAt)
	if ttl < 0 {
		ttl = 0
	}
	
	ctx := context.Background()
	err := redisClient.Set(ctx, key, data, ttl).Err()
	if err != nil {
		logger.Error("添加白名单失败", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	logger.Info("添加到白名单",
		zap.String("type", entry.Type),
		zap.String("id", entry.ID),
		zap.String("reason", entry.Reason),
	)
	
	c.JSON(http.StatusOK, gin.H{
		"status":  "OK",
		"message": "added to whitelist",
		"entry":   entry,
	})
}

func handleRemoveFromWhitelist(c *gin.Context) {
	entryType := c.Param("type")
	id := c.Param("id")
	
	key := fmt.Sprintf("whitelist:%s:%s", entryType, id)
	
	ctx := context.Background()
	result, err := redisClient.Del(ctx, key).Result()
	if err != nil {
		logger.Error("删除白名单失败", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	logger.Info("从白名单移除",
		zap.String("type", entryType),
		zap.String("id", id),
	)
	
	c.JSON(http.StatusOK, gin.H{
		"status":  "OK",
		"deleted": result,
	})
}

func handleCheckWhitelist(c *gin.Context) {
	entryType := c.Param("type")
	id := c.Param("id")
	
	key := fmt.Sprintf("whitelist:%s:%s", entryType, id)
	
	ctx := context.Background()
	data, err := redisClient.Get(ctx, key).Result()
	if err == redis.Nil {
		c.JSON(http.StatusOK, gin.H{
			"in_whitelist": false,
		})
		return
	} else if err != nil {
		logger.Error("检查白名单失败", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	var entry WhitelistEntry
	json.Unmarshal([]byte(data), &entry)
	
	c.JSON(http.StatusOK, gin.H{
		"in_whitelist": true,
		"entry":        entry,
	})
}

func handleListWhitelist(c *gin.Context) {
	ctx := context.Background()
	keys, err := redisClient.Keys(ctx, "whitelist:*").Result()
	if err != nil {
		logger.Error("获取白名单列表失败", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	entries := make([]WhitelistEntry, 0, len(keys))
	for _, key := range keys {
		data, err := redisClient.Get(ctx, key).Result()
		if err != nil {
			continue
		}
		
		var entry WhitelistEntry
		if json.Unmarshal([]byte(data), &entry) == nil {
			entries = append(entries, entry)
		}
	}
	
	c.JSON(http.StatusOK, gin.H{
		"total":   len(entries),
		"entries": entries,
	})
}

func handleGetStats(c *gin.Context) {
	statsMutex.RLock()
	defer statsMutex.RUnlock()
	
	ctx := context.Background()
	info, _ := redisClient.Info(ctx, "memory").Result()
	
	var keyCount int64
	keys, _ := redisClient.Keys(ctx, "*").Result()
	keyCount = int64(len(keys))
	
	cacheStats.Uptime = time.Since(startTime)
	cacheStats.KeyCount = keyCount
	
	if cacheStats.TotalHits+cacheStats.TotalMisses > 0 {
		cacheStats.HitRate = float64(cacheStats.TotalHits) / float64(cacheStats.TotalHits+cacheStats.TotalMisses)
	}
	
	c.JSON(http.StatusOK, gin.H{
		"statistics": cacheStats,
		"redis_info": info,
	})
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

func startCleanupRoutine() {
	ticker := time.NewTicker(config.CleanupInterval)
	defer ticker.Stop()
	
	for range ticker.C {
		logger.Debug("执行缓存清理任务")
	}
}

func main() {
	defer logger.Sync()
	
	config = loadConfig()
	initRedis()
	
	go startCleanupRoutine()
	
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	
	setupRoutes(r)
	
	server := &http.Server{
		Addr:    ":" + config.ServerPort,
		Handler: r,
	}
	
	go func() {
		logger.Info("缓存管理服务启动", zap.String("port", config.ServerPort))
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
	
	redisClient.Close()
	
	logger.Info("缓存管理服务已关闭")
}
