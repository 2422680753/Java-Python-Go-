package com.riskcontrol.gateway.service;

import com.riskcontrol.gateway.model.RiskResult;
import com.riskcontrol.gateway.model.Transaction;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
public class TransactionService {

    private static final Logger logger = LoggerFactory.getLogger(TransactionService.class);

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private FallbackService fallbackService;

    private static final String TRANSACTION_TOPIC = "transactions";
    private static final String RISK_RESULT_PREFIX = "risk:result:";
    private static final String BLACKLIST_PREFIX = "blacklist:";
    private static final String WHITELIST_PREFIX = "whitelist:";
    private static final String LARGE_TXN_WHITELIST_PREFIX = "whitelist:large-txn:";

    @CircuitBreaker(name = "transaction-gateway", fallbackMethod = "processTransactionFallback")
    @RateLimiter(name = "transaction-rate")
    @TimeLimiter(name = "transaction-gateway")
    @Retryable(value = {Exception.class}, maxAttempts = 3, backoff = @Backoff(delay = 100, multiplier = 2))
    public RiskResult processTransaction(Transaction transaction) {
        transaction.setTransactionTime(LocalDateTime.now());
        transaction.setCreateTime(LocalDateTime.now());

        RiskResult quickCheckResult = quickCheck(transaction);
        if (quickCheckResult != null) {
            saveRiskResult(quickCheckResult);
            return quickCheckResult;
        }

        try {
            kafkaTemplate.send(TRANSACTION_TOPIC, transaction.getTransactionId(), transaction)
                    .addCallback(
                            success -> logger.debug("交易发送成功: transactionId={}", transaction.getTransactionId()),
                            failure -> logger.error("交易发送失败: transactionId={}, error={}", 
                                    transaction.getTransactionId(), failure.getMessage())
                    );
        } catch (Exception e) {
            logger.warn("Kafka发送异常，尝试降级处理: transactionId={}", transaction.getTransactionId());
            return fallbackService.kafkaSendFallback(transaction, e);
        }

        RiskResult initialResult = RiskResult.review(
                transaction.getTransactionId(),
                0.3,
                "交易正在进行风控评估",
                new HashMap<>()
        );
        saveRiskResult(initialResult);

        return initialResult;
    }

    public RiskResult processTransactionFallback(Transaction transaction, Exception ex) {
        return fallbackService.processTransactionFallback(transaction, ex);
    }

    private RiskResult quickCheck(Transaction transaction) {
        Map<String, Object> factors = new HashMap<>();

        RiskResult largeTxnWhitelistResult = checkLargeTransactionWhitelist(transaction);
        if (largeTxnWhitelistResult != null) {
            return largeTxnWhitelistResult;
        }

        String blacklistKey = BLACKLIST_PREFIX + transaction.getUserId();
        if (Boolean.TRUE.equals(redisTemplate.hasKey(blacklistKey))) {
            factors.put("reason", "用户在黑名单中");
            return RiskResult.reject(transaction.getTransactionId(), 1.0, "用户已被加入黑名单", factors);
        }

        if (transaction.getIpAddress() != null) {
            String ipBlacklistKey = BLACKLIST_PREFIX + "ip:" + transaction.getIpAddress();
            if (Boolean.TRUE.equals(redisTemplate.hasKey(ipBlacklistKey))) {
                factors.put("reason", "IP地址在黑名单中");
                return RiskResult.reject(transaction.getTransactionId(), 1.0, "IP地址已被加入黑名单", factors);
            }
        }

        String whitelistKey = WHITELIST_PREFIX + transaction.getUserId();
        if (Boolean.TRUE.equals(redisTemplate.hasKey(whitelistKey))) {
            factors.put("reason", "用户在白名单中");
            return RiskResult.approve(transaction.getTransactionId(), 0.0, factors);
        }

        return null;
    }

    private RiskResult checkLargeTransactionWhitelist(Transaction transaction) {
        if (transaction.getAmount() == null) {
            return null;
        }

        double amount = transaction.getAmount().doubleValue();
        
        if (amount < 100000) {
            return null;
        }

        Map<String, Object> factors = new HashMap<>();
        factors.put("largeTransaction", true);
        factors.put("amount", amount);

        String userKey = LARGE_TXN_WHITELIST_PREFIX + "user:" + transaction.getUserId();
        Object userWhitelist = redisTemplate.opsForValue().get(userKey);
        if (userWhitelist != null) {
            factors.put("reason", "用户在大额交易白名单中");
            logger.info("大额交易白名单用户: userId={}, amount={}", transaction.getUserId(), amount);
            return RiskResult.approve(transaction.getTransactionId(), 0.05, factors);
        }

        String accountKey = LARGE_TXN_WHITELIST_PREFIX + "account:" + transaction.getAccountId();
        Object accountWhitelist = redisTemplate.opsForValue().get(accountKey);
        if (accountWhitelist != null) {
            factors.put("reason", "账户在大额交易白名单中");
            logger.info("大额交易白名单账户: accountId={}, amount={}", transaction.getAccountId(), amount);
            return RiskResult.approve(transaction.getTransactionId(), 0.05, factors);
        }

        return null;
    }

    public RiskResult getTransactionResult(String transactionId) {
        String key = RISK_RESULT_PREFIX + transactionId;
        Object result = redisTemplate.opsForValue().get(key);
        if (result instanceof RiskResult) {
            return (RiskResult) result;
        }

        RiskResult cachedResult = fallbackService.getCachedResult(transactionId);
        if (cachedResult != null) {
            return cachedResult;
        }

        return null;
    }

    public void saveRiskResult(RiskResult result) {
        String key = RISK_RESULT_PREFIX + result.getTransactionId();
        redisTemplate.opsForValue().set(key, result, 24, TimeUnit.HOURS);
    }

    public void addToLargeTransactionWhitelist(String type, String id, 
            double maxAmount, int maxDailyTransactions, long expireSeconds) {
        String key = LARGE_TXN_WHITELIST_PREFIX + type + ":" + id;
        
        Map<String, Object> entry = new HashMap<>();
        entry.put("type", type);
        entry.put("id", id);
        entry.put("maxAmount", maxAmount);
        entry.put("maxDailyTransactions", maxDailyTransactions);
        entry.put("createTime", LocalDateTime.now().toString());
        
        redisTemplate.opsForValue().set(key, entry);
        
        if (expireSeconds > 0) {
            redisTemplate.expire(key, expireSeconds, TimeUnit.SECONDS);
        }
        
        logger.info("添加到大额交易白名单: type={}, id={}", type, id);
    }

    public void removeFromLargeTransactionWhitelist(String type, String id) {
        String key = LARGE_TXN_WHITELIST_PREFIX + type + ":" + id;
        redisTemplate.delete(key);
        logger.info("从大额交易白名单移除: type={}, id={}", type, id);
    }
}
