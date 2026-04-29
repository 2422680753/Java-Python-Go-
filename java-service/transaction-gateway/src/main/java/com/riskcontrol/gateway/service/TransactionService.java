package com.riskcontrol.gateway.service;

import com.riskcontrol.gateway.model.RiskResult;
import com.riskcontrol.gateway.model.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class TransactionService {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    private static final String TRANSACTION_TOPIC = "transactions";
    private static final String RISK_RESULT_PREFIX = "risk:result:";
    private static final String BLACKLIST_PREFIX = "blacklist:";
    private static final String WHITELIST_PREFIX = "whitelist:";

    public RiskResult processTransaction(Transaction transaction) {
        transaction.setTransactionTime(LocalDateTime.now());
        transaction.setCreateTime(LocalDateTime.now());

        RiskResult quickCheckResult = quickCheck(transaction);
        if (quickCheckResult != null) {
            saveRiskResult(quickCheckResult);
            return quickCheckResult;
        }

        kafkaTemplate.send(TRANSACTION_TOPIC, transaction.getTransactionId(), transaction);

        RiskResult initialResult = RiskResult.review(
                transaction.getTransactionId(),
                0.5,
                "交易正在进行风控评估",
                new HashMap<>()
        );
        saveRiskResult(initialResult);

        return initialResult;
    }

    private RiskResult quickCheck(Transaction transaction) {
        Map<String, Object> factors = new HashMap<>();

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

    public RiskResult getTransactionResult(String transactionId) {
        String key = RISK_RESULT_PREFIX + transactionId;
        Object result = redisTemplate.opsForValue().get(key);
        if (result instanceof RiskResult) {
            return (RiskResult) result;
        }
        return null;
    }

    public void saveRiskResult(RiskResult result) {
        String key = RISK_RESULT_PREFIX + result.getTransactionId();
        redisTemplate.opsForValue().set(key, result, 24, TimeUnit.HOURS);
    }
}
