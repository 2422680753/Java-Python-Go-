package com.riskcontrol.rules.service;

import com.alibaba.fastjson.JSON;
import com.riskcontrol.rules.model.TransactionFact;
import org.kie.api.KieBase;
import org.kie.api.runtime.KieSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class BatchProcessingService {

    private static final Logger logger = LoggerFactory.getLogger(BatchProcessingService.class);

    @Autowired
    private KieBase kieBase;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private DynamicScoringService dynamicScoringService;

    private static final String TRANSACTION_TOPIC = "transactions";
    private static final String RULES_RESULT_TOPIC = "rules-results";
    private static final String USER_TRANSACTION_PREFIX = "user:txn:";
    private static final String USER_DAILY_AMOUNT_PREFIX = "user:daily:";
    private static final String USER_TRUST_PREFIX = "user:trust:";

    private final AtomicLong batchProcessedCount = new AtomicLong(0);
    private final AtomicLong batchErrorCount = new AtomicLong(0);

    public List<Map<String, Object>> processBatch(List<String> messages) {
        List<Map<String, Object>> results = new ArrayList<>();
        List<TransactionFact> facts = new ArrayList<>();
        
        Map<String, Set<String>> devicesToAdd = new HashMap<>();
        Map<String, Set<String>> locationsToAdd = new HashMap<>();
        Map<String, Long> userTxnCounts = new HashMap<>();
        Map<String, BigDecimal> userDailyAmounts = new HashMap<>();

        for (String message : messages) {
            try {
                Map<String, Object> transactionMap = JSON.parseObject(message, Map.class);
                TransactionFact fact = convertToFact(transactionMap);
                facts.add(fact);
            } catch (Exception e) {
                logger.error("解析消息失败: {}", e.getMessage());
                batchErrorCount.incrementAndGet();
            }
        }

        enrichBatchData(facts);

        for (TransactionFact fact : facts) {
            try {
                KieSession kieSession = kieBase.newKieSession();
                kieSession.insert(fact);
                kieSession.fireAllRules();
                kieSession.dispose();

                double dynamicScore = dynamicScoringService.calculateDynamicScore(
                    fact.getUserId(), fact.getAmount(), true
                );
                
                String whitelistDecision = dynamicScoringService.checkLargeTransactionWhitelist(
                    fact.getUserId(), fact.getAmount()
                );
                
                if ("APPROVE".equals(whitelistDecision)) {
                    fact.setRuleDecision("APPROVE");
                    fact.setRuleRiskScore(Math.max(0.1, fact.getRuleRiskScore() * 0.5));
                    fact.setRuleReason("白名单用户，自动通过");
                } else {
                    double combinedScore = fact.getRuleRiskScore() * 0.7 + dynamicScore * 0.3;
                    fact.setRuleRiskScore(combinedScore);
                }

                Map<String, Object> result = buildRulesResult(fact);
                results.add(result);

                String userId = fact.getUserId();
                userTxnCounts.merge(userId, 1L, Long::sum);
                userDailyAmounts.merge(userId, fact.getAmount(), BigDecimal::add);
                
                if (fact.getDeviceId() != null) {
                    devicesToAdd.computeIfAbsent(userId, k -> new HashSet<>()).add(fact.getDeviceId());
                }
                if (fact.getLocation() != null) {
                    locationsToAdd.computeIfAbsent(userId, k -> new HashSet<>()).add(fact.getLocation());
                }

            } catch (Exception e) {
                logger.error("处理交易失败: transactionId={}, error={}", 
                    fact.getTransactionId(), e.getMessage());
                batchErrorCount.incrementAndGet();
            }
        }

        asyncUpdateBatchStats(userTxnCounts, userDailyAmounts, devicesToAdd, locationsToAdd);
        asyncSendBatchResults(results);

        batchProcessedCount.addAndGet(results.size());
        
        logger.info("批量处理完成: 消息数={}, 成功数={}, 失败数={}", 
            messages.size(), results.size(), batchErrorCount.get());

        return results;
    }

    private void enrichBatchData(List<TransactionFact> facts) {
        Set<String> userIds = new HashSet<>();
        Set<String> deviceKeys = new HashSet<>();
        Set<String> locationKeys = new HashSet<>();

        for (TransactionFact fact : facts) {
            userIds.add(fact.getUserId());
            if (fact.getDeviceId() != null) {
                deviceKeys.add("user:devices:" + fact.getUserId());
            }
            if (fact.getLocation() != null) {
                locationKeys.add("user:locations:" + fact.getUserId());
            }
        }

        Map<String, Object> batchResults = redisTemplate.execute((RedisCallback<Map<String, Object>>) connection -> {
            Map<String, Object> result = new HashMap<>();
            
            for (String userId : userIds) {
                String txnCountKey = USER_TRANSACTION_PREFIX + userId + ":count";
                String dailyKey = USER_DAILY_AMOUNT_PREFIX + userId;
                String trustKey = USER_TRUST_PREFIX + userId;
                String loginFailKey = "user:login:failures:" + userId;
                String newUserKey = "user:new:" + userId;
                
                byte[] txnCountValue = connection.get(txnCountKey.getBytes());
                byte[] dailyValue = connection.get(dailyKey.getBytes());
                byte[] trustValue = connection.get(trustKey.getBytes());
                byte[] loginFailValue = connection.get(loginFailKey.getBytes());
                byte[] newUserValue = connection.get(newUserKey.getBytes());
                
                result.put(txnCountKey, txnCountValue != null ? new String(txnCountValue) : null);
                result.put(dailyKey, dailyValue != null ? new String(dailyValue) : null);
                result.put(trustKey, trustValue != null ? new String(trustValue) : null);
                result.put(loginFailKey, loginFailValue != null ? new String(loginFailValue) : null);
                result.put(newUserKey, newUserValue != null ? new String(newUserValue) : null);
            }
            
            return result;
        });

        for (TransactionFact fact : facts) {
            String userId = fact.getUserId();
            
            String txnCountValue = (String) batchResults.get(USER_TRANSACTION_PREFIX + userId + ":count");
            fact.setUserTransactionCount(txnCountValue != null ? Integer.parseInt(txnCountValue) : 0);
            
            String dailyValue = (String) batchResults.get(USER_DAILY_AMOUNT_PREFIX + userId);
            fact.setUserDailyAmount(dailyValue != null ? new BigDecimal(dailyValue) : BigDecimal.ZERO);
            
            String loginFailValue = (String) batchResults.get("user:login:failures:" + userId);
            fact.setUserLoginFailures(loginFailValue != null ? Integer.parseInt(loginFailValue) : 0);
            
            String newUserValue = (String) batchResults.get("user:new:" + userId);
            fact.setNewUser(newUserValue != null && Boolean.parseBoolean(newUserValue));
            
            fact.setHighRiskCountry(false);
            fact.setSuspiciousRecipient(false);
        }
    }

    private TransactionFact convertToFact(Map<String, Object> transactionMap) {
        TransactionFact fact = new TransactionFact();
        fact.setTransactionId((String) transactionMap.get("transactionId"));
        fact.setUserId((String) transactionMap.get("userId"));
        fact.setAccountId((String) transactionMap.get("accountId"));
        fact.setTransactionType((String) transactionMap.get("transactionType"));
        fact.setRecipient((String) transactionMap.get("recipient"));
        fact.setDeviceId((String) transactionMap.get("deviceId"));
        fact.setIpAddress((String) transactionMap.get("ipAddress"));
        fact.setLocation((String) transactionMap.get("location"));

        Object amountObj = transactionMap.get("amount");
        if (amountObj != null) {
            if (amountObj instanceof BigDecimal) {
                fact.setAmount((BigDecimal) amountObj);
            } else {
                fact.setAmount(new BigDecimal(amountObj.toString()));
            }
        }

        Object timeObj = transactionMap.get("transactionTime");
        if (timeObj != null) {
            fact.setTransactionTime(LocalDateTime.parse(timeObj.toString(), DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        }

        return fact;
    }

    private Map<String, Object> buildRulesResult(TransactionFact fact) {
        Map<String, Object> result = new HashMap<>();
        result.put("transactionId", fact.getTransactionId());
        result.put("userId", fact.getUserId());
        result.put("decision", fact.getRuleDecision());
        result.put("riskScore", fact.getRuleRiskScore());
        result.put("reason", fact.getRuleReason());
        result.put("timestamp", LocalDateTime.now().toString());

        Map<String, Object> factors = new HashMap<>();
        factors.put("userTransactionCount", fact.getUserTransactionCount());
        factors.put("userDailyAmount", fact.getUserDailyAmount());
        factors.put("isNewUser", fact.isNewUser());
        factors.put("isNewDevice", fact.isNewDevice());
        factors.put("isNewLocation", fact.isNewLocation());
        result.put("riskFactors", factors);

        return result;
    }

    @Async("taskExecutor")
    public void asyncUpdateBatchStats(
            Map<String, Long> userTxnCounts,
            Map<String, BigDecimal> userDailyAmounts,
            Map<String, Set<String>> devicesToAdd,
            Map<String, Set<String>> locationsToAdd) {
        
        CompletableFuture.runAsync(() -> {
            redisTemplate.executePipelined((RedisCallback<Void>) connection -> {
                for (Map.Entry<String, Long> entry : userTxnCounts.entrySet()) {
                    String key = USER_TRANSACTION_PREFIX + entry.getKey() + ":count";
                    connection.incrBy(key.getBytes(), entry.getValue());
                    connection.expire(key.getBytes(), 30 * 24 * 60 * 60);
                }

                for (Map.Entry<String, BigDecimal> entry : userDailyAmounts.entrySet()) {
                    String key = USER_DAILY_AMOUNT_PREFIX + entry.getKey();
                    byte[] current = connection.get(key.getBytes());
                    BigDecimal currentAmount = current != null ? new BigDecimal(new String(current)) : BigDecimal.ZERO;
                    connection.set(key.getBytes(), currentAmount.add(entry.getValue()).toString().getBytes());
                    connection.expire(key.getBytes(), 24 * 60 * 60);
                }

                for (Map.Entry<String, Set<String>> entry : devicesToAdd.entrySet()) {
                    String key = "user:devices:" + entry.getKey();
                    for (String device : entry.getValue()) {
                        connection.sAdd(key.getBytes(), device.getBytes());
                    }
                    connection.expire(key.getBytes(), 90 * 24 * 60 * 60);
                }

                for (Map.Entry<String, Set<String>> entry : locationsToAdd.entrySet()) {
                    String key = "user:locations:" + entry.getKey();
                    for (String location : entry.getValue()) {
                        connection.sAdd(key.getBytes(), location.getBytes());
                    }
                    connection.expire(key.getBytes(), 90 * 24 * 60 * 60);
                }

                return null;
            });
            
            logger.debug("批量统计更新完成: 用户数={}", userTxnCounts.size());
        });
    }

    @Async("taskExecutor")
    public void asyncSendBatchResults(List<Map<String, Object>> results) {
        CompletableFuture.runAsync(() -> {
            for (Map<String, Object> result : results) {
                try {
                    String transactionId = (String) result.get("transactionId");
                    kafkaTemplate.send(RULES_RESULT_TOPIC, transactionId, result);
                } catch (Exception e) {
                    logger.error("发送规则结果失败: {}", e.getMessage());
                }
            }
            logger.debug("批量结果发送完成: 数量={}", results.size());
        });
    }

    public long getBatchProcessedCount() {
        return batchProcessedCount.get();
    }

    public long getBatchErrorCount() {
        return batchErrorCount.get();
    }

    public void resetMetrics() {
        batchProcessedCount.set(0);
        batchErrorCount.set(0);
    }
}
