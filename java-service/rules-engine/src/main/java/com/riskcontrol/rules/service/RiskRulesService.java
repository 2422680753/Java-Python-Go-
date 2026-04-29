package com.riskcontrol.rules.service;

import com.alibaba.fastjson.JSON;
import com.riskcontrol.rules.model.TransactionFact;
import org.kie.api.runtime.KieSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class RiskRulesService {

    private static final Logger logger = LoggerFactory.getLogger(RiskRulesService.class);

    @Autowired
    private KieSession kieSession;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    private static final String TRANSACTION_TOPIC = "transactions";
    private static final String RULES_RESULT_TOPIC = "rules-results";
    private static final String USER_TRANSACTION_PREFIX = "user:txn:";
    private static final String USER_DAILY_AMOUNT_PREFIX = "user:daily:";

    @KafkaListener(topics = TRANSACTION_TOPIC, groupId = "rules-engine")
    public void processTransaction(String message) {
        try {
            Map<String, Object> transactionMap = JSON.parseObject(message, Map.class);
            TransactionFact fact = convertToFact(transactionMap);

            enrichTransactionData(fact);

            kieSession.insert(fact);
            kieSession.fireAllRules();
            kieSession.dispose();

            logger.info("规则引擎处理完成: transactionId={}, decision={}, score={}",
                    fact.getTransactionId(), fact.getRuleDecision(), fact.getRuleRiskScore());

            Map<String, Object> result = buildRulesResult(fact);
            kafkaTemplate.send(RULES_RESULT_TOPIC, fact.getTransactionId(), result);

            updateTransactionStats(fact);

        } catch (Exception e) {
            logger.error("规则引擎处理交易失败: {}", e.getMessage(), e);
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

    private void enrichTransactionData(TransactionFact fact) {
        String userId = fact.getUserId();

        Object txnCount = redisTemplate.opsForValue().get(USER_TRANSACTION_PREFIX + userId + ":count");
        fact.setUserTransactionCount(txnCount != null ? ((Number) txnCount).intValue() : 0);

        Object dailyAmount = redisTemplate.opsForValue().get(USER_DAILY_AMOUNT_PREFIX + userId);
        fact.setUserDailyAmount(dailyAmount != null ? new BigDecimal(dailyAmount.toString()) : BigDecimal.ZERO);

        Object loginFailures = redisTemplate.opsForValue().get("user:login:failures:" + userId);
        fact.setUserLoginFailures(loginFailures != null ? ((Number) loginFailures).intValue() : 0);

        Object isNewUser = redisTemplate.opsForValue().get("user:new:" + userId);
        fact.setNewUser(isNewUser != null && (Boolean) isNewUser);

        if (fact.getDeviceId() != null) {
            Object isNewDevice = redisTemplate.opsForSet().isMember("user:devices:" + userId, fact.getDeviceId());
            fact.setNewDevice(isNewDevice == null || !isNewDevice);
        }

        if (fact.getLocation() != null) {
            Object isNewLocation = redisTemplate.opsForSet().isMember("user:locations:" + userId, fact.getLocation());
            fact.setNewLocation(isNewLocation == null || !isNewLocation);
        }

        fact.setHighRiskCountry(false);
        fact.setSuspiciousRecipient(false);
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

    private void updateTransactionStats(TransactionFact fact) {
        String userId = fact.getUserId();

        redisTemplate.opsForValue().increment(USER_TRANSACTION_PREFIX + userId + ":count");
        redisTemplate.expire(USER_TRANSACTION_PREFIX + userId + ":count", 30, TimeUnit.DAYS);

        String dailyKey = USER_DAILY_AMOUNT_PREFIX + userId;
        Object current = redisTemplate.opsForValue().get(dailyKey);
        BigDecimal currentAmount = current != null ? new BigDecimal(current.toString()) : BigDecimal.ZERO;
        redisTemplate.opsForValue().set(dailyKey, currentAmount.add(fact.getAmount()));
        redisTemplate.expire(dailyKey, 1, TimeUnit.DAYS);

        if (fact.getDeviceId() != null) {
            redisTemplate.opsForSet().add("user:devices:" + userId, fact.getDeviceId());
            redisTemplate.expire("user:devices:" + userId, 90, TimeUnit.DAYS);
        }

        if (fact.getLocation() != null) {
            redisTemplate.opsForSet().add("user:locations:" + userId, fact.getLocation());
            redisTemplate.expire("user:locations:" + userId, 90, TimeUnit.DAYS);
        }
    }
}
