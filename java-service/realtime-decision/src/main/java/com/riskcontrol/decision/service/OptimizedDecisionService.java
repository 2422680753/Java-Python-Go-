package com.riskcontrol.decision.service;

import com.alibaba.fastjson.JSON;
import com.riskcontrol.decision.model.FinalDecision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class OptimizedDecisionService {

    private static final Logger logger = LoggerFactory.getLogger(OptimizedDecisionService.class);

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    private static final String RULES_RESULT_TOPIC = "rules-results";
    private static final String ML_RESULT_TOPIC = "ml-results";
    private static final String FINAL_DECISION_TOPIC = "final-decisions";
    private static final String PENDING_DECISION_PREFIX = "pending:decision:";
    private static final String FINAL_DECISION_PREFIX = "final:decision:";
    private static final String GATEWAY_RESULT_PREFIX = "risk:result:";

    private static final double RULES_WEIGHT = 0.6;
    private static final double ML_WEIGHT = 0.4;

    private final ConcurrentHashMap<String, PendingDecision> pendingDecisions = new ConcurrentHashMap<>();
    private final AtomicLong totalDecisionsMade = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);

    public static class PendingDecision {
        final String transactionId;
        final String userId;
        Double rulesScore;
        String rulesDecision;
        String rulesReason;
        Map<String, Object> rulesFactors;
        Double mlScore;
        String mlDecision;
        String mlReason;
        Map<String, Object> mlFactors;
        final long createTime;

        public PendingDecision(String transactionId, String userId) {
            this.transactionId = transactionId;
            this.userId = userId;
            this.createTime = System.currentTimeMillis();
        }

        public boolean isReady() {
            return rulesScore != null && mlScore != null;
        }
    }

    @KafkaListener(
        topics = RULES_RESULT_TOPIC,
        groupId = "decision-service",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void processRulesResult(
            @Payload String message,
            Acknowledgment acknowledgment) {
        
        long startTime = System.currentTimeMillis();
        
        try {
            Map<String, Object> rulesResult = JSON.parseObject(message, Map.class);
            String transactionId = (String) rulesResult.get("transactionId");
            String userId = (String) rulesResult.get("userId");
            double rulesScore = ((Number) rulesResult.get("riskScore")).doubleValue();
            String rulesDecision = (String) rulesResult.get("decision");

            logger.debug("收到规则引擎结果: transactionId={}, score={}", transactionId, rulesScore);

            if ("REJECT".equals(rulesDecision)) {
                FinalDecision finalDecision = FinalDecision.reject(
                    transactionId, userId, rulesScore, 0.0,
                    (Map<String, Object>) rulesResult.get("riskFactors"),
                    "规则引擎拒绝: " + rulesResult.get("reason")
                );
                publishFinalDecision(finalDecision);
                acknowledgment.acknowledge();
                totalDecisionsMade.incrementAndGet();
                return;
            }

            PendingDecision pending = pendingDecisions.computeIfAbsent(
                transactionId, k -> new PendingDecision(transactionId, userId)
            );

            synchronized (pending) {
                pending.rulesScore = rulesScore;
                pending.rulesDecision = rulesDecision;
                pending.rulesReason = (String) rulesResult.get("reason");
                pending.rulesFactors = (Map<String, Object>) rulesResult.get("riskFactors");

                if (pending.isReady()) {
                    makeFinalDecision(pending);
                    pendingDecisions.remove(transactionId);
                }
            }

            acknowledgment.acknowledge();
            totalDecisionsMade.incrementAndGet();

        } catch (Exception e) {
            logger.error("处理规则引擎结果失败: {}", e.getMessage(), e);
            totalErrors.incrementAndGet();
            acknowledgment.acknowledge();
        }
    }

    @KafkaListener(
        topics = ML_RESULT_TOPIC,
        groupId = "decision-service",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void processMlResult(
            @Payload String message,
            Acknowledgment acknowledgment) {
        
        try {
            Map<String, Object> mlResult = JSON.parseObject(message, Map.class);
            String transactionId = (String) mlResult.get("transactionId");
            String userId = (String) mlResult.get("userId");
            double mlScore = ((Number) mlResult.get("riskScore")).doubleValue();
            String mlDecision = (String) mlResult.get("decision");

            logger.debug("收到机器学习结果: transactionId={}, score={}", transactionId, mlScore);

            PendingDecision pending = pendingDecisions.computeIfAbsent(
                transactionId, k -> new PendingDecision(transactionId, userId)
            );

            synchronized (pending) {
                pending.mlScore = mlScore;
                pending.mlDecision = mlDecision;
                pending.mlReason = (String) mlResult.get("reason");
                pending.mlFactors = (Map<String, Object>) mlResult.get("riskFactors");

                if (pending.isReady()) {
                    makeFinalDecision(pending);
                    pendingDecisions.remove(transactionId);
                }
            }

            acknowledgment.acknowledge();
            totalDecisionsMade.incrementAndGet();

        } catch (Exception e) {
            logger.error("处理机器学习结果失败: {}", e.getMessage(), e);
            totalErrors.incrementAndGet();
            acknowledgment.acknowledge();
        }
    }

    private void makeFinalDecision(PendingDecision pending) {
        String transactionId = pending.transactionId;
        String userId = pending.userId;
        double rulesScore = pending.rulesScore;
        double mlScore = pending.mlScore;

        Map<String, Object> combinedFactors = new HashMap<>();
        if (pending.rulesFactors != null) {
            combinedFactors.putAll(pending.rulesFactors);
        }
        if (pending.mlFactors != null) {
            combinedFactors.putAll(pending.mlFactors);
        }

        double totalScore = rulesScore * RULES_WEIGHT + mlScore * ML_WEIGHT;
        FinalDecision finalDecision;

        if (totalScore >= 0.8) {
            finalDecision = FinalDecision.reject(
                transactionId, userId, rulesScore, mlScore,
                combinedFactors,
                "综合风险评分过高: " + String.format("%.2f", totalScore)
            );
        } else if (totalScore >= 0.5) {
            finalDecision = FinalDecision.review(
                transactionId, userId, rulesScore, mlScore,
                combinedFactors,
                "需要人工审核: 综合风险评分为 " + String.format("%.2f", totalScore)
            );
        } else {
            finalDecision = FinalDecision.approve(
                transactionId, userId, rulesScore, mlScore,
                combinedFactors,
                "低风险交易，自动通过"
            );
        }

        asyncPublishFinalDecision(finalDecision);

        logger.info("最终决策: transactionId={}, decision={}, totalScore={}",
            transactionId, finalDecision.getFinalDecision(), finalDecision.getTotalRiskScore());
    }

    @Async("taskExecutor")
    public void asyncPublishFinalDecision(FinalDecision decision) {
        CompletableFuture.runAsync(() -> {
            String transactionId = decision.getTransactionId();
            
            List<Object> results = redisTemplate.executePipelined((RedisCallback<Void>) connection -> {
                String finalKey = FINAL_DECISION_PREFIX + transactionId;
                String gatewayKey = GATEWAY_RESULT_PREFIX + transactionId;

                Map<String, Object> gatewayResult = new HashMap<>();
                gatewayResult.put("transactionId", decision.getTransactionId());
                gatewayResult.put("decision", decision.getFinalDecision());
                gatewayResult.put("riskScore", decision.getTotalRiskScore());
                gatewayResult.put("riskFactors", decision.getRiskFactors());
                gatewayResult.put("reason", decision.getReason());
                gatewayResult.put("decisionTime", decision.getDecisionTime().toString());

                try {
                    connection.setEx(
                        finalKey.getBytes(),
                        24 * 60 * 60,
                        JSON.toJSONBytes(decision)
                    );
                    connection.setEx(
                        gatewayKey.getBytes(),
                        24 * 60 * 60,
                        JSON.toJSONBytes(gatewayResult)
                    );
                } catch (Exception e) {
                    logger.error("Redis pipeline 执行失败: {}", e.getMessage());
                }

                return null;
            });

            try {
                kafkaTemplate.send(FINAL_DECISION_TOPIC, decision.getTransactionId(), decision);
            } catch (Exception e) {
                logger.error("发送最终决策到 Kafka 失败: {}", e.getMessage());
            }
        });
    }

    private void publishFinalDecision(FinalDecision decision) {
        asyncPublishFinalDecision(decision);
    }

    public FinalDecision getFinalDecision(String transactionId) {
        String key = FINAL_DECISION_PREFIX + transactionId;
        Object result = redisTemplate.opsForValue().get(key);
        if (result instanceof FinalDecision) {
            return (FinalDecision) result;
        }
        if (result instanceof String) {
            try {
                return JSON.parseObject((String) result, FinalDecision.class);
            } catch (Exception e) {
                logger.error("解析最终决策失败: {}", e.getMessage());
            }
        }
        return null;
    }

    public Map<String, Object> getDecisionStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalDecisionsMade", totalDecisionsMade.get());
        stats.put("totalErrors", totalErrors.get());
        stats.put("pendingDecisions", pendingDecisions.size());
        stats.put("timestamp", System.currentTimeMillis());
        return stats;
    }

    public void clearExpiredPendingDecisions() {
        long cutoffTime = System.currentTimeMillis() - 5 * 60 * 1000;
        
        Iterator<Map.Entry<String, PendingDecision>> iterator = pendingDecisions.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, PendingDecision> entry = iterator.next();
            if (entry.getValue().createTime < cutoffTime) {
                logger.warn("清除过期的待处理决策: transactionId={}", entry.getKey());
                iterator.remove();
                
                redisTemplate.delete(PENDING_DECISION_PREFIX + entry.getKey());
            }
        }
    }
}
