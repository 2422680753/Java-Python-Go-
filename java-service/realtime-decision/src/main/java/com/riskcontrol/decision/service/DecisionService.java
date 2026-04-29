package com.riskcontrol.decision.service;

import com.alibaba.fastjson.JSON;
import com.riskcontrol.decision.model.FinalDecision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class DecisionService {

    private static final Logger logger = LoggerFactory.getLogger(DecisionService.class);

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

    @KafkaListener(topics = RULES_RESULT_TOPIC, groupId = "decision-service")
    public void processRulesResult(String message) {
        try {
            Map<String, Object> rulesResult = JSON.parseObject(message, Map.class);
            String transactionId = (String) rulesResult.get("transactionId");
            String userId = (String) rulesResult.get("userId");
            String rulesDecision = (String) rulesResult.get("decision");
            double rulesScore = ((Number) rulesResult.get("riskScore")).doubleValue();

            logger.info("收到规则引擎结果: transactionId={}, decision={}, score={}",
                    transactionId, rulesDecision, rulesScore);

            if ("REJECT".equals(rulesDecision)) {
                FinalDecision finalDecision = FinalDecision.reject(
                        transactionId, userId, rulesScore, 0.0,
                        (Map<String, Object>) rulesResult.get("riskFactors"),
                        "规则引擎拒绝: " + rulesResult.get("reason")
                );
                publishFinalDecision(finalDecision);
                return;
            }

            String pendingKey = PENDING_DECISION_PREFIX + transactionId;
            Map<String, Object> pendingData = (Map<String, Object>) redisTemplate.opsForValue().get(pendingKey);

            if (pendingData == null) {
                pendingData = new HashMap<>();
                pendingData.put("transactionId", transactionId);
                pendingData.put("userId", userId);
                pendingData.put("rulesDecision", rulesDecision);
                pendingData.put("rulesScore", rulesScore);
                pendingData.put("rulesFactors", rulesResult.get("riskFactors"));
                pendingData.put("rulesReason", rulesResult.get("reason"));
                redisTemplate.opsForValue().set(pendingKey, pendingData, 5, TimeUnit.MINUTES);
            } else {
                pendingData.put("rulesDecision", rulesDecision);
                pendingData.put("rulesScore", rulesScore);
                pendingData.put("rulesFactors", rulesResult.get("riskFactors"));
                pendingData.put("rulesReason", rulesResult.get("reason"));
                redisTemplate.opsForValue().set(pendingKey, pendingData, 5, TimeUnit.MINUTES);

                if (pendingData.containsKey("mlScore")) {
                    makeFinalDecision(pendingData);
                }
            }

        } catch (Exception e) {
            logger.error("处理规则引擎结果失败: {}", e.getMessage(), e);
        }
    }

    @KafkaListener(topics = ML_RESULT_TOPIC, groupId = "decision-service")
    public void processMlResult(String message) {
        try {
            Map<String, Object> mlResult = JSON.parseObject(message, Map.class);
            String transactionId = (String) mlResult.get("transactionId");
            String userId = (String) mlResult.get("userId");
            String mlDecision = (String) mlResult.get("decision");
            double mlScore = ((Number) mlResult.get("riskScore")).doubleValue();

            logger.info("收到机器学习结果: transactionId={}, decision={}, score={}",
                    transactionId, mlDecision, mlScore);

            String pendingKey = PENDING_DECISION_PREFIX + transactionId;
            Map<String, Object> pendingData = (Map<String, Object>) redisTemplate.opsForValue().get(pendingKey);

            if (pendingData == null) {
                pendingData = new HashMap<>();
                pendingData.put("transactionId", transactionId);
                pendingData.put("userId", userId);
                pendingData.put("mlDecision", mlDecision);
                pendingData.put("mlScore", mlScore);
                pendingData.put("mlFactors", mlResult.get("riskFactors"));
                pendingData.put("mlReason", mlResult.get("reason"));
                redisTemplate.opsForValue().set(pendingKey, pendingData, 5, TimeUnit.MINUTES);
            } else {
                pendingData.put("mlDecision", mlDecision);
                pendingData.put("mlScore", mlScore);
                pendingData.put("mlFactors", mlResult.get("riskFactors"));
                pendingData.put("mlReason", mlResult.get("reason"));
                redisTemplate.opsForValue().set(pendingKey, pendingData, 5, TimeUnit.MINUTES);

                if (pendingData.containsKey("rulesScore")) {
                    makeFinalDecision(pendingData);
                }
            }

        } catch (Exception e) {
            logger.error("处理机器学习结果失败: {}", e.getMessage(), e);
        }
    }

    private void makeFinalDecision(Map<String, Object> pendingData) {
        String transactionId = (String) pendingData.get("transactionId");
        String userId = (String) pendingData.get("userId");
        double rulesScore = ((Number) pendingData.get("rulesScore")).doubleValue();
        double mlScore = ((Number) pendingData.get("mlScore")).doubleValue();

        Map<String, Object> combinedFactors = new HashMap<>();
        if (pendingData.containsKey("rulesFactors")) {
            combinedFactors.putAll((Map<String, Object>) pendingData.get("rulesFactors"));
        }
        if (pendingData.containsKey("mlFactors")) {
            combinedFactors.putAll((Map<String, Object>) pendingData.get("mlFactors"));
        }

        double totalScore = rulesScore * 0.6 + mlScore * 0.4;
        FinalDecision finalDecision;

        if (totalScore >= 0.7) {
            finalDecision = FinalDecision.reject(
                    transactionId, userId, rulesScore, mlScore,
                    combinedFactors,
                    "综合风险评分过高: " + String.format("%.2f", totalScore)
            );
        } else if (totalScore >= 0.4) {
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

        publishFinalDecision(finalDecision);

        String pendingKey = PENDING_DECISION_PREFIX + transactionId;
        redisTemplate.delete(pendingKey);
    }

    private void publishFinalDecision(FinalDecision decision) {
        logger.info("发布最终决策: transactionId={}, decision={}, totalScore={}",
                decision.getTransactionId(), decision.getFinalDecision(), decision.getTotalRiskScore());

        String finalKey = FINAL_DECISION_PREFIX + decision.getTransactionId();
        redisTemplate.opsForValue().set(finalKey, decision, 24, TimeUnit.HOURS);

        Map<String, Object> gatewayResult = new HashMap<>();
        gatewayResult.put("transactionId", decision.getTransactionId());
        gatewayResult.put("decision", decision.getFinalDecision());
        gatewayResult.put("riskScore", decision.getTotalRiskScore());
        gatewayResult.put("riskFactors", decision.getRiskFactors());
        gatewayResult.put("reason", decision.getReason());
        gatewayResult.put("decisionTime", decision.getDecisionTime().toString());

        String gatewayKey = GATEWAY_RESULT_PREFIX + decision.getTransactionId();
        redisTemplate.opsForValue().set(gatewayKey, gatewayResult, 24, TimeUnit.HOURS);

        kafkaTemplate.send(FINAL_DECISION_TOPIC, decision.getTransactionId(), decision);
    }

    public FinalDecision getFinalDecision(String transactionId) {
        String key = FINAL_DECISION_PREFIX + transactionId;
        Object result = redisTemplate.opsForValue().get(key);
        if (result instanceof FinalDecision) {
            return (FinalDecision) result;
        }
        return null;
    }
}
