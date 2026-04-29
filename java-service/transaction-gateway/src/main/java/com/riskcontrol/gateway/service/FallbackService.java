package com.riskcontrol.gateway.service;

import com.riskcontrol.gateway.model.RiskResult;
import com.riskcontrol.gateway.model.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class FallbackService {

    private static final Logger logger = LoggerFactory.getLogger(FallbackService.class);

    private final Map<String, Transaction> pendingTransactions = new ConcurrentHashMap<>();
    private final Map<String, RiskResult> cachedResults = new ConcurrentHashMap<>();

    public RiskResult processTransactionFallback(Transaction transaction, Exception ex) {
        logger.warn("交易处理进入降级模式: transactionId={}, reason={}", 
                transaction.getTransactionId(), ex.getMessage());
        
        return performQuickAssessment(transaction);
    }

    public RiskResult kafkaSendFallback(Transaction transaction, Exception ex) {
        logger.warn("Kafka发送失败，进入降级模式: transactionId={}, reason={}",
                transaction.getTransactionId(), ex.getMessage());
        
        pendingTransactions.put(transaction.getTransactionId(), transaction);
        
        return performQuickAssessment(transaction);
    }

    private RiskResult performQuickAssessment(Transaction transaction) {
        Map<String, Object> factors = new HashMap<>();
        factors.put("mode", "degraded");
        factors.put("quick_check", true);
        
        double riskScore = 0.3;
        String reason = "降级模式：快速评估";
        String decision = "REVIEW";
        
        if (transaction.getAmount() != null && transaction.getAmount().doubleValue() > 100000) {
            riskScore = 0.6;
            reason = "降级模式：大额交易需要审核";
            decision = "REVIEW";
        }
        
        if (transaction.getAmount() != null && transaction.getAmount().doubleValue() < 1000) {
            riskScore = 0.1;
            reason = "降级模式：小额交易快速通过";
            decision = "APPROVE";
        }
        
        RiskResult result = RiskResult.builder()
                .transactionId(transaction.getTransactionId())
                .decision(decision)
                .riskScore(riskScore)
                .riskFactors(factors)
                .reason(reason)
                .decisionTime(LocalDateTime.now())
                .build();
        
        cachedResults.put(transaction.getTransactionId(), result);
        
        return result;
    }

    public RiskResult getCachedResult(String transactionId) {
        return cachedResults.get(transactionId);
    }

    public int getPendingTransactionCount() {
        return pendingTransactions.size();
    }

    public void clearPendingTransactions() {
        pendingTransactions.clear();
    }

    public Map<String, Object> getFallbackStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("pendingTransactions", pendingTransactions.size());
        stats.put("cachedResults", cachedResults.size());
        return stats;
    }
}
