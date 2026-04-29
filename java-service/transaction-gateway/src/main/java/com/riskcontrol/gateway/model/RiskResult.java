package com.riskcontrol.gateway.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RiskResult {
    private String transactionId;
    private String decision;
    private double riskScore;
    private Map<String, Object> riskFactors;
    private String reason;
    private LocalDateTime decisionTime;

    public static RiskResult approve(String transactionId, double riskScore, Map<String, Object> factors) {
        return RiskResult.builder()
                .transactionId(transactionId)
                .decision("APPROVE")
                .riskScore(riskScore)
                .riskFactors(factors)
                .reason("交易通过风控检查")
                .decisionTime(LocalDateTime.now())
                .build();
    }

    public static RiskResult reject(String transactionId, double riskScore, String reason, Map<String, Object> factors) {
        return RiskResult.builder()
                .transactionId(transactionId)
                .decision("REJECT")
                .riskScore(riskScore)
                .riskFactors(factors)
                .reason(reason)
                .decisionTime(LocalDateTime.now())
                .build();
    }

    public static RiskResult review(String transactionId, double riskScore, String reason, Map<String, Object> factors) {
        return RiskResult.builder()
                .transactionId(transactionId)
                .decision("REVIEW")
                .riskScore(riskScore)
                .riskFactors(factors)
                .reason(reason)
                .decisionTime(LocalDateTime.now())
                .build();
    }
}
