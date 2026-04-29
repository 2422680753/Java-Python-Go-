package com.riskcontrol.decision.model;

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
public class FinalDecision {
    private String transactionId;
    private String userId;
    private String finalDecision;
    private double totalRiskScore;
    private double rulesScore;
    private double mlScore;
    private Map<String, Object> riskFactors;
    private String reason;
    private LocalDateTime decisionTime;
    private String decisionSource;

    public static FinalDecision approve(String transactionId, String userId, double rulesScore, double mlScore,
                                         Map<String, Object> factors, String reason) {
        return FinalDecision.builder()
                .transactionId(transactionId)
                .userId(userId)
                .finalDecision("APPROVE")
                .totalRiskScore(calculateTotalScore(rulesScore, mlScore))
                .rulesScore(rulesScore)
                .mlScore(mlScore)
                .riskFactors(factors)
                .reason(reason)
                .decisionTime(LocalDateTime.now())
                .decisionSource("AUTO")
                .build();
    }

    public static FinalDecision reject(String transactionId, String userId, double rulesScore, double mlScore,
                                        Map<String, Object> factors, String reason) {
        return FinalDecision.builder()
                .transactionId(transactionId)
                .userId(userId)
                .finalDecision("REJECT")
                .totalRiskScore(calculateTotalScore(rulesScore, mlScore))
                .rulesScore(rulesScore)
                .mlScore(mlScore)
                .riskFactors(factors)
                .reason(reason)
                .decisionTime(LocalDateTime.now())
                .decisionSource("AUTO")
                .build();
    }

    public static FinalDecision review(String transactionId, String userId, double rulesScore, double mlScore,
                                        Map<String, Object> factors, String reason) {
        return FinalDecision.builder()
                .transactionId(transactionId)
                .userId(userId)
                .finalDecision("REVIEW")
                .totalRiskScore(calculateTotalScore(rulesScore, mlScore))
                .rulesScore(rulesScore)
                .mlScore(mlScore)
                .riskFactors(factors)
                .reason(reason)
                .decisionTime(LocalDateTime.now())
                .decisionSource("AUTO")
                .build();
    }

    private static double calculateTotalScore(double rulesScore, double mlScore) {
        return rulesScore * 0.6 + mlScore * 0.4;
    }
}
