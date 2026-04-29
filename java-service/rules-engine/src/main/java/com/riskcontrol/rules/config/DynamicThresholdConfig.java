package com.riskcontrol.rules.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@Configuration
@ConfigurationProperties(prefix = "risk.threshold")
public class DynamicThresholdConfig {

    private Map<String, ThresholdLevel> levels = new HashMap<>();
    private List<WhitelistRule> whitelistRules = new ArrayList<>();
    private Map<String, Double> dynamicWeights = new HashMap<>();
    private BigDecimal largeTransactionThreshold = new BigDecimal("100000");
    private BigDecimal veryLargeThreshold = new BigDecimal("500000");
    private double rejectThreshold = 0.7;
    private double reviewThreshold = 0.4;
    private double approveThreshold = 0.3;
    private int maxTransactionPerHour = 100;
    private BigDecimal maxDailyAmount = new BigDecimal("500000");

    @Data
    public static class ThresholdLevel {
        private String name;
        private double minScore;
        private double maxScore;
        private String decision;
        private String description;
        private Map<String, Double> factorWeights = new HashMap<>();
    }

    @Data
    public static class WhitelistRule {
        private String type;
        private String id;
        private String reason;
        private boolean autoApprove = true;
        private BigDecimal maxAmount;
        private int maxDailyTransactions;
        private long expireTime;
    }

    public ThresholdLevel getThresholdLevel(double score) {
        return levels.values().stream()
                .filter(level -> score >= level.getMinScore() && score < level.getMaxScore())
                .findFirst()
                .orElseGet(() -> {
                    ThresholdLevel defaultLevel = new ThresholdLevel();
                    defaultLevel.setName("DEFAULT");
                    defaultLevel.setMinScore(0.0);
                    defaultLevel.setMaxScore(1.0);
                    defaultLevel.setDecision("REVIEW");
                    defaultLevel.setDescription("默认审核");
                    return defaultLevel;
                });
    }

    public boolean isWhitelisted(String type, String id) {
        return whitelistRules.stream()
                .anyMatch(rule -> rule.getType().equalsIgnoreCase(type) &&
                        rule.getId().equals(id) &&
                        (rule.getExpireTime() == 0 || rule.getExpireTime() > System.currentTimeMillis()));
    }

    public WhitelistRule getWhitelistRule(String type, String id) {
        return whitelistRules.stream()
                .filter(rule -> rule.getType().equalsIgnoreCase(type) &&
                        rule.getId().equals(id))
                .findFirst()
                .orElse(null);
    }
}
