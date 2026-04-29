package com.riskcontrol.rules.service;

import com.riskcontrol.rules.config.DynamicThresholdConfig;
import com.riskcontrol.rules.model.TransactionFact;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class DynamicScoringService {

    private static final Logger logger = LoggerFactory.getLogger(DynamicScoringService.class);

    @Autowired
    private DynamicThresholdConfig thresholdConfig;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    private static final String USER_PROFILE_PREFIX = "user:profile:";
    private static final String USER_HISTORY_PREFIX = "user:history:";
    private static final String LARGE_TXN_WHITELIST_PREFIX = "whitelist:large-txn:";

    public double calculateDynamicScore(TransactionFact fact) {
        double baseScore = fact.getRuleRiskScore();
        
        Map<String, Object> adjustmentFactors = calculateAdjustmentFactors(fact);
        
        double adjustedScore = applyDynamicWeights(baseScore, adjustmentFactors, fact);
        
        adjustedScore = applyContextualAdjustments(adjustedScore, fact);
        
        adjustedScore = Math.max(0.0, Math.min(1.0, adjustedScore));
        
        logger.info("动态评分计算完成: transactionId={}, baseScore={}, adjustedScore={}",
                fact.getTransactionId(), baseScore, adjustedScore);
        
        return adjustedScore;
    }

    private Map<String, Object> calculateAdjustmentFactors(TransactionFact fact) {
        Map<String, Object> factors = new HashMap<>();
        
        UserProfile profile = getUserProfile(fact.getUserId());
        
        double userTrustLevel = calculateUserTrustLevel(profile, fact);
        factors.put("userTrustLevel", userTrustLevel);
        
        double behaviorConsistency = calculateBehaviorConsistency(profile, fact);
        factors.put("behaviorConsistency", behaviorConsistency);
        
        double amountContext = calculateAmountContext(fact, profile);
        factors.put("amountContext", amountContext);
        
        double timeContext = calculateTimeContext(fact);
        factors.put("timeContext", timeContext);
        
        return factors;
    }

    private double applyDynamicWeights(double baseScore, Map<String, Object> factors, TransactionFact fact) {
        double adjustedScore = baseScore;
        
        double userTrustLevel = ((Number) factors.get("userTrustLevel")).doubleValue();
        double behaviorConsistency = ((Number) factors.get("behaviorConsistency")).doubleValue();
        double amountContext = ((Number) factors.get("amountContext")).doubleValue();
        double timeContext = ((Number) factors.get("timeContext")).doubleValue();
        
        if (userTrustLevel > 0.7 && baseScore < 0.5) {
            adjustedScore *= 0.7;
            logger.debug("高信任用户风险评分降低: userTrustLevel={}, scoreReduction=30%", userTrustLevel);
        } else if (userTrustLevel < 0.3) {
            adjustedScore *= 1.3;
            logger.debug("低信任用户风险评分增加: userTrustLevel={}, scoreIncrease=30%", userTrustLevel);
        }
        
        if (behaviorConsistency > 0.8 && baseScore < 0.5) {
            adjustedScore *= 0.8;
        }
        
        if (amountContext > 0.5) {
            adjustedScore *= (1 + amountContext * 0.5);
        }
        
        if (timeContext > 0.5) {
            adjustedScore *= 1.2;
        }
        
        return adjustedScore;
    }

    private double applyContextualAdjustments(double score, TransactionFact fact) {
        if (isInLargeTransactionWhitelist(fact)) {
            logger.info("用户在大额交易白名单中，风险评分降低: userId={}", fact.getUserId());
            return score * 0.3;
        }
        
        if (fact.getAmount().compareTo(thresholdConfig.getVeryLargeThreshold()) >= 0) {
            boolean hasHistory = hasSuccessfulLargeTransactionHistory(fact.getUserId());
            if (hasHistory) {
                logger.info("用户有大额交易历史，风险评分适度增加: userId={}", fact.getUserId());
                return Math.min(1.0, score + 0.1);
            }
        }
        
        if (isVIPUser(fact.getUserId())) {
            return score * 0.5;
        }
        
        return score;
    }

    private UserProfile getUserProfile(String userId) {
        String key = USER_PROFILE_PREFIX + userId;
        Object data = redisTemplate.opsForValue().get(key);
        
        if (data instanceof UserProfile) {
            return (UserProfile) data;
        }
        
        UserProfile profile = new UserProfile();
        profile.setUserId(userId);
        profile.setTrustLevel(0.5);
        profile.setAccountAge(0);
        profile.setTotalTransactions(0);
        profile.setSuccessRate(1.0);
        profile.setAvgTransactionAmount(BigDecimal.ZERO);
        
        return profile;
    }

    private double calculateUserTrustLevel(UserProfile profile, TransactionFact fact) {
        double trustLevel = profile.getTrustLevel();
        
        if (profile.getAccountAge() > 30) {
            trustLevel += 0.1;
        }
        
        if (profile.getTotalTransactions() > 100) {
            trustLevel += 0.1;
        }
        
        if (profile.getSuccessRate() > 0.95) {
            trustLevel += 0.1;
        }
        
        if (fact.getUserLoginFailures() > 0) {
            trustLevel -= fact.getUserLoginFailures() * 0.1;
        }
        
        return Math.max(0.0, Math.min(1.0, trustLevel));
    }

    private double calculateBehaviorConsistency(UserProfile profile, TransactionFact fact) {
        double consistency = 0.5;
        
        if (!fact.isNewUser() && !fact.isNewDevice() && !fact.isNewLocation()) {
            consistency += 0.3;
        }
        
        if (fact.isNewDevice()) {
            consistency -= 0.2;
        }
        if (fact.isNewLocation()) {
            consistency -= 0.2;
        }
        
        return Math.max(0.0, Math.min(1.0, consistency));
    }

    private double calculateAmountContext(TransactionFact fact, UserProfile profile) {
        double contextScore = 0.0;
        
        if (profile.getAvgTransactionAmount().compareTo(BigDecimal.ZERO) > 0) {
            BigDecimal ratio = fact.getAmount().divide(
                    profile.getAvgTransactionAmount().add(BigDecimal.ONE), 
                    2, RoundingMode.HALF_UP);
            
            if (ratio.compareTo(new BigDecimal("5")) >= 0) {
                contextScore = 0.8;
            } else if (ratio.compareTo(new BigDecimal("3")) >= 0) {
                contextScore = 0.5;
            } else if (ratio.compareTo(new BigDecimal("2")) >= 0) {
                contextScore = 0.2;
            }
        }
        
        if (fact.getAmount().compareTo(thresholdConfig.getVeryLargeThreshold()) >= 0) {
            contextScore = Math.max(contextScore, 0.9);
        } else if (fact.getAmount().compareTo(thresholdConfig.getLargeTransactionThreshold()) >= 0) {
            contextScore = Math.max(contextScore, 0.5);
        }
        
        return contextScore;
    }

    private double calculateTimeContext(TransactionFact fact) {
        LocalTime transactionTime = LocalTime.now();
        
        if (transactionTime.isAfter(LocalTime.of(0, 0)) && 
            transactionTime.isBefore(LocalTime.of(6, 0))) {
            return 0.7;
        }
        
        if (transactionTime.isAfter(LocalTime.of(22, 0)) || 
            transactionTime.isBefore(LocalTime.of(7, 0))) {
            return 0.4;
        }
        
        return 0.0;
    }

    private boolean isInLargeTransactionWhitelist(TransactionFact fact) {
        String userKey = LARGE_TXN_WHITELIST_PREFIX + "user:" + fact.getUserId();
        String accountKey = LARGE_TXN_WHITELIST_PREFIX + "account:" + fact.getAccountId();
        
        return Boolean.TRUE.equals(redisTemplate.hasKey(userKey)) ||
               Boolean.TRUE.equals(redisTemplate.hasKey(accountKey));
    }

    private boolean hasSuccessfulLargeTransactionHistory(String userId) {
        String key = USER_HISTORY_PREFIX + userId + ":large-success";
        Long count = redisTemplate.opsForValue().increment(key, 0);
        return count != null && count >= 3;
    }

    private boolean isVIPUser(String userId) {
        String key = "vip:user:" + userId;
        return Boolean.TRUE.equals(redisTemplate.hasKey(key));
    }

    public void addToLargeTransactionWhitelist(String type, String id, 
            BigDecimal maxAmount, int maxDailyTransactions, long expireSeconds) {
        String key = LARGE_TXN_WHITELIST_PREFIX + type + ":" + id;
        
        Map<String, Object> whitelistEntry = new HashMap<>();
        whitelistEntry.put("type", type);
        whitelistEntry.put("id", id);
        whitelistEntry.put("maxAmount", maxAmount);
        whitelistEntry.put("maxDailyTransactions", maxDailyTransactions);
        whitelistEntry.put("createTime", LocalDateTime.now().toString());
        
        redisTemplate.opsForValue().set(key, whitelistEntry);
        
        if (expireSeconds > 0) {
            redisTemplate.expire(key, expireSeconds, TimeUnit.SECONDS);
        }
        
        logger.info("添加到大额交易白名单: type={}, id={}, maxAmount={}", type, id, maxAmount);
    }

    public void removeFromLargeTransactionWhitelist(String type, String id) {
        String key = LARGE_TXN_WHITELIST_PREFIX + type + ":" + id;
        redisTemplate.delete(key);
        logger.info("从大额交易白名单移除: type={}, id={}", type, id);
    }

    public void updateUserProfile(String userId, UserProfile profile) {
        String key = USER_PROFILE_PREFIX + userId;
        redisTemplate.opsForValue().set(key, profile, 30, TimeUnit.DAYS);
    }

    public void recordSuccessfulLargeTransaction(String userId) {
        String key = USER_HISTORY_PREFIX + userId + ":large-success";
        redisTemplate.opsForValue().increment(key);
        redisTemplate.expire(key, 90, TimeUnit.DAYS);
    }

    public static class UserProfile implements java.io.Serializable {
        private String userId;
        private double trustLevel;
        private int accountAge;
        private int totalTransactions;
        private double successRate;
        private BigDecimal avgTransactionAmount;
        private LocalDateTime lastActiveTime;

        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }
        public double getTrustLevel() { return trustLevel; }
        public void setTrustLevel(double trustLevel) { this.trustLevel = trustLevel; }
        public int getAccountAge() { return accountAge; }
        public void setAccountAge(int accountAge) { this.accountAge = accountAge; }
        public int getTotalTransactions() { return totalTransactions; }
        public void setTotalTransactions(int totalTransactions) { this.totalTransactions = totalTransactions; }
        public double getSuccessRate() { return successRate; }
        public void setSuccessRate(double successRate) { this.successRate = successRate; }
        public BigDecimal getAvgTransactionAmount() { return avgTransactionAmount; }
        public void setAvgTransactionAmount(BigDecimal avgTransactionAmount) { this.avgTransactionAmount = avgTransactionAmount; }
        public LocalDateTime getLastActiveTime() { return lastActiveTime; }
        public void setLastActiveTime(LocalDateTime lastActiveTime) { this.lastActiveTime = lastActiveTime; }
    }
}
