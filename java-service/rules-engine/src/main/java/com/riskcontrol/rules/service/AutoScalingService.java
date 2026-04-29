package com.riskcontrol.rules.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class AutoScalingService {

    private static final Logger logger = LoggerFactory.getLogger(AutoScalingService.class);

    @Autowired
    private ConsumerMonitoringService monitoringService;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry endpointRegistry;

    private static final String SCALING_PREFIX = "scaling:";
    private static final String SCALING_HISTORY_PREFIX = "scaling:history:";

    private long scaleUpLagThreshold = 5000;
    private long scaleDownLagThreshold = 500;
    private long scaleUpErrorThreshold = 3;
    private int minConsumerThreads = 2;
    private int maxConsumerThreads = 32;
    private int currentConsumerThreads = 4;

    private long cooldownPeriodMs = 60000;
    private long lastScaleUpTime = 0;
    private long lastScaleDownTime = 0;

    private final AtomicInteger consecutiveScaleUpChecks = new AtomicInteger(0);
    private final AtomicInteger consecutiveScaleDownChecks = new AtomicInteger(0);
    private static final int REQUIRED_CONSECUTIVE_CHECKS = 3;

    public enum ScalingAction {
        SCALE_UP,
        SCALE_DOWN,
        NO_ACTION
    }

    public static class ScalingDecision {
        private final ScalingAction action;
        private final String reason;
        private final double confidence;
        private final Map<String, Object> metrics;
        private final LocalDateTime timestamp;

        public ScalingDecision(ScalingAction action, String reason, double confidence, Map<String, Object> metrics) {
            this.action = action;
            this.reason = reason;
            this.confidence = confidence;
            this.metrics = metrics;
            this.timestamp = LocalDateTime.now();
        }

        public ScalingAction getAction() {
            return action;
        }

        public String getReason() {
            return reason;
        }
    }

    @PostConstruct
    public void init() {
        logger.info("自动扩容服务初始化完成");
        logger.info("当前消费者线程数: {}", currentConsumerThreads);
        logger.info("扩容阈值: lag={} messages", scaleUpLagThreshold);
        logger.info("缩容阈值: lag={} messages", scaleDownLagThreshold);
    }

    @Scheduled(fixedRate = 10000)
    public void checkAndScale() {
        try {
            ScalingDecision decision = evaluateScalingNeed();
            
            if (decision.getAction() != ScalingAction.NO_ACTION) {
                executeScaling(decision);
            }

            recordMetrics(decision);

        } catch (Exception e) {
            logger.error("自动扩缩容检查失败: {}", e.getMessage(), e);
        }
    }

    public ScalingDecision evaluateScalingNeed() {
        Map<String, Object> metrics = new HashMap<>();
        
        long totalLag = monitoringService.calculateTotalLag();
        double errorRate = monitoringService.getErrorRate();
        long currentTime = System.currentTimeMillis();

        metrics.put("totalLag", totalLag);
        metrics.put("errorRate", errorRate);
        metrics.put("currentThreads", currentConsumerThreads);
        metrics.put("scaleUpThreshold", scaleUpLagThreshold);
        metrics.put("scaleDownThreshold", scaleDownLagThreshold);

        logger.debug("扩缩容评估: lag={}, errorRate={}, threads={}", 
            totalLag, errorRate, currentConsumerThreads);

        if (currentConsumerThreads < maxConsumerThreads &&
            totalLag > scaleUpLagThreshold) {
            
            int checks = consecutiveScaleUpChecks.incrementAndGet();
            consecutiveScaleDownChecks.set(0);

            if (checks >= REQUIRED_CONSECUTIVE_CHECKS) {
                if (currentTime - lastScaleUpTime < cooldownPeriodMs) {
                    return new ScalingDecision(
                        ScalingAction.NO_ACTION,
                        "扩容冷却中",
                        0.0,
                        metrics
                    );
                }

                double confidence = calculateScaleUpConfidence(totalLag, errorRate);
                return new ScalingDecision(
                    ScalingAction.SCALE_UP,
                    String.format("消息积压超过阈值: %d > %d", totalLag, scaleUpLagThreshold),
                    confidence,
                    metrics
                );
            }
        }

        if (currentConsumerThreads > minConsumerThreads &&
            totalLag < scaleDownLagThreshold) {
            
            int checks = consecutiveScaleDownChecks.incrementAndGet();
            consecutiveScaleUpChecks.set(0);

            if (checks >= REQUIRED_CONSECUTIVE_CHECKS) {
                if (currentTime - lastScaleDownTime < cooldownPeriodMs) {
                    return new ScalingDecision(
                        ScalingAction.NO_ACTION,
                        "缩容冷却中",
                        0.0,
                        metrics
                    );
                }

                double confidence = calculateScaleDownConfidence(totalLag, errorRate);
                return new ScalingDecision(
                    ScalingAction.SCALE_DOWN,
                    String.format("消息积压低于阈值: %d < %d", totalLag, scaleDownLagThreshold),
                    confidence,
                    metrics
                );
            }
        }

        consecutiveScaleUpChecks.set(0);
        consecutiveScaleDownChecks.set(0);

        return new ScalingDecision(
            ScalingAction.NO_ACTION,
            "无需扩缩容",
            0.0,
            metrics
        );
    }

    private double calculateScaleUpConfidence(long lag, double errorRate) {
        double lagFactor = Math.min(1.0, (double) lag / (scaleUpLagThreshold * 2));
        double errorFactor = errorRate > 0.01 ? 0.5 : 1.0;
        return lagFactor * 0.7 + errorFactor * 0.3;
    }

    private double calculateScaleDownConfidence(long lag, double errorRate) {
        if (errorRate > 0.005) {
            return 0.0;
        }
        return 1.0 - Math.min(1.0, (double) lag / scaleDownLagThreshold);
    }

    private void executeScaling(ScalingDecision decision) {
        long currentTime = System.currentTimeMillis();
        int newThreadCount = currentConsumerThreads;

        if (decision.getAction() == ScalingAction.SCALE_UP) {
            newThreadCount = Math.min(maxConsumerThreads, currentConsumerThreads + 2);
            lastScaleUpTime = currentTime;
            logger.info("执行扩容: 线程数 {} -> {}", currentConsumerThreads, newThreadCount);
            
        } else if (decision.getAction() == ScalingAction.SCALE_DOWN) {
            newThreadCount = Math.max(minConsumerThreads, currentConsumerThreads - 1);
            lastScaleDownTime = currentTime;
            logger.info("执行缩容: 线程数 {} -> {}", currentConsumerThreads, newThreadCount);
        }

        if (newThreadCount != currentConsumerThreads) {
            currentConsumerThreads = newThreadCount;
            
            Map<String, Object> scalingRecord = new HashMap<>();
            scalingRecord.put("action", decision.getAction().name());
            scalingRecord.put("reason", decision.getReason());
            scalingRecord.put("oldThreadCount", currentConsumerThreads);
            scalingRecord.put("newThreadCount", newThreadCount);
            scalingRecord.put("metrics", decision.metrics);
            scalingRecord.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

            String historyKey = SCALING_HISTORY_PREFIX + System.currentTimeMillis();
            redisTemplate.opsForValue().set(historyKey, scalingRecord, 7, java.util.concurrent.TimeUnit.DAYS);

            logger.info("扩缩容执行完成: action={}, threads={}", 
                decision.getAction(), currentConsumerThreads);
        }
    }

    private void recordMetrics(ScalingDecision decision) {
        Map<String, Object> status = new HashMap<>();
        status.put("currentThreads", currentConsumerThreads);
        status.put("minThreads", minConsumerThreads);
        status.put("maxThreads", maxConsumerThreads);
        status.put("scaleUpThreshold", scaleUpLagThreshold);
        status.put("scaleDownThreshold", scaleDownLagThreshold);
        status.put("lastAction", decision.getAction().name());
        status.put("lastReason", decision.getReason());
        status.put("timestamp", System.currentTimeMillis());

        redisTemplate.opsForValue().set(
            SCALING_PREFIX + "status",
            status,
            1,
            java.util.concurrent.TimeUnit.HOURS
        );
    }

    public void triggerManualScale(int targetThreads) {
        if (targetThreads < minConsumerThreads || targetThreads > maxConsumerThreads) {
            throw new IllegalArgumentException(
                String.format("目标线程数必须在 [%d, %d] 范围内", minConsumerThreads, maxConsumerThreads)
            );
        }

        int oldThreads = currentConsumerThreads;
        currentConsumerThreads = targetThreads;

        logger.info("手动扩容: 线程数 {} -> {}", oldThreads, targetThreads);

        Map<String, Object> scalingRecord = new HashMap<>();
        scalingRecord.put("action", "MANUAL");
        scalingRecord.put("oldThreadCount", oldThreads);
        scalingRecord.put("newThreadCount", targetThreads);
        scalingRecord.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        String historyKey = SCALING_HISTORY_PREFIX + System.currentTimeMillis();
        redisTemplate.opsForValue().set(historyKey, scalingRecord, 7, java.util.concurrent.TimeUnit.DAYS);
    }

    public Map<String, Object> getScalingStatus() {
        Map<String, Object> status = new HashMap<>();
        
        status.put("currentConsumerThreads", currentConsumerThreads);
        status.put("minConsumerThreads", minConsumerThreads);
        status.put("maxConsumerThreads", maxConsumerThreads);
        status.put("scaleUpLagThreshold", scaleUpLagThreshold);
        status.put("scaleDownLagThreshold", scaleDownLagThreshold);
        status.put("cooldownPeriodMs", cooldownPeriodMs);
        status.put("lastScaleUpTime", lastScaleUpTime);
        status.put("lastScaleDownTime", lastScaleDownTime);
        status.put("consecutiveScaleUpChecks", consecutiveScaleUpChecks.get());
        status.put("consecutiveScaleDownChecks", consecutiveScaleDownChecks.get());
        
        status.put("totalLag", monitoringService.calculateTotalLag());
        status.put("errorRate", monitoringService.getErrorRate());

        return status;
    }

    public List<Map<String, Object>> getScalingHistory(int limit) {
        List<Map<String, Object>> history = new ArrayList<>();
        
        Set<String> keys = redisTemplate.keys(SCALING_HISTORY_PREFIX + "*");
        if (keys != null) {
            List<String> keyList = new ArrayList<>(keys);
            keyList.sort(Collections.reverseOrder());
            
            for (String key : keyList.stream().limit(limit).toList()) {
                Object record = redisTemplate.opsForValue().get(key);
                if (record instanceof Map) {
                    history.add((Map<String, Object>) record);
                }
            }
        }

        return history;
    }

    public void updateScalingConfiguration(
            long scaleUpLag,
            long scaleDownLag,
            int minThreads,
            int maxThreads,
            long cooldownMs) {
        
        if (minThreads < 1) {
            throw new IllegalArgumentException("最小线程数必须大于0");
        }
        if (maxThreads < minThreads) {
            throw new IllegalArgumentException("最大线程数必须大于等于最小线程数");
        }
        if (scaleDownLag >= scaleUpLag) {
            throw new IllegalArgumentException("缩容阈值必须小于扩容阈值");
        }

        this.scaleUpLagThreshold = scaleUpLag;
        this.scaleDownLagThreshold = scaleDownLag;
        this.minConsumerThreads = minThreads;
        this.maxConsumerThreads = maxThreads;
        this.cooldownPeriodMs = cooldownMs;

        this.currentConsumerThreads = Math.min(maxThreads, Math.max(minThreads, currentConsumerThreads));

        logger.info("扩缩容配置已更新: scaleUp={}, scaleDown={}, minThreads={}, maxThreads={}, cooldown={}ms",
            scaleUpLag, scaleDownLag, minThreads, maxThreads, cooldownMs);
    }
}
