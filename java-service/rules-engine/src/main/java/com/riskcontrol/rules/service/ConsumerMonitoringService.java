package com.riskcontrol.rules.service;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class ConsumerMonitoringService {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerMonitoringService.class);

    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    @Autowired
    private KafkaListenerEndpointRegistry endpointRegistry;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    private static final String METRICS_PREFIX = "consumer:metrics:";
    private static final String LAG_PREFIX = "consumer:lag:";
    private static final String ALERT_PREFIX = "consumer:alert:";

    private final Map<String, TopicPartitionMetrics> partitionMetrics = new ConcurrentHashMap<>();
    private final AtomicLong totalMessagesProcessed = new AtomicLong(0);
    private final AtomicLong totalMessagesFailed = new AtomicLong(0);
    private final AtomicLong lastProcessTime = new AtomicLong(0);

    private long lagWarningThreshold = 1000;
    private long lagCriticalThreshold = 10000;
    private double errorRateWarningThreshold = 0.01;
    private double errorRateCriticalThreshold = 0.05;

    public static class TopicPartitionMetrics {
        private final String topic;
        private final int partition;
        private long currentOffset;
        private long committedOffset;
        private long endOffset;
        private long lag;
        private long messagesProcessed;
        private long messagesFailed;
        private LocalDateTime lastCommitTime;
        private LocalDateTime lastProcessTime;

        public TopicPartitionMetrics(String topic, int partition) {
            this.topic = topic;
            this.partition = partition;
            this.lastCommitTime = LocalDateTime.now();
            this.lastProcessTime = LocalDateTime.now();
        }

        public long getLag() {
            return lag;
        }

        public void setLag(long lag) {
            this.lag = lag;
        }

        public long getMessagesProcessed() {
            return messagesProcessed;
        }

        public void incrementMessagesProcessed() {
            this.messagesProcessed++;
            this.lastProcessTime = LocalDateTime.now();
        }

        public void incrementMessagesFailed() {
            this.messagesFailed++;
        }

        public long getMessagesFailed() {
            return messagesFailed;
        }
    }

    @PostConstruct
    public void init() {
        logger.info("消费者监控服务初始化完成");
    }

    @Scheduled(fixedRate = 10000)
    public void monitorConsumerLag() {
        try (Consumer<String, String> consumer = consumerFactory.createConsumer()) {
            Map<String, Object> lagReport = new HashMap<>();
            Map<String, Object> partitionInfo = new HashMap<>();

            List<String> topics = Arrays.asList("transactions", "rules-results", "ml-results", "final-decisions");
            
            for (String topic : topics) {
                Set<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(info -> new TopicPartition(topic, info.partition()))
                    .collect(java.util.stream.Collectors.toSet());

                consumer.assign(partitions);
                consumer.seekToEnd(partitions);

                Map<TopicPartition, Long> endOffsets = new HashMap<>();
                for (TopicPartition tp : partitions) {
                    endOffsets.put(tp, consumer.position(tp));
                }

                for (TopicPartition tp : partitions) {
                    String key = tp.topic() + "-" + tp.partition();
                    TopicPartitionMetrics metrics = partitionMetrics.computeIfAbsent(
                        key, k -> new TopicPartitionMetrics(tp.topic(), tp.partition()));

                    OffsetAndMetadata committed = consumer.committed(tp);
                    long committedOffset = committed != null ? committed.offset() : 0;
                    long endOffset = endOffsets.getOrDefault(tp, 0L);
                    long lag = endOffset - committedOffset;

                    metrics.setLag(lag);

                    Map<String, Object> info = new HashMap<>();
                    info.put("topic", tp.topic());
                    info.put("partition", tp.partition());
                    info.put("committedOffset", committedOffset);
                    info.put("endOffset", endOffset);
                    info.put("lag", lag);
                    info.put("processed", metrics.getMessagesProcessed());
                    info.put("failed", metrics.getMessagesFailed());
                    partitionInfo.put(key, info);

                    checkAndTriggerAlert(tp.topic(), tp.partition(), lag, metrics);
                }
            }

            lagReport.put("timestamp", System.currentTimeMillis());
            lagReport.put("partitions", partitionInfo);
            lagReport.put("totalLag", calculateTotalLag());
            lagReport.put("totalProcessed", totalMessagesProcessed.get());
            lagReport.put("totalFailed", totalMessagesFailed.get());

            redisTemplate.opsForValue().set(
                METRICS_PREFIX + "status",
                lagReport,
                5,
                java.util.concurrent.TimeUnit.MINUTES
            );

            logger.debug("消费者延迟监控完成: 总延迟={}", calculateTotalLag());

        } catch (Exception e) {
            logger.error("监控消费者延迟失败: {}", e.getMessage(), e);
        }
    }

    private void checkAndTriggerAlert(String topic, int partition, long lag, TopicPartitionMetrics metrics) {
        String alertKey = ALERT_PREFIX + topic + "-" + partition;
        String lagKey = LAG_PREFIX + topic + "-" + partition;

        redisTemplate.opsForValue().set(lagKey, lag, 1, java.util.concurrent.TimeUnit.HOURS);

        if (lag > lagCriticalThreshold) {
            Map<String, Object> alert = new HashMap<>();
            alert.put("type", "CRITICAL");
            alert.put("topic", topic);
            alert.put("partition", partition);
            alert.put("lag", lag);
            alert.put("threshold", lagCriticalThreshold);
            alert.put("message", "消费者延迟超过临界阈值！");
            alert.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

            redisTemplate.opsForValue().set(alertKey, alert, 1, java.util.concurrent.TimeUnit.HOURS);
            logger.error("CRITICAL ALERT: 主题={}, 分区={}, 延迟={}, 阈值={}", 
                topic, partition, lag, lagCriticalThreshold);

        } else if (lag > lagWarningThreshold) {
            Map<String, Object> alert = new HashMap<>();
            alert.put("type", "WARNING");
            alert.put("topic", topic);
            alert.put("partition", partition);
            alert.put("lag", lag);
            alert.put("threshold", lagWarningThreshold);
            alert.put("message", "消费者延迟超过警告阈值");
            alert.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

            redisTemplate.opsForValue().set(alertKey, alert, 30, java.util.concurrent.TimeUnit.MINUTES);
            logger.warn("WARNING: 主题={}, 分区={}, 延迟={}, 阈值={}", 
                topic, partition, lag, lagWarningThreshold);
        } else {
            redisTemplate.delete(alertKey);
        }
    }

    public void recordMessageProcessed(String topic, int partition, boolean success) {
        String key = topic + "-" + partition;
        TopicPartitionMetrics metrics = partitionMetrics.computeIfAbsent(
            key, k -> new TopicPartitionMetrics(topic, partition));

        if (success) {
            metrics.incrementMessagesProcessed();
            totalMessagesProcessed.incrementAndGet();
        } else {
            metrics.incrementMessagesFailed();
            totalMessagesFailed.incrementAndGet();
        }

        lastProcessTime.set(System.currentTimeMillis());
    }

    public long calculateTotalLag() {
        return partitionMetrics.values().stream()
            .mapToLong(TopicPartitionMetrics::getLag)
            .sum();
    }

    public double getErrorRate() {
        long total = totalMessagesProcessed.get() + totalMessagesFailed.get();
        if (total == 0) return 0.0;
        return (double) totalMessagesFailed.get() / total;
    }

    public Map<String, Object> getConsumerStatus() {
        Map<String, Object> status = new HashMap<>();
        
        status.put("totalProcessed", totalMessagesProcessed.get());
        status.put("totalFailed", totalMessagesFailed.get());
        status.put("totalLag", calculateTotalLag());
        status.put("errorRate", String.format("%.4f", getErrorRate()));
        status.put("lastProcessTime", lastProcessTime.get());
        status.put("timestamp", System.currentTimeMillis());

        Set<MessageListenerContainer> containers = endpointRegistry.getAllListenerContainers();
        status.put("activeContainers", containers.size());
        
        Map<String, Object> containerStatus = new HashMap<>();
        for (MessageListenerContainer container : containers) {
            String id = container.getListenerId();
            Map<String, Object> info = new HashMap<>();
            info.put("isRunning", container.isRunning());
            info.put("isPaused", container.isContainerPaused());
            containerStatus.put(id, info);
        }
        status.put("containers", containerStatus);

        Map<String, Object> partitions = new HashMap<>();
        for (Map.Entry<String, TopicPartitionMetrics> entry : partitionMetrics.entrySet()) {
            TopicPartitionMetrics m = entry.getValue();
            Map<String, Object> info = new HashMap<>();
            info.put("lag", m.getLag());
            info.put("processed", m.getMessagesProcessed());
            info.put("failed", m.getMessagesFailed());
            partitions.put(entry.getKey(), info);
        }
        status.put("partitions", partitions);

        return status;
    }

    public List<Map<String, Object>> getActiveAlerts() {
        List<Map<String, Object>> alerts = new ArrayList<>();
        
        Set<String> keys = redisTemplate.keys(ALERT_PREFIX + "*");
        if (keys != null) {
            for (String key : keys) {
                Object alert = redisTemplate.opsForValue().get(key);
                if (alert instanceof Map) {
                    alerts.add((Map<String, Object>) alert);
                }
            }
        }

        return alerts;
    }

    public void updateThresholds(long warningLag, long criticalLag, double warningError, double criticalError) {
        this.lagWarningThreshold = warningLag;
        this.lagCriticalThreshold = criticalLag;
        this.errorRateWarningThreshold = warningError;
        this.errorRateCriticalThreshold = criticalError;
        logger.info("监控阈值已更新: lagWarning={}, lagCritical={}, errorWarning={}, errorCritical={}",
            warningLag, criticalLag, warningError, criticalError);
    }

    public long getLagWarningThreshold() {
        return lagWarningThreshold;
    }

    public long getLagCriticalThreshold() {
        return lagCriticalThreshold;
    }
}
