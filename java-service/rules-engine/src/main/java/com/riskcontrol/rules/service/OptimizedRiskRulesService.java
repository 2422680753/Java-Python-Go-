package com.riskcontrol.rules.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class OptimizedRiskRulesService {

    private static final Logger logger = LoggerFactory.getLogger(OptimizedRiskRulesService.class);

    @Autowired
    private BatchProcessingService batchProcessingService;

    @Autowired
    private ConsumerMonitoringService monitoringService;

    private static final String TRANSACTION_TOPIC = "transactions";

    @KafkaListener(
        topics = TRANSACTION_TOPIC,
        groupId = "rules-engine",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void processTransaction(
            @Payload String message,
            Acknowledgment acknowledgment) {
        
        try {
            List<String> singleMessageBatch = List.of(message);
            var results = batchProcessingService.processBatch(singleMessageBatch);
            
            if (!results.isEmpty()) {
                monitoringService.recordMessageProcessed(TRANSACTION_TOPIC, 0, true);
            } else {
                monitoringService.recordMessageProcessed(TRANSACTION_TOPIC, 0, false);
            }
            
            acknowledgment.acknowledge();

        } catch (Exception e) {
            logger.error("处理交易消息失败: {}", e.getMessage(), e);
            monitoringService.recordMessageProcessed(TRANSACTION_TOPIC, 0, false);
        }
    }

    @KafkaListener(
        topics = TRANSACTION_TOPIC,
        groupId = "rules-engine-batch",
        containerFactory = "batchKafkaListenerContainerFactory"
    )
    public void processBatchTransactions(
            @Payload List<String> messages,
            Acknowledgment acknowledgment) {
        
        long startTime = System.currentTimeMillis();
        
        try {
            logger.info("批量处理开始: 消息数={}", messages.size());
            
            var results = batchProcessingService.processBatch(messages);
            
            for (int i = 0; i < messages.size(); i++) {
                boolean success = i < results.size();
                monitoringService.recordMessageProcessed(TRANSACTION_TOPIC, i % 8, success);
            }
            
            acknowledgment.acknowledge();
            
            long duration = System.currentTimeMillis() - startTime;
            double tps = (double) messages.size() / duration * 1000;
            
            logger.info("批量处理完成: 消息数={}, 成功数={}, 耗时={}ms, TPS={}", 
                messages.size(), results.size(), duration, String.format("%.2f", tps));

        } catch (Exception e) {
            logger.error("批量处理交易消息失败: {}", e.getMessage(), e);
            
            for (int i = 0; i < messages.size(); i++) {
                monitoringService.recordMessageProcessed(TRANSACTION_TOPIC, i % 8, false);
            }
        }
    }
}
