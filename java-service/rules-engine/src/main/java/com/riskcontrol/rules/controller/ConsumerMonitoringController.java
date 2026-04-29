package com.riskcontrol.rules.controller;

import com.riskcontrol.rules.service.AutoScalingService;
import com.riskcontrol.rules.service.BatchProcessingService;
import com.riskcontrol.rules.service.ConsumerMonitoringService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/consumer")
public class ConsumerMonitoringController {

    @Autowired
    private ConsumerMonitoringService monitoringService;

    @Autowired
    private AutoScalingService autoScalingService;

    @Autowired
    private BatchProcessingService batchProcessingService;

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getConsumerStatus() {
        return ResponseEntity.ok(monitoringService.getConsumerStatus());
    }

    @GetMapping("/alerts")
    public ResponseEntity<List<Map<String, Object>>> getActiveAlerts() {
        return ResponseEntity.ok(monitoringService.getActiveAlerts());
    }

    @GetMapping("/scaling/status")
    public ResponseEntity<Map<String, Object>> getScalingStatus() {
        return ResponseEntity.ok(autoScalingService.getScalingStatus());
    }

    @GetMapping("/scaling/history")
    public ResponseEntity<List<Map<String, Object>>> getScalingHistory(
            @RequestParam(defaultValue = "10") int limit) {
        return ResponseEntity.ok(autoScalingService.getScalingHistory(limit));
    }

    @PostMapping("/scaling/manual")
    public ResponseEntity<Map<String, Object>> manualScale(
            @RequestBody Map<String, Integer> request) {
        
        Integer targetThreads = request.get("targetThreads");
        if (targetThreads == null) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", "targetThreads is required");
            return ResponseEntity.badRequest().body(error);
        }

        try {
            autoScalingService.triggerManualScale(targetThreads);
            
            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("targetThreads", targetThreads);
            result.put("message", "Manual scaling triggered successfully");
            
            return ResponseEntity.ok(result);
            
        } catch (IllegalArgumentException e) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    @PostMapping("/scaling/config")
    public ResponseEntity<Map<String, Object>> updateScalingConfig(
            @RequestBody Map<String, Object> request) {
        
        try {
            long scaleUpLag = ((Number) request.getOrDefault("scaleUpLag", 5000)).longValue();
            long scaleDownLag = ((Number) request.getOrDefault("scaleDownLag", 500)).longValue();
            int minThreads = ((Number) request.getOrDefault("minThreads", 2)).intValue();
            int maxThreads = ((Number) request.getOrDefault("maxThreads", 32)).intValue();
            long cooldownMs = ((Number) request.getOrDefault("cooldownMs", 60000)).longValue();

            autoScalingService.updateScalingConfiguration(
                scaleUpLag, scaleDownLag, minThreads, maxThreads, cooldownMs
            );

            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("config", autoScalingService.getScalingStatus());
            
            return ResponseEntity.ok(result);
            
        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    @PostMapping("/monitoring/thresholds")
    public ResponseEntity<Map<String, Object>> updateMonitoringThresholds(
            @RequestBody Map<String, Object> request) {
        
        try {
            long warningLag = ((Number) request.getOrDefault("warningLag", 1000)).longValue();
            long criticalLag = ((Number) request.getOrDefault("criticalLag", 10000)).longValue();
            double warningError = ((Number) request.getOrDefault("warningError", 0.01)).doubleValue();
            double criticalError = ((Number) request.getOrDefault("criticalError", 0.05)).doubleValue();

            monitoringService.updateThresholds(
                warningLag, criticalLag, warningError, criticalError
            );

            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("warningLag", warningLag);
            result.put("criticalLag", criticalLag);
            result.put("warningError", warningError);
            result.put("criticalError", criticalError);
            
            return ResponseEntity.ok(result);
            
        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    @GetMapping("/batch/metrics")
    public ResponseEntity<Map<String, Object>> getBatchMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("batchProcessedCount", batchProcessingService.getBatchProcessedCount());
        metrics.put("batchErrorCount", batchProcessingService.getBatchErrorCount());
        return ResponseEntity.ok(metrics);
    }

    @PostMapping("/batch/reset-metrics")
    public ResponseEntity<Map<String, Object>> resetBatchMetrics() {
        batchProcessingService.resetMetrics();
        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("message", "Batch metrics reset successfully");
        return ResponseEntity.ok(result);
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> health = new HashMap<>();
        health.put("status", "UP");
        health.put("consumerMonitoring", "ACTIVE");
        health.put("autoScaling", "ACTIVE");
        health.put("timestamp", System.currentTimeMillis());
        return ResponseEntity.ok(health);
    }
}
