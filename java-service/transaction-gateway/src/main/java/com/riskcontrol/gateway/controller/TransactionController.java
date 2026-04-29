package com.riskcontrol.gateway.controller;

import com.riskcontrol.gateway.model.RiskResult;
import com.riskcontrol.gateway.model.Transaction;
import com.riskcontrol.gateway.service.TransactionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.UUID;

@RestController
@RequestMapping("/api/transactions")
public class TransactionController {

    @Autowired
    private TransactionService transactionService;

    @PostMapping
    public ResponseEntity<RiskResult> processTransaction(@Valid @RequestBody Transaction transaction) {
        if (transaction.getTransactionId() == null) {
            transaction.setTransactionId(UUID.randomUUID().toString());
        }
        RiskResult result = transactionService.processTransaction(transaction);
        return ResponseEntity.ok(result);
    }

    @GetMapping("/{transactionId}")
    public ResponseEntity<RiskResult> getTransactionResult(@PathVariable String transactionId) {
        RiskResult result = transactionService.getTransactionResult(transactionId);
        return result != null ? ResponseEntity.ok(result) : ResponseEntity.notFound().build();
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("交易网关服务运行正常");
    }
}
