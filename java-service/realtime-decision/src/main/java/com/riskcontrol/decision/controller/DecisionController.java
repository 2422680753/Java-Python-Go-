package com.riskcontrol.decision.controller;

import com.riskcontrol.decision.model.FinalDecision;
import com.riskcontrol.decision.service.DecisionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/decisions")
public class DecisionController {

    @Autowired
    private DecisionService decisionService;

    @GetMapping("/{transactionId}")
    public ResponseEntity<FinalDecision> getFinalDecision(@PathVariable String transactionId) {
        FinalDecision decision = decisionService.getFinalDecision(transactionId);
        return decision != null ? ResponseEntity.ok(decision) : ResponseEntity.notFound().build();
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("实时决策服务运行正常");
    }
}
