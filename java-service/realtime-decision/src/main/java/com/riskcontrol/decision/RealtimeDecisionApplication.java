package com.riskcontrol.decision;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@SpringBootApplication
@EnableDiscoveryClient
public class RealtimeDecisionApplication {
    public static void main(String[] args) {
        SpringApplication.run(RealtimeDecisionApplication.class, args);
    }
}
