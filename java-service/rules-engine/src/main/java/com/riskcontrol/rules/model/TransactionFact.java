package com.riskcontrol.rules.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransactionFact {
    private String transactionId;
    private String userId;
    private String accountId;
    private BigDecimal amount;
    private String transactionType;
    private String recipient;
    private String deviceId;
    private String ipAddress;
    private String location;
    private LocalDateTime transactionTime;

    private int userTransactionCount;
    private BigDecimal userDailyAmount;
    private int userLoginFailures;
    private boolean isNewUser;
    private boolean isNewDevice;
    private boolean isNewLocation;
    private boolean isHighRiskCountry;
    private boolean isSuspiciousRecipient;

    private double ruleRiskScore;
    private String ruleDecision;
    private String ruleReason;
}
