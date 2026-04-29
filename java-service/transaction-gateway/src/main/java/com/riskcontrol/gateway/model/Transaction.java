package com.riskcontrol.gateway.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {
    private String transactionId;

    @NotBlank(message = "用户ID不能为空")
    private String userId;

    @NotBlank(message = "账户ID不能为空")
    private String accountId;

    @NotNull(message = "交易金额不能为空")
    @Positive(message = "交易金额必须大于0")
    private BigDecimal amount;

    @NotBlank(message = "交易类型不能为空")
    private String transactionType;

    @NotBlank(message = "收款方信息不能为空")
    private String recipient;

    private String deviceId;
    private String ipAddress;
    private String location;

    private LocalDateTime transactionTime;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
