# 金融风控实时反欺诈系统

构建毫秒级极速响应防线，本系统实时扫描每一笔交易数据，精准识别潜在欺诈行为并即时拦截。以超低延迟构筑动态风控屏障，有效抵御异常交易风险，保障资金流转安全。

## 系统架构

### 高可用微服务架构

采用**高可用集群架构**，支持弹性扩容、故障转移、限流熔断，确保突发流量下系统稳定运行。

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              客户端 / 交易系统                                      │
└─────────────────────────────┬─────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Nginx 负载均衡器 (端口 80)                                │
│  - least_conn 负载均衡策略                                                         │
│  - 健康检查 + 故障转移                                                              │
│  - 长连接保持 + Gzip 压缩                                                          │
└─────────────────────────────┬─────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    交易网关集群 (2实例) - 限流+熔断+降级                          │
│  ┌─────────────────────┐  ┌─────────────────────┐                               │
│  │ transaction-gateway-1│  │ transaction-gateway-2│                               │
│  │ Resilience4j 限流    │  │ Resilience4j 限流    │                               │
│  │ 断路器+降级服务       │  │ 断路器+降级服务       │                               │
│  └─────────────────────┘  └─────────────────────┘                               │
└─────────────────────────────┬─────────────────────────────────────────────────────┘
                              │
                              ▼ Kafka Topic: transactions (3分区, 2副本)
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Kafka 集群 (2 Broker)                                    │
│  ┌─────────────────┐  ┌─────────────────┐                                        │
│  │    kafka-1      │  │    kafka-2      │                                        │
│  │  Broker ID: 1   │  │  Broker ID: 2   │                                        │
│  │ 端口: 19092     │  │ 端口: 29092     │                                        │
│  └─────────────────┘  └─────────────────┘                                        │
└─────────────────────────────┬─────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┬───────────────────┐
          ▼                   ▼                   ▼                   ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│  规则引擎集群    │ │  ML评估服务     │ │  流处理服务      │ │  数据处理服务    │
│  (2实例)        │ │  (Python)       │ │  (Go)           │ │  (Go)           │
│  - Drools规则   │ │  - XGBoost      │ │  - 实时异常检测  │ │  - 高性能处理   │
│  - 动态评分      │ │  - 特征工程      │ │  - 行为分析      │ │  - 统计更新      │
│  - 多级阈值      │ │  - 流量预测      │ │  - 扩缩容决策    │ │  - 用户画像      │
└────────┬────────┘ └────────┬────────┘ └────────┬────────┘ └────────┬────────┘
         │                    │                    │                    │
         └────────────────────┴────────────────────┴────────────────────┘
                              │
                              ▼
         ┌─────────────────────────────────────────────────────────────┐
         │                    实时决策服务 (Java)                        │
         │  - 综合规则+ML结果 (60%+40%)                                 │
         │  - 动态权重融合                                              │
         │  - 最终决策输出                                              │
         └─────────────────────────────┬───────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Redis 主从复制 (高可用)                                   │
│  ┌─────────────────┐  ┌─────────────────┐                                        │
│  │  redis-primary  │  │  redis-replica  │                                        │
│  │  主节点 (读写)   │  │  从节点 (只读)   │                                        │
│  │  端口: 6379     │  │  端口: 6380     │                                        │
│  └─────────────────┘  └─────────────────┘                                        │
└─────────────────────────────┬─────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         监控系统 (Prometheus + Grafana)                          │
│  - Prometheus: 指标采集 (9090)                                                  │
│  - Grafana: 可视化仪表盘 (3000)                                                  │
│  - 交易网关/规则引擎/决策服务 实时监控                                           │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## 核心优化功能

### 1. 优化的风控模型阈值

#### 多级风险阈值 (降低误杀率)

| 风险区间 | 决策 | 说明 |
|---------|------|------|
| **≥ 0.8** | REJECT (拒绝) | 高风险，自动拒绝 |
| **0.6 ~ 0.8** | REVIEW (重点审核) | 中高风险，需要重点审核 |
| **0.4 ~ 0.6** | REVIEW (审核) | 中等风险，需要审核 |
| **0.25 ~ 0.4** | APPROVE (通过) | 中低风险，自动通过 |
| **< 0.25** | APPROVE (通过) | 低风险，自动通过 |

#### 动态评分机制

- **用户信任度权重**: 高信任用户风险评分降低 30%
- **历史交易记录**: 有大额交易成功历史的用户评分适度增加
- **行为一致性**: 一致性高的用户风险评分降低 20%
- **时间上下文**: 非高峰时段风险评分降低

### 2. 大额交易白名单

#### 白名单类型

| 类型 | 说明 | 适用场景 |
|-----|------|---------|
| **用户白名单** | 基于用户ID | VIP用户、企业客户 |
| **账户白名单** | 基于账户ID | 常用账户、对公账户 |
| **VIP白名单** | 特殊VIP用户 | 高净值客户、钻石会员 |

#### 白名单管理 API

```bash
# 添加大额交易白名单 (交易网关)
POST /api/transactions/whitelist/add

{
  "type": "user",
  "id": "user_vip_001",
  "maxAmount": 1000000.00,
  "maxDailyTransactions": 50,
  "expireSeconds": 2592000
}

# 从白名单移除
POST /api/transactions/whitelist/remove

{
  "type": "user",
  "id": "user_vip_001"
}
```

### 3. 限流、熔断、降级策略

#### 限流配置 (Resilience4j RateLimiter)

| 限流器 | 限制 | 刷新周期 | 说明 |
|-------|------|---------|------|
| transaction-rate | 5000次/秒 | 1秒 | 交易请求限流 |
| heavy-operation | 100次/秒 | 1秒 | 耗时操作限流 |

#### 熔断配置 (Resilience4j CircuitBreaker)

| 参数 | 值 | 说明 |
|-----|-----|------|
| 失败率阈值 | 40% | 超过则打开断路器 |
| 慢调用率阈值 | 50% | 超过则打开断路器 |
| 慢调用时长 | 1秒 | 超过算慢调用 |
| 等待时间 | 15秒 | 半开状态等待 |
| 半开调用数 | 10次 | 半开状态测试调用数 |

#### 降级策略

```java
// 快速评估降级逻辑
public RiskResult processTransactionFallback(Transaction transaction, Exception ex) {
    // 小额交易快速通过
    if (transaction.getAmount() < 1000) {
        return RiskResult.approve(transactionId, 0.1, factors, "降级模式：小额快速通过");
    }
    
    // 大额交易需要审核
    return RiskResult.review(transactionId, 0.5, factors, "降级模式：需要人工审核");
}
```

### 4. 高可用架构

#### Kafka 集群配置

- **Broker数量**: 2节点 (kafka-1, kafka-2)
- **Topic分区**: 3分区
- **副本因子**: 2副本
- **ISR最小**: 2个副本

#### Redis 主从配置

- **主节点**: redis-primary (端口 6379) - 读写
- **从节点**: redis-replica (端口 6380) - 只读
- **持久化**: AOF 持久化

#### 服务集群

| 服务 | 实例数 | 资源限制 |
|-----|-------|---------|
| 交易网关 | 2实例 | 2核/512MB |
| 规则引擎 | 2实例 | 2核/1GB |
| 实时决策 | 1实例 | 2核/512MB |
| ML评估 | 1实例 | 4核/2GB |

### 5. 流量预测与智能负载均衡

#### 流量预测模型

- **TPS预测**: GradientBoostingRegressor
- **延迟预测**: RandomForestRegressor
- **特征工程**: 小时、分钟、星期、历史滞后、滚动统计

#### 弹性扩缩容决策

| 条件 | 建议操作 | 置信度 |
|-----|---------|--------|
| 预测 TPS > 3000 * 1.2 | SCALE_OUT (扩容) | 85% |
| 预测 TPS > 3000 | SCALE_OUT (扩容) | 70% |
| 预测 TPS < 500 * 0.5 | SCALE_IN (缩容) | 75% |

#### Nginx 负载均衡策略

```nginx
upstream transaction_gateways {
    least_conn;  # 最少连接策略
    server transaction-gateway-1:8080 max_fails=3 fail_timeout=30s;
    server transaction-gateway-2:8080 max_fails=3 fail_timeout=30s;
    keepalive 32;  # 长连接保持
}

# 故障转移配置
proxy_next_upstream error timeout invalid_header http_500 http_502 http_503 http_504;
proxy_next_upstream_tries 2;
```

### 6. 监控系统

#### Prometheus 监控目标

| 目标 | 端点 | 采样间隔 |
|-----|------|---------|
| 交易网关 | /actuator/prometheus | 5秒 |
| 规则引擎 | /actuator/prometheus | 5秒 |
| 实时决策 | /actuator/prometheus | 5秒 |
| Kafka | /metrics | 15秒 |
| Nginx | /nginx_status | 15秒 |

#### Grafana 仪表盘

- **TPS监控**: 实时交易吞吐量
- **延迟监控**: P99、P95、平均延迟
- **错误率监控**: 5xx错误率
- **熔断器状态**: 各服务熔断器状态
- **Kafka消息积压**: 消费者组滞后

## 技术栈

### Java 服务
- **Spring Boot 2.7** - 应用框架
- **Spring Kafka** - 消息队列集成
- **Spring Data Redis** - 缓存操作
- **Drools 7.57** - 规则引擎
- **Resilience4j 1.7.1** - 熔断/限流/降级
- **Lombok** - 代码简化

### Python 服务
- **FastAPI** - Web 框架
- **XGBoost** - 机器学习模型
- **Scikit-learn** - 数据处理
- **Pandas/Numpy** - 数值计算
- **Kafka-Python** - Kafka 客户端
- **Redis-Py** - Redis 客户端

### Go 服务
- **Gin** - Web 框架
- **Segmentio/Kafka-Go** - Kafka 客户端
- **Go-Redis** - Redis 客户端
- **Zap** - 日志框架
- **Viper** - 配置管理

### 基础设施
- **Kafka 集群** - 消息队列 (2 Broker)
- **Redis 主从** - 缓存/数据存储
- **Nginx** - 负载均衡
- **Prometheus** - 监控采集
- **Grafana** - 可视化
- **Docker** - 容器化部署

## 项目结构

```
Java-Python-Go-
├── java-service/                     # Java 微服务
│   ├── transaction-gateway/          # 交易网关服务
│   │   └── src/main/
│   │       ├── java/com/riskcontrol/gateway/
│   │       │   ├── config/           # Resilience4j配置
│   │       │   ├── controller/       # 控制器
│   │       │   ├── model/            # 数据模型
│   │       │   ├── service/          # 业务逻辑
│   │       │   │   ├── TransactionService.java    # 核心服务(限流熔断)
│   │       │   │   └── FallbackService.java       # 降级服务
│   │       │   └── TransactionGatewayApplication.java
│   │       └── resources/
│   │           └── application.yml    # Resilience4j配置
│   ├── rules-engine/                 # 规则引擎服务
│   │   └── src/main/
│   │       ├── java/com/riskcontrol/rules/
│   │       │   ├── config/           # 动态阈值配置
│   │       │   ├── model/            # 事实模型
│   │       │   ├── service/          # 规则服务
│   │       │   │   ├── RiskRulesService.java
│   │       │   │   └── DynamicScoringService.java   # 动态评分
│   │       │   └── RulesEngineApplication.java
│   │       └── resources/
│   │           ├── rules/
│   │           │   └── risk-rules.drl  # 优化规则定义
│   │           └── application.yml
│   ├── realtime-decision/            # 实时决策服务
│   │   └── src/main/
│   │       └── ...
│   └── pom.xml
├── python-service/                   # Python 服务
│   ├── model-training/               # 模型训练
│   │   ├── train_model.py            # 欺诈检测模型
│   │   └── traffic_predictor.py      # 流量预测模型
│   ├── risk-assessment/              # 风险评估服务
│   │   └── app.py
│   ├── requirements.txt
│   └── Dockerfile
├── go-service/                       # Go 服务
│   ├── data-processing/
│   ├── stream-processing/
│   ├── cache-management/
│   └── go.mod
├── docker/                           # Docker配置
│   ├── docker-compose.yml            # 高可用编排
│   ├── nginx.conf                    # 负载均衡配置
│   ├── prometheus.yml                # 监控配置
│   └── start-services.bat            # 启动脚本
├── configs/
│   └── .env.example
└── README.md
```

## 快速开始

### 环境要求
- Docker & Docker Compose
- Java 17+ (本地开发)
- Python 3.11+ (本地开发)
- Go 1.19+ (本地开发)

### 使用 Docker 启动

1. **克隆项目**
```bash
cd Java-Python-Go-
```

2. **启动所有服务 (高可用集群)**
```bash
# Windows
docker\start-services.bat

# 或直接使用 docker-compose
cd docker
docker-compose up -d
```

3. **弹性扩容命令**
```bash
# 扩容交易网关到4实例
docker-compose up -d --scale transaction-gateway-1=2 --scale transaction-gateway-2=2

# 扩容规则引擎到4实例
docker-compose up -d --scale rules-engine-1=2 --scale rules-engine-2=2
```

4. **查看服务状态**
```bash
docker-compose ps
```

### 访问入口

| 服务 | 地址 | 说明 |
|-----|------|------|
| **负载均衡入口** | http://localhost:80 | Nginx LB |
| 交易网关 | http://localhost:8080 | 直接访问 |
| 规则引擎 | http://localhost:8081 | 直接访问 |
| 实时决策 | http://localhost:8082 | 直接访问 |
| ML评估 | http://localhost:8083 | 直接访问 |
| Kafka UI | http://localhost:8088 | Kafka监控 |
| Redis Insight | http://localhost:8001 | Redis监控 |
| Prometheus | http://localhost:9090 | 监控采集 |
| **Grafana** | http://localhost:3000 | 仪表盘 (admin/admin123) |

## API 文档

### 提交交易 (负载均衡入口)

```bash
POST http://localhost:80/api/transactions
Content-Type: application/json

{
  "userId": "user_123",
  "accountId": "acc_456",
  "amount": 5000.00,
  "transactionType": "transfer",
  "recipient": "user_789",
  "deviceId": "device_001",
  "ipAddress": "192.168.1.1",
  "location": "Beijing"
}
```

### 响应示例

```json
{
  "transactionId": "txn_123456",
  "decision": "APPROVE",
  "riskScore": 0.15,
  "riskFactors": {
    "userTrustLevel": 0.85,
    "behaviorConsistency": 0.92
  },
  "reason": "低风险交易，自动通过",
  "decisionTime": "2024-01-15T10:30:00"
}
```

### 熔断器健康检查

```bash
GET http://localhost:8080/actuator/health
GET http://localhost:8080/actuator/circuitbreakers
GET http://localhost:8080/actuator/ratelimiters
```

## 性能指标

### 目标性能

| 指标 | 目标值 | 说明 |
|-----|-------|------|
| **P99 延迟** | < 500ms | 99%交易处理延迟 |
| **P95 延迟** | < 200ms | 95%交易处理延迟 |
| **吞吐量** | 10000+ TPS | 单集群 |
| **可用性** | 99.99% | 年度 |
| **误杀率** | < 1% | 优化后 |

### 限流阈值

| 服务 | 限流值 | 说明 |
|-----|-------|------|
| 交易网关 | 5000 TPS/实例 | Resilience4j限流 |
| 规则引擎 | 2000 TPS/实例 | Drools规则评估 |
| ML评估 | 500 TPS/实例 | XGBoost预测 |

## 故障处理

### 熔断器状态流转

```
CLOSED (正常) 
    │
    ▼ 失败率 > 40% 或 慢调用率 > 50%
OPEN (熔断)
    │
    ▼ 等待 15秒
HALF_OPEN (半开)
    │
    ├─ 测试调用成功 ──► CLOSED (恢复)
    │
    └─ 测试调用失败 ──► OPEN (保持熔断)
```

### 降级策略

| 场景 | 降级行为 | 影响 |
|-----|---------|------|
| 交易网关熔断 | 快速评估模式 | 小额通过，大额审核 |
| 规则引擎熔断 | 使用ML单模型 | 精度略有下降 |
| Kafka不可用 | 内存队列缓冲 | 临时存储 |
| Redis不可用 | 数据库降级 | 延迟增加 |

## 开发指南

### 添加新规则

1. 编辑 `java-service/rules-engine/src/main/resources/rules/risk-rules.drl`
2. 规则支持动态阈值配置
3. 重启规则引擎服务

### 调整阈值

修改 `java-service/rules-engine/src/main/resources/application.yml`:

```yaml
risk:
  threshold:
    large-transaction-threshold: 100000
    reject-threshold: 0.8
    review-threshold: 0.5
    approve-threshold: 0.25
```

### 训练流量预测模型

```bash
cd python-service/model-training
python traffic_predictor.py
```

## 许可证

本项目仅供学习和研究使用。

---

**构建毫秒级极速响应防线，保障资金流转安全。**

**高可用、可弹性扩容、限流熔断保护，确保突发流量下系统稳定运行。**
