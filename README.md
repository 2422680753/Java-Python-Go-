# 金融风控实时反欺诈系统

构建毫秒级极速响应防线，本系统实时扫描每一笔交易数据，精准识别潜在欺诈行为并即时拦截。以超低延迟构筑动态风控屏障，有效抵御异常交易风险，保障资金流转安全。

## 系统架构

### 整体架构
采用微服务架构设计，通过 Kafka 消息队列实现异步解耦，Redis 作为高速缓存层，实现毫秒级响应。

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              客户端 / 交易系统                                  │
└─────────────────────────────┬─────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         交易网关服务 (Java - 8080)                            │
│  - 交易请求接收与验证                                                         │
│  - 黑白名单快速检查                                                            │
│  - 交易数据发送到 Kafka                                                        │
└─────────────────────────────┬─────────────────────────────────────────────────┘
                              │
                              ▼ Kafka Topic: transactions
┌─────────────────────────────────────────────────────────────────────────────┐
│                      ┌─────────────────────────────────────┐                  │
│                      │         Kafka 消息队列               │                  │
│                      └─────────────────────────────────────┘                  │
└─────────────────────────────┬─────────────────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│  规则引擎服务    │ │  流处理服务      │ │  数据处理服务    │
│  (Java - 8081)  │ │  (Go - 8085)    │ │  (Go - 8084)    │
│  - Drools规则   │ │  - 实时异常检测  │ │  - 高性能处理   │
│  - 规则动态更新  │ │  - 用户行为分析  │ │  - 统计信息更新 │
└────────┬────────┘ └────────┬────────┘ └────────┬────────┘
         │                    │                    │
         ▼                    ▼                    │
┌─────────────────┐ ┌─────────────────┐          │
│规则结果主题     │ │ML结果主题       │          │
│rules-results    │ │ml-results       │          │
└────────┬────────┘ └────────┬────────┘          │
         │                    │                    │
         └─────────┬──────────┴───────────────────┘
                   ▼
         ┌─────────────────────────┐
         │   实时决策服务           │
         │   (Java - 8082)         │
         │  - 综合规则和ML结果      │
         │  - 权重融合决策          │
         │  - 最终决策输出          │
         └───────────┬─────────────┘
                     ▼
         ┌─────────────────────────┐
         │   机器学习评估服务       │
         │   (Python - 8083)       │
         │  - XGBoost模型预测       │
         │  - 特征工程处理          │
         │  - 模型训练与管理        │
         └───────────┬─────────────┘
                     ▼
         ┌─────────────────────────┐
         │   缓存管理服务           │
         │   (Go - 8086)           │
         │  - 黑白名单管理          │
         │  - 缓存统一接口          │
         │  - 过期时间管理          │
         └─────────────────────────┘
```

## 技术栈

### Java 服务
- **Spring Boot 2.7** - 应用框架
- **Spring Kafka** - 消息队列集成
- **Spring Data Redis** - 缓存操作
- **Drools 7.57** - 规则引擎
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
- **Kafka** - 消息队列
- **Redis** - 缓存/数据存储
- **Docker** - 容器化部署

## 项目结构

```
Java-Python-Go-
├── java-service/                 # Java 微服务
│   ├── transaction-gateway/      # 交易网关服务
│   │   └── src/main/
│   │       ├── java/com/riskcontrol/gateway/
│   │       │   ├── controller/   # 控制器
│   │       │   ├── model/        # 数据模型
│   │       │   ├── service/      # 业务逻辑
│   │       │   └── TransactionGatewayApplication.java
│   │       └── resources/
│   │           └── application.yml
│   ├── rules-engine/             # 规则引擎服务
│   │   └── src/main/
│   │       ├── java/com/riskcontrol/rules/
│   │       │   ├── config/       # Drools配置
│   │       │   ├── model/        # 事实模型
│   │       │   ├── service/      # 规则服务
│   │       │   └── RulesEngineApplication.java
│   │       └── resources/
│   │           ├── rules/
│   │           │   └── risk-rules.drl  # 规则定义
│   │           └── application.yml
│   ├── realtime-decision/        # 实时决策服务
│   │   └── src/main/
│   │       ├── java/com/riskcontrol/decision/
│   │       │   ├── controller/   # 控制器
│   │       │   ├── model/        # 决策模型
│   │       │   ├── service/      # 决策逻辑
│   │       │   └── RealtimeDecisionApplication.java
│   │       └── resources/
│   │           └── application.yml
│   └── pom.xml                   # Maven父项目
├── python-service/               # Python 服务
│   ├── model-training/           # 模型训练
│   │   └── train_model.py        # 训练脚本
│   ├── risk-assessment/          # 风险评估服务
│   │   └── app.py                # FastAPI应用
│   ├── requirements.txt          # Python依赖
│   └── Dockerfile
├── go-service/                   # Go 服务
│   ├── data-processing/          # 数据处理服务
│   │   └── main.go
│   ├── stream-processing/        # 流处理服务
│   │   └── main.go
│   ├── cache-management/         # 缓存管理服务
│   │   └── main.go
│   ├── go.mod
│   ├── Dockerfile-data
│   ├── Dockerfile-stream
│   └── Dockerfile-cache
├── docker/                       # Docker配置
│   ├── docker-compose.yml        # 编排文件
│   └── start-services.bat        # 启动脚本
├── configs/                      # 配置文件
│   └── .env.example              # 环境变量示例
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

2. **启动所有服务**
```bash
# Windows
docker\start-services.bat

# 或直接使用 docker-compose
cd docker
docker-compose up -d
```

3. **查看服务状态**
```bash
docker-compose ps
```

4. **查看服务日志**
```bash
docker-compose logs -f transaction-gateway
```

### 本地开发

1. **启动基础设施**
```bash
cd docker
docker-compose up -d zookeeper kafka redis
```

2. **Java 服务**
```bash
cd java-service
mvn clean install
mvn spring-boot:run -pl transaction-gateway
```

3. **Python 服务**
```bash
cd python-service
pip install -r requirements.txt

# 训练模型
python model-training/train_model.py

# 启动评估服务
cd risk-assessment
uvicorn app:app --reload --port 8083
```

4. **Go 服务**
```bash
cd go-service

# 数据处理服务
cd data-processing
go run main.go

# 流处理服务
cd ../stream-processing
go run main.go

# 缓存管理服务
cd ../cache-management
go run main.go
```

## API 文档

### 交易网关 (端口 8080)

**提交交易**
```bash
POST /api/transactions

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

**查询交易结果**
```bash
GET /api/transactions/{transactionId}
```

### 缓存管理 (端口 8086)

**添加到黑名单**
```bash
POST /api/blacklist

{
  "id": "user_123",
  "type": "user",
  "reason": "可疑活动",
  "expireAt": "2024-12-31T23:59:59Z"
}
```

**检查黑名单**
```bash
GET /api/blacklist/{type}/{id}
```

**添加到白名单**
```bash
POST /api/whitelist

{
  "id": "user_456",
  "type": "user",
  "reason": "VIP用户",
  "expireAt": "2025-12-31T23:59:59Z"
}
```

### 机器学习评估 (端口 8083)

**评估交易风险**
```bash
POST /api/assess

{
  "transaction_id": "txn_001",
  "user_id": "user_123",
  "amount": 10000.00,
  "transaction_type": "transfer",
  "is_new_user": false,
  "is_new_device": true,
  "is_new_location": false,
  "user_transaction_count": 50,
  "login_failures_24h": 0
}
```

## 风控规则

### 内置规则 (Drools)

| 规则名称 | 触发条件 | 风险评分 | 决策 |
|---------|---------|---------|------|
| 大额交易风险 | 金额 > 10万 | +0.3 | REVIEW |
| 新用户首次交易 | 新用户且交易数=0 | +0.2 | REVIEW |
| 新设备交易 | 新设备登录 | +0.15 | REVIEW |
| 新位置交易 | 新地理位置 | +0.15 | REVIEW |
| 登录失败多次 | 登录失败≥3次 | +0.4 | REJECT |
| 单日累计过高 | 单日累计>50万 | +0.25 | REVIEW |
| 高风险国家 | 来自高风险地区 | +0.5 | REJECT |

### 决策权重

- 规则引擎: 60%
- 机器学习模型: 40%

### 风险阈值

- **≥ 0.7**: REJECT (拒绝)
- **≥ 0.4 且 < 0.7**: REVIEW (人工审核)
- **< 0.4**: APPROVE (通过)

## 监控工具

### Kafka UI
- 地址: http://localhost:8088
- 功能: 消息队列监控、主题管理、消费者组监控

### Redis Insight
- 地址: http://localhost:8001
- 功能: 缓存数据可视化、性能监控、内存分析

## 性能特点

### 低延迟设计
- **异步处理**: 基于 Kafka 的异步消息队列
- **多级缓存**: Redis 缓存热点数据
- **并行计算**: Go 协程、Java 线程池
- **预加载**: 规则、模型预加载到内存

### 高可用
- **无状态服务**: 所有服务可水平扩展
- **消息持久化**: Kafka 消息持久化存储
- **故障恢复**: Redis 持久化 + 服务自动重启

### 可扩展性
- **微服务架构**: 各服务独立部署和扩展
- **动态配置**: 规则可热更新，无需重启
- **API 接口**: 标准化 RESTful API

## 服务端口

| 服务 | 端口 | 说明 |
|-----|------|------|
| transaction-gateway | 8080 | 交易网关 |
| rules-engine | 8081 | 规则引擎 |
| realtime-decision | 8082 | 实时决策 |
| ml-risk-assessment | 8083 | 机器学习评估 |
| data-processing | 8084 | 数据处理 |
| stream-processing | 8085 | 流处理 |
| cache-management | 8086 | 缓存管理 |
| kafka-ui | 8088 | Kafka监控 |
| redis-insight | 8001 | Redis监控 |

## 开发指南

### 添加新规则
1. 编辑 `java-service/rules-engine/src/main/resources/rules/risk-rules.drl`
2. 重启规则引擎服务（或配置热加载）

### 训练新模型
1. 准备训练数据
2. 修改 `python-service/model-training/train_model.py`
3. 运行训练脚本
4. 新模型自动加载到评估服务

### 添加新的决策规则
修改 `java-service/realtime-decision/src/main/java/com/riskcontrol/decision/service/DecisionService.java` 中的决策逻辑。

## 许可证

本项目仅供学习和研究使用。

---

**构建毫秒级极速响应防线，保障资金流转安全。**
