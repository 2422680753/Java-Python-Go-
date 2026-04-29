@echo off
echo ==========================================
echo 金融风控实时反欺诈系统 - 高可用服务启动脚本
echo ==========================================

set DOCKER_DIR=%~dp0

echo.
echo [1/5] 启动基础设施服务 (ZooKeeper, Kafka集群, Redis主从)...
docker-compose -f "%DOCKER_DIR%docker-compose.yml" up -d zookeeper kafka-1 kafka-2 redis-primary redis-replica

echo.
echo 等待基础设施服务就绪 (约45秒)...
timeout /t 45 /nobreak >nul

echo.
echo [2/5] 启动核心服务 (交易网关集群, 规则引擎集群, 实时决策)...
docker-compose -f "%DOCKER_DIR%docker-compose.yml" up -d transaction-gateway-1 transaction-gateway-2 rules-engine-1 rules-engine-2 realtime-decision nginx-lb

echo.
echo 等待核心服务就绪...
timeout /t 20 /nobreak >nul

echo.
echo [3/5] 启动机器学习和数据处理服务...
docker-compose -f "%DOCKER_DIR%docker-compose.yml" up -d ml-risk-assessment data-processing stream-processing cache-management

echo.
echo 等待服务就绪...
timeout /t 15 /nobreak >nul

echo.
echo [4/5] 启动监控系统 (Prometheus, Grafana)...
docker-compose -f "%DOCKER_DIR%docker-compose.yml" up -d prometheus grafana

echo.
echo [5/5] 启动监控工具 (Kafka UI, Redis Insight)...
docker-compose -f "%DOCKER_DIR%docker-compose.yml" up -d kafka-ui redis-insight

echo.
echo ==========================================
echo 服务启动完成！
echo ==========================================
echo.
echo 【负载均衡入口】
echo   - Nginx LB:           http://localhost:80
echo.
echo 【核心服务 - 高可用集群】
echo   - 交易网关集群:
echo     * 实例1: http://localhost:8080 (transaction-gateway-1)
echo     * 实例2: http://localhost:8080 (transaction-gateway-2)
echo   - 规则引擎集群:
echo     * 实例1: http://localhost:8081 (rules-engine-1)
echo     * 实例2: http://localhost:8081 (rules-engine-2)
echo   - 实时决策:         http://localhost:8082
echo   - 机器学习评估:     http://localhost:8083
echo   - 数据处理:         http://localhost:8084
echo   - 流处理:           http://localhost:8085
echo   - 缓存管理:         http://localhost:8086
echo.
echo 【消息队列 - Kafka集群】
echo   - Broker 1:         localhost:19092
echo   - Broker 2:         localhost:29092
echo   - Kafka UI:         http://localhost:8088
echo.
echo 【缓存层 - Redis主从】
echo   - 主节点:           localhost:6379
echo   - 从节点:           localhost:6380
echo   - Redis Insight:    http://localhost:8001
echo.
echo 【监控系统】
echo   - Prometheus:       http://localhost:9090
echo   - Grafana:          http://localhost:3000 (admin/admin123)
echo.
echo 【弹性扩容命令】
echo   - 扩容交易网关:     docker-compose up -d --scale transaction-gateway-1=3 --scale transaction-gateway-2=3
echo   - 扩容规则引擎:     docker-compose up -d --scale rules-engine-1=3 --scale rules-engine-2=3
echo.
echo 【常用管理命令】
echo   - 检查服务状态:     docker-compose ps
echo   - 查看服务日志:     docker-compose logs -f [service_name]
echo   - 停止所有服务:     docker-compose down
echo   - 停止并删除数据:   docker-compose down -v
echo.
