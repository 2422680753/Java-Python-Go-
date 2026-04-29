@echo off
echo ==========================================
echo 金融风控实时反欺诈系统 - 服务启动脚本
echo ==========================================

set DOCKER_DIR=%~dp0

echo.
echo [1/4] 启动基础设施服务 (ZooKeeper, Kafka, Redis)...
docker-compose -f "%DOCKER_DIR%docker-compose.yml" up -d zookeeper kafka redis

echo.
echo 等待基础设施服务就绪...
timeout /t 30 /nobreak >nul

echo.
echo [2/4] 启动核心服务...
docker-compose -f "%DOCKER_DIR%docker-compose.yml" up -d transaction-gateway rules-engine realtime-decision

echo.
echo [3/4] 启动机器学习和数据处理服务...
docker-compose -f "%DOCKER_DIR%docker-compose.yml" up -d ml-risk-assessment data-processing stream-processing cache-management

echo.
echo [4/4] 启动监控工具...
docker-compose -f "%DOCKER_DIR%docker-compose.yml" up -d kafka-ui redis-insight

echo.
echo ==========================================
echo 服务启动完成！
echo ==========================================
echo.
echo 服务端口：
echo   - 交易网关:         http://localhost:8080
echo   - 规则引擎:         http://localhost:8081
echo   - 实时决策:         http://localhost:8082
echo   - 机器学习评估:     http://localhost:8083
echo   - 数据处理:         http://localhost:8084
echo   - 流处理:           http://localhost:8085
echo   - 缓存管理:         http://localhost:8086
echo   - Kafka UI:         http://localhost:8088
echo   - Redis Insight:    http://localhost:8001
echo.
echo 检查服务状态: docker-compose ps
echo 查看日志: docker-compose logs -f [service_name]
echo.
