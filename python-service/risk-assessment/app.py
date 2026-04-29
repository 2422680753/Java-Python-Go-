import os
import sys
import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager

import joblib
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MODEL_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'model-training', 'models')

model = None
preprocessor = None
redis_client = None
kafka_producer = None
kafka_consumer = None


class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float
    transaction_type: str
    hour_of_day: Optional[int] = None
    day_of_week: Optional[int] = None
    is_new_user: bool = False
    is_new_device: bool = False
    is_new_location: bool = False
    user_transaction_count: int = 0
    user_daily_amount: float = 0.0
    login_failures_24h: int = 0
    is_high_risk_country: bool = False
    suspicious_recipient: bool = False
    device_id: Optional[str] = None
    ip_address: Optional[str] = None
    location: Optional[str] = None


class RiskAssessmentResult(BaseModel):
    transaction_id: str
    user_id: str
    decision: str
    risk_score: float
    risk_factors: Dict[str, Any]
    reason: str
    assessment_time: str


def load_model():
    global model, preprocessor
    
    model_path = os.path.join(MODEL_DIR, 'fraud_detection_model.pkl')
    preprocessor_path = os.path.join(MODEL_DIR, 'preprocessor.pkl')
    
    if os.path.exists(model_path) and os.path.exists(preprocessor_path):
        logger.info("加载已训练的模型...")
        model = joblib.load(model_path)
        preprocessor = joblib.load(preprocessor_path)
        logger.info("模型加载完成")
    else:
        logger.warning("模型文件不存在，请先运行 train_model.py 训练模型")
        from model_training.train_model import train_model
        logger.info("开始训练模型...")
        model, preprocessor = train_model()


def init_redis():
    global redis_client
    try:
        redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=int(os.getenv('REDIS_DB', 3)),
            decode_responses=True
        )
        redis_client.ping()
        logger.info("Redis连接成功")
    except Exception as e:
        logger.warning(f"Redis连接失败: {e}，将使用内存存储")


def init_kafka():
    global kafka_producer, kafka_consumer
    
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    
    try:
        kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks=1
        )
        logger.info("Kafka生产者初始化成功")
    except Exception as e:
        logger.warning(f"Kafka生产者初始化失败: {e}")
    
    try:
        kafka_consumer = KafkaConsumer(
            'transactions',
            bootstrap_servers=kafka_servers,
            group_id='ml-risk-assessment',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logger.info("Kafka消费者初始化成功")
    except Exception as e:
        logger.warning(f"Kafka消费者初始化失败: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_model()
    init_redis()
    init_kafka()
    yield
    if kafka_producer:
        kafka_producer.close()
    if kafka_consumer:
        kafka_consumer.close()


app = FastAPI(
    title="机器学习风险评估服务",
    description="使用训练好的机器学习模型进行实时交易风险评估",
    version="1.0.0",
    lifespan=lifespan
)


def assess_risk(transaction: Transaction) -> RiskAssessmentResult:
    if model is None or preprocessor is None:
        return RiskAssessmentResult(
            transaction_id=transaction.transaction_id,
            user_id=transaction.user_id,
            decision="REVIEW",
            risk_score=0.5,
            risk_factors={"error": "模型未加载"},
            reason="模型未加载，需要人工审核",
            assessment_time=datetime.now().isoformat()
        )
    
    now = datetime.now()
    hour_of_day = transaction.hour_of_day if transaction.hour_of_day is not None else now.hour
    day_of_week = transaction.day_of_week if transaction.day_of_week is not None else now.weekday()
    
    input_data = pd.DataFrame([{
        'amount': transaction.amount,
        'transaction_type': transaction.transaction_type,
        'hour_of_day': hour_of_day,
        'day_of_week': day_of_week,
        'is_new_user': int(transaction.is_new_user),
        'is_new_device': int(transaction.is_new_device),
        'is_new_location': int(transaction.is_new_location),
        'user_transaction_count': transaction.user_transaction_count,
        'user_daily_amount': transaction.user_daily_amount,
        'login_failures_24h': transaction.login_failures_24h,
        'is_high_risk_country': int(transaction.is_high_risk_country),
        'suspicious_recipient': int(transaction.suspicious_recipient)
    }])
    
    try:
        processed_data = preprocessor.transform(input_data)
        risk_score = float(model.predict_proba(processed_data)[0][1])
        
        risk_factors = {
            "amount": transaction.amount,
            "transaction_type": transaction.transaction_type,
            "is_new_user": transaction.is_new_user,
            "is_new_device": transaction.is_new_device,
            "is_new_location": transaction.is_new_location,
            "user_transaction_count": transaction.user_transaction_count,
            "login_failures_24h": transaction.login_failures_24h,
            "is_high_risk_country": transaction.is_high_risk_country,
            "suspicious_recipient": transaction.suspicious_recipient
        }
        
        if risk_score >= 0.7:
            decision = "REJECT"
            reason = f"高风险交易: 风险评分 {risk_score:.2f}，超过阈值"
        elif risk_score >= 0.4:
            decision = "REVIEW"
            reason = f"中等风险交易: 风险评分 {risk_score:.2f}，需要人工审核"
        else:
            decision = "APPROVE"
            reason = f"低风险交易: 风险评分 {risk_score:.2f}，自动通过"
        
        return RiskAssessmentResult(
            transaction_id=transaction.transaction_id,
            user_id=transaction.user_id,
            decision=decision,
            risk_score=risk_score,
            risk_factors=risk_factors,
            reason=reason,
            assessment_time=datetime.now().isoformat()
        )
        
    except Exception as e:
        logger.error(f"风险评估失败: {e}")
        return RiskAssessmentResult(
            transaction_id=transaction.transaction_id,
            user_id=transaction.user_id,
            decision="REVIEW",
            risk_score=0.5,
            risk_factors={"error": str(e)},
            reason="评估过程出错，需要人工审核",
            assessment_time=datetime.now().isoformat()
        )


@app.post("/api/assess", response_model=RiskAssessmentResult)
async def assess_transaction(transaction: Transaction):
    result = assess_risk(transaction)
    
    if redis_client:
        key = f"ml:result:{transaction.transaction_id}"
        redis_client.setex(key, 86400, json.dumps(result.dict()))
    
    if kafka_producer:
        kafka_producer.send('ml-results', value=result.dict())
    
    return result


@app.get("/api/result/{transaction_id}")
async def get_assessment_result(transaction_id: str):
    if redis_client:
        key = f"ml:result:{transaction_id}"
        result = redis_client.get(key)
        if result:
            return json.loads(result)
    
    raise HTTPException(status_code=404, detail="评估结果未找到")


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "model_loaded": model is not None,
        "redis_connected": redis_client is not None,
        "kafka_connected": kafka_producer is not None
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8083)
