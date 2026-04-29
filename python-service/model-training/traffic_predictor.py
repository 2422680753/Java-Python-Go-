import os
import sys
import json
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from collections import deque

import joblib
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MODEL_DIR = os.path.join(os.path.dirname(__file__), 'models')
os.makedirs(MODEL_DIR, exist_ok=True)


class TrafficPredictor:
    def __init__(self):
        self.tps_model = None
        self.latency_model = None
        self.scaler = None
        self.tps_history = deque(maxlen=1000)
        self.latency_history = deque(maxlen=1000)
        self.timestamps = deque(maxlen=1000)
        
    def generate_historical_data(self, hours: int = 24 * 7) -> pd.DataFrame:
        logger.info(f"生成 {hours} 小时的历史流量数据...")
        
        data_points = hours * 12
        timestamps = [datetime.now() - timedelta(minutes=5 * i) for i in range(data_points)]
        timestamps.reverse()
        
        tps_values = []
        latency_values = []
        
        for ts in timestamps:
            hour = ts.hour
            
            if 9 <= hour < 18:
                base_tps = 1000 + np.random.normal(0, 200)
                base_latency = 50 + np.random.normal(0, 10)
            elif 18 <= hour < 22:
                base_tps = 2000 + np.random.normal(0, 400)
                base_latency = 80 + np.random.normal(0, 15)
            elif 22 <= hour or hour < 6:
                base_tps = 200 + np.random.normal(0, 50)
                base_latency = 30 + np.random.normal(0, 5)
            else:
                base_tps = 500 + np.random.normal(0, 100)
                base_latency = 40 + np.random.normal(0, 8)
            
            if ts.weekday() >= 5:
                base_tps *= 1.5
                base_latency *= 1.2
            
            if np.random.random() < 0.05:
                base_tps *= 3
                base_latency *= 2
            
            tps_values.append(max(10, base_tps))
            latency_values.append(max(1, base_latency))
        
        df = pd.DataFrame({
            'timestamp': timestamps,
            'tps': tps_values,
            'latency_ms': latency_values
        })
        
        df['hour'] = df['timestamp'].dt.hour
        df['minute'] = df['timestamp'].dt.minute
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
        df['is_peak_hour'] = ((df['hour'] >= 9) & (df['hour'] < 18) | 
                               (df['hour'] >= 18) & (df['hour'] < 22)).astype(int)
        df['is_off_peak'] = ((df['hour'] >= 22) | (df['hour'] < 6)).astype(int)
        
        df['tps_lag_1'] = df['tps'].shift(1)
        df['tps_lag_2'] = df['tps'].shift(2)
        df['tps_lag_3'] = df['tps'].shift(3)
        df['tps_rolling_mean_5'] = df['tps'].rolling(window=5).mean()
        df['tps_rolling_std_5'] = df['tps'].rolling(window=5).std()
        df['tps_rolling_mean_12'] = df['tps'].rolling(window=12).mean()
        
        df['latency_lag_1'] = df['latency_ms'].shift(1)
        df['latency_lag_2'] = df['latency_ms'].shift(2)
        
        df = df.dropna()
        
        return df
    
    def extract_features(self, df: pd.DataFrame, is_training: bool = True) -> Tuple[pd.DataFrame, Optional[pd.Series]]:
        feature_cols = [
            'hour', 'minute', 'day_of_week', 
            'is_weekend', 'is_peak_hour', 'is_off_peak',
            'tps_lag_1', 'tps_lag_2', 'tps_lag_3',
            'tps_rolling_mean_5', 'tps_rolling_std_5', 'tps_rolling_mean_12',
            'latency_lag_1', 'latency_lag_2'
        ]
        
        X = df[feature_cols].copy()
        
        if is_training:
            y_tps = df['tps']
            y_latency = df['latency_ms']
            return X, y_tps, y_latency
        
        return X, None
    
    def train_models(self, df: pd.DataFrame) -> Dict:
        logger.info("开始训练流量预测模型...")
        
        X, y_tps, y_latency = self.extract_features(df)
        
        X_train, X_test, y_tps_train, y_tps_test, y_latency_train, y_latency_test = train_test_split(
            X, y_tps, y_latency, test_size=0.2, random_state=42, shuffle=False
        )
        
        self.scaler = StandardScaler()
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        logger.info("训练TPS预测模型...")
        self.tps_model = GradientBoostingRegressor(
            n_estimators=200,
            max_depth=8,
            learning_rate=0.1,
            random_state=42
        )
        self.tps_model.fit(X_train_scaled, y_tps_train)
        
        tps_pred = self.tps_model.predict(X_test_scaled)
        tps_mse = mean_squared_error(y_tps_test, tps_pred)
        tps_mae = mean_absolute_error(y_tps_test, tps_pred)
        tps_r2 = r2_score(y_tps_test, tps_pred)
        
        logger.info(f"TPS模型评估 - MSE: {tps_mse:.2f}, MAE: {tps_mae:.2f}, R2: {tps_r2:.4f}")
        
        logger.info("训练延迟预测模型...")
        self.latency_model = RandomForestRegressor(
            n_estimators=150,
            max_depth=10,
            random_state=42
        )
        self.latency_model.fit(X_train_scaled, y_latency_train)
        
        latency_pred = self.latency_model.predict(X_test_scaled)
        latency_mse = mean_squared_error(y_latency_test, latency_pred)
        latency_mae = mean_absolute_error(y_latency_test, latency_pred)
        latency_r2 = r2_score(y_latency_test, latency_pred)
        
        logger.info(f"延迟模型评估 - MSE: {latency_mse:.2f}, MAE: {latency_mae:.2f}, R2: {latency_r2:.4f}")
        
        metrics = {
            'tps_mse': tps_mse,
            'tps_mae': tps_mae,
            'tps_r2': tps_r2,
            'latency_mse': latency_mse,
            'latency_mae': latency_mae,
            'latency_r2': latency_r2
        }
        
        return metrics
    
    def predict_next_hour(self, current_features: Dict) -> Dict:
        if self.tps_model is None or self.latency_model is None:
            logger.warning("模型未训练，无法预测")
            return {'error': 'Model not trained'}
        
        features_df = pd.DataFrame([current_features])
        
        try:
            X, _ = self.extract_features(features_df, is_training=False)
            X_scaled = self.scaler.transform(X)
            
            predicted_tps = self.tps_model.predict(X_scaled)[0]
            predicted_latency = self.latency_model.predict(X_scaled)[0]
            
            return {
                'predicted_tps': float(predicted_tps),
                'predicted_latency_ms': float(predicted_latency),
                'prediction_time': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"预测失败: {e}")
            return {'error': str(e)}
    
    def record_metrics(self, tps: float, latency_ms: float):
        self.tps_history.append(tps)
        self.latency_history.append(latency_ms)
        self.timestamps.append(datetime.now())
    
    def get_current_stats(self) -> Dict:
        if len(self.tps_history) == 0:
            return {'error': 'No data available'}
        
        tps_array = np.array(self.tps_history)
        latency_array = np.array(self.latency_history)
        
        return {
            'current_tps': float(tps_array[-1]) if len(tps_array) > 0 else 0,
            'avg_tps_5min': float(np.mean(tps_array[-12:])) if len(tps_array) >= 12 else float(np.mean(tps_array)),
            'avg_tps_1hour': float(np.mean(tps_array)) if len(tps_array) > 0 else 0,
            'max_tps': float(np.max(tps_array)) if len(tps_array) > 0 else 0,
            'current_latency_ms': float(latency_array[-1]) if len(latency_array) > 0 else 0,
            'avg_latency_ms': float(np.mean(latency_array)) if len(latency_array) > 0 else 0,
            'p99_latency_ms': float(np.percentile(latency_array, 99)) if len(latency_array) >= 100 else 0,
            'data_points': len(self.tps_history)
        }
    
    def predict_scaling_need(self, current_tps: float, current_latency_ms: float, 
                               current_instances: int) -> Dict:
        stats = self.get_current_stats()
        if 'error' in stats:
            return {'error': 'Insufficient data'}
        
        predicted = self.predict_next_hour(self._create_feature_vector())
        
        if 'error' in predicted:
            predicted_tps = current_tps * 1.5
            predicted_latency = current_latency_ms * 1.5
        else:
            predicted_tps = predicted['predicted_tps']
            predicted_latency = predicted['predicted_latency_ms']
        
        scaling_decision = self._calculate_scaling(
            current_tps, current_latency_ms, 
            predicted_tps, predicted_latency,
            current_instances
        )
        
        return {
            'current_tps': current_tps,
            'current_latency_ms': current_latency_ms,
            'predicted_tps': predicted_tps,
            'predicted_latency_ms': predicted_latency,
            'current_instances': current_instances,
            'recommended_action': scaling_decision['action'],
            'recommended_instances': scaling_decision['instances'],
            'reason': scaling_decision['reason'],
            'confidence': scaling_decision['confidence']
        }
    
    def _create_feature_vector(self) -> Dict:
        now = datetime.now()
        return {
            'hour': now.hour,
            'minute': now.minute,
            'day_of_week': now.weekday(),
            'is_weekend': 1 if now.weekday() >= 5 else 0,
            'is_peak_hour': 1 if (9 <= now.hour < 18) or (18 <= now.hour < 22) else 0,
            'is_off_peak': 1 if (now.hour >= 22) or (now.hour < 6) else 0,
            'tps_lag_1': self.tps_history[-1] if len(self.tps_history) >= 1 else 0,
            'tps_lag_2': self.tps_history[-2] if len(self.tps_history) >= 2 else 0,
            'tps_lag_3': self.tps_history[-3] if len(self.tps_history) >= 3 else 0,
            'tps_rolling_mean_5': np.mean(list(self.tps_history)[-5:]) if len(self.tps_history) >= 5 else 0,
            'tps_rolling_std_5': np.std(list(self.tps_history)[-5:]) if len(self.tps_history) >= 5 else 0,
            'tps_rolling_mean_12': np.mean(list(self.tps_history)[-12:]) if len(self.tps_history) >= 12 else 0,
            'latency_lag_1': self.latency_history[-1] if len(self.latency_history) >= 1 else 0,
            'latency_lag_2': self.latency_history[-2] if len(self.latency_history) >= 2 else 0
        }
    
    def _calculate_scaling(self, current_tps: float, current_latency_ms: float,
                           predicted_tps: float, predicted_latency_ms: float,
                           current_instances: int) -> Dict:
        TPS_THRESHOLD_HIGH = 3000
        TPS_THRESHOLD_LOW = 500
        LATENCY_THRESHOLD_HIGH = 200
        LATENCY_THRESHOLD_LOW = 50
        MAX_INSTANCES = 10
        MIN_INSTANCES = 2
        
        tps_ratio = predicted_tps / max(current_tps, 1)
        latency_ratio = predicted_latency_ms / max(current_latency_ms, 1)
        
        action = 'NONE'
        instances = current_instances
        reason = 'No scaling needed'
        confidence = 0.5
        
        if (predicted_tps > TPS_THRESHOLD_HIGH * 1.2 or 
            predicted_latency_ms > LATENCY_THRESHOLD_HIGH * 1.5 or
            (tps_ratio > 1.5 and current_tps > TPS_THRESHOLD_HIGH * 0.8)):
            
            if current_instances < MAX_INSTANCES:
                action = 'SCALE_OUT'
                instances = min(current_instances + 2, MAX_INSTANCES)
                reason = f"Predicted high load: TPS={predicted_tps:.0f}, Latency={predicted_latency_ms:.0f}ms"
                confidence = 0.85
        
        elif (predicted_tps > TPS_THRESHOLD_HIGH or 
              predicted_latency_ms > LATENCY_THRESHOLD_HIGH or
              (tps_ratio > 1.2 and current_tps > TPS_THRESHOLD_HIGH * 0.6)):
            
            if current_instances < MAX_INSTANCES:
                action = 'SCALE_OUT'
                instances = min(current_instances + 1, MAX_INSTANCES)
                reason = f"Predicted moderate load: TPS={predicted_tps:.0f}, Latency={predicted_latency_ms:.0f}ms"
                confidence = 0.7
        
        elif (predicted_tps < TPS_THRESHOLD_LOW * 0.5 and 
              predicted_latency_ms < LATENCY_THRESHOLD_LOW and
              tps_ratio < 0.8 and
              current_instances > MIN_INSTANCES):
            
            action = 'SCALE_IN'
            instances = max(current_instances - 1, MIN_INSTANCES)
            reason = f"Predicted low load: TPS={predicted_tps:.0f}, Latency={predicted_latency_ms:.0f}ms"
            confidence = 0.75
        
        return {
            'action': action,
            'instances': instances,
            'reason': reason,
            'confidence': confidence
        }
    
    def save_models(self):
        joblib.dump(self.tps_model, os.path.join(MODEL_DIR, 'tps_predictor.pkl'))
        joblib.dump(self.latency_model, os.path.join(MODEL_DIR, 'latency_predictor.pkl'))
        joblib.dump(self.scaler, os.path.join(MODEL_DIR, 'traffic_scaler.pkl'))
        logger.info(f"模型已保存到: {MODEL_DIR}")
    
    def load_models(self):
        tps_path = os.path.join(MODEL_DIR, 'tps_predictor.pkl')
        latency_path = os.path.join(MODEL_DIR, 'latency_predictor.pkl')
        scaler_path = os.path.join(MODEL_DIR, 'traffic_scaler.pkl')
        
        if os.path.exists(tps_path) and os.path.exists(latency_path) and os.path.exists(scaler_path):
            self.tps_model = joblib.load(tps_path)
            self.latency_model = joblib.load(latency_path)
            self.scaler = joblib.load(scaler_path)
            logger.info("模型加载成功")
            return True
        return False


if __name__ == "__main__":
    predictor = TrafficPredictor()
    
    if not predictor.load_models():
        logger.info("未找到已保存的模型，开始训练新模型...")
        historical_data = predictor.generate_historical_data(hours=24 * 7)
        metrics = predictor.train_models(historical_data)
        predictor.save_models()
        print(f"训练完成，模型指标: {metrics}")
    
    print("\n模拟实时流量监控...")
    for i in range(10):
        tps = 1000 + np.random.normal(0, 200)
        latency = 50 + np.random.normal(0, 10)
        predictor.record_metrics(tps, latency)
        
        if i % 5 == 4:
            scaling_need = predictor.predict_scaling_need(tps, latency, 3)
            print(f"\n第 {i+1} 次监控:")
            print(f"  当前TPS: {tps:.0f}, 延迟: {latency:.0f}ms")
            print(f"  建议操作: {scaling_need.get('recommended_action', 'NONE')}")
            print(f"  建议实例数: {scaling_need.get('recommended_instances', 3)}")
            print(f"  原因: {scaling_need.get('reason', 'N/A')}")
    
    print("\n当前统计信息:")
    stats = predictor.get_current_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")
