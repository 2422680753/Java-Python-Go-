import pandas as pd
import numpy as np
import joblib
import os
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
from xgboost import XGBClassifier
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MODEL_DIR = os.path.join(os.path.dirname(__file__), 'models')
os.makedirs(MODEL_DIR, exist_ok=True)

def generate_sample_data(n_samples=100000, fraud_ratio=0.05):
    np.random.seed(42)
    
    data = {
        'transaction_id': [f'tx_{i}' for i in range(n_samples)],
        'user_id': [f'user_{np.random.randint(0, 10000)}' for _ in range(n_samples)],
        'amount': np.random.exponential(scale=500, size=n_samples),
        'transaction_type': np.random.choice(['transfer', 'payment', 'withdrawal', 'deposit'], 
                                               size=n_samples, p=[0.3, 0.4, 0.2, 0.1]),
        'hour_of_day': np.random.randint(0, 24, size=n_samples),
        'day_of_week': np.random.randint(0, 7, size=n_samples),
        'is_new_user': np.random.choice([0, 1], size=n_samples, p=[0.8, 0.2]),
        'is_new_device': np.random.choice([0, 1], size=n_samples, p=[0.7, 0.3]),
        'is_new_location': np.random.choice([0, 1], size=n_samples, p=[0.75, 0.25]),
        'user_transaction_count': np.random.poisson(lam=50, size=n_samples),
        'user_daily_amount': np.random.exponential(scale=2000, size=n_samples),
        'login_failures_24h': np.random.poisson(lam=0.5, size=n_samples),
        'is_high_risk_country': np.random.choice([0, 1], size=n_samples, p=[0.95, 0.05]),
        'suspicious_recipient': np.random.choice([0, 1], size=n_samples, p=[0.9, 0.1]),
    }
    
    df = pd.DataFrame(data)
    
    n_fraud = int(n_samples * fraud_ratio)
    fraud_indices = np.random.choice(df.index, size=n_fraud, replace=False)
    
    df.loc[fraud_indices, 'amount'] = np.random.exponential(scale=5000, size=n_fraud)
    df.loc[fraud_indices, 'is_new_device'] = np.random.choice([0, 1], size=n_fraud, p=[0.3, 0.7])
    df.loc[fraud_indices, 'is_new_location'] = np.random.choice([0, 1], size=n_fraud, p=[0.4, 0.6])
    df.loc[fraud_indices, 'login_failures_24h'] = np.random.poisson(lam=3, size=n_fraud)
    df.loc[fraud_indices, 'is_high_risk_country'] = np.random.choice([0, 1], size=n_fraud, p=[0.6, 0.4])
    
    df['is_fraud'] = 0
    df.loc[fraud_indices, 'is_fraud'] = 1
    
    return df

def create_preprocessor():
    numeric_features = ['amount', 'hour_of_day', 'day_of_week', 'user_transaction_count',
                        'user_daily_amount', 'login_failures_24h']
    categorical_features = ['transaction_type']
    binary_features = ['is_new_user', 'is_new_device', 'is_new_location', 
                       'is_high_risk_country', 'suspicious_recipient']
    
    numeric_transformer = Pipeline(steps=[
        ('scaler', StandardScaler())
    ])
    
    categorical_transformer = Pipeline(steps=[
        ('onehot', OneHotEncoder(drop='first', sparse_output=False))
    ])
    
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numeric_transformer, numeric_features),
            ('cat', categorical_transformer, categorical_features),
            ('bin', 'passthrough', binary_features)
        ])
    
    return preprocessor, numeric_features + binary_features

def train_model():
    logger.info("开始生成训练数据...")
    df = generate_sample_data()
    
    logger.info(f"数据形状: {df.shape}")
    logger.info(f"欺诈样本数: {df['is_fraud'].sum()}")
    logger.info(f"欺诈比例: {df['is_fraud'].mean():.2%}")
    
    feature_cols = ['amount', 'transaction_type', 'hour_of_day', 'day_of_week',
                    'is_new_user', 'is_new_device', 'is_new_location',
                    'user_transaction_count', 'user_daily_amount',
                    'login_failures_24h', 'is_high_risk_country', 'suspicious_recipient']
    
    X = df[feature_cols]
    y = df['is_fraud']
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    
    logger.info(f"训练集大小: {X_train.shape}")
    logger.info(f"测试集大小: {X_test.shape}")
    
    preprocessor, feature_names = create_preprocessor()
    
    X_train_processed = preprocessor.fit_transform(X_train)
    X_test_processed = preprocessor.transform(X_test)
    
    logger.info("开始训练XGBoost模型...")
    
    scale_pos_weight = len(y_train[y_train == 0]) / len(y_train[y_train == 1])
    
    model = XGBClassifier(
        n_estimators=100,
        max_depth=6,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=scale_pos_weight,
        random_state=42,
        use_label_encoder=False,
        eval_metric='logloss'
    )
    
    model.fit(X_train_processed, y_train)
    
    y_pred = model.predict(X_test_processed)
    y_pred_proba = model.predict_proba(X_test_processed)[:, 1]
    
    logger.info("\n模型评估结果:")
    logger.info(f"ROC AUC Score: {roc_auc_score(y_test, y_pred_proba):.4f}")
    logger.info("\n分类报告:")
    logger.info(classification_report(y_test, y_pred))
    logger.info("\n混淆矩阵:")
    logger.info(confusion_matrix(y_test, y_pred))
    
    model_path = os.path.join(MODEL_DIR, 'fraud_detection_model.pkl')
    preprocessor_path = os.path.join(MODEL_DIR, 'preprocessor.pkl')
    
    joblib.dump(model, model_path)
    joblib.dump(preprocessor, preprocessor_path)
    
    logger.info(f"\n模型已保存到: {model_path}")
    logger.info(f"预处理器已保存到: {preprocessor_path}")
    
    return model, preprocessor

if __name__ == '__main__':
    train_model()
