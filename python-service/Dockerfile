FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY model-training/ ./model-training/
COPY risk-assessment/ ./risk-assessment/

RUN useradd -m riskcontrol && chown -R riskcontrol:riskcontrol /app

USER riskcontrol

WORKDIR /app/risk-assessment

EXPOSE 8083

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8083"]
