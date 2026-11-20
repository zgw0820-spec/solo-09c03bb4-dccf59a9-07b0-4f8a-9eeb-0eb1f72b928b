FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app ./app
COPY mock_k8s ./mock_k8s

RUN mkdir -p /data

EXPOSE 8080 8001

# 默认启动巡检服务；模拟服务在 compose 里用独立 command 启动
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
