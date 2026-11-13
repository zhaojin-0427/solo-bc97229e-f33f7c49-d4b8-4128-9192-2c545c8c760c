FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /srv/lake

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY scripts ./scripts
COPY samples ./samples

ENV DATA_DIR=/data \
    LEASE_TTL_SEC=30 \
    RECLAIM_INTERVAL_SEC=2
VOLUME ["/data"]
EXPOSE 8000

# 多 worker 进程共享 DATA_DIR：验证撤回 / 租约 / 回收在多进程下成立
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "3", "--threads", "4", "--timeout", "300", "app.web:app"]
