FROM python:3.12-alpine

ENV PYTHONUNBUFFERED=1 \
    TZ=UTC \
    DISCOVERY_INTERVAL_SECONDS=60 \
    JOB_TIMEOUT_SECONDS=3600 \
    MAX_CONCURRENT_JOBS=10 \
    MAX_JITTER_SECONDS=3600 \
    OUTPUT_LIMIT_BYTES=4096 \
    LOG_JOB_OUTPUT=true \
    DOCKER_SOCKET=/var/run/docker.sock

WORKDIR /app
RUN apk add --no-cache tzdata
COPY docker_cron.py /app/docker_cron.py

ENTRYPOINT ["python3", "/app/docker_cron.py"]
