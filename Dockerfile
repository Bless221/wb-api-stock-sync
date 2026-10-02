# ============================================================================
# Stage 1: Builder (compile dependencies)
# ============================================================================
FROM python:3.11-slim as builder

WORKDIR /tmp/build

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    libssl-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/usr/local -r requirements.txt

# ============================================================================
# Stage 2: Runtime (minimal secure image)
# ============================================================================
FROM python:3.11-slim

WORKDIR /app

RUN groupadd -r syncuser && useradd -r -g syncuser -m -d /home/syncuser syncuser

COPY --from=builder /usr/local /usr/local

COPY --chown=syncuser:syncuser . /app/

RUN mkdir -p /app/data /app/logs /app/backups \
    && chown -R syncuser:syncuser /app/data /app/logs /app/backups

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

HEALTHCHECK --interval=60s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import aiohttp; import pydantic; import pandas" || exit 1

USER syncuser

CMD ["python", "main.py"]
