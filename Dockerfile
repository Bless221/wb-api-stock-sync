# ============================================================================
# Этап 1: Сборка зависимостей (Builder)
# ============================================================================
FROM python:3.12-slim as builder

WORKDIR /tmp/build

# Устанавливаем системные зависимости для сборки C-расширений (если понадобятся для pandas/sqlite)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    libssl-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
# Устанавливаем колеса зависимостей в изолированную директорию
RUN pip install --no-cache-dir --user -r requirements.txt

# ============================================================================
# Этап 2: Финальный продакшен-образ (Runtime)
# ============================================================================
FROM python:3.12-slim

WORKDIR /app

# Создаем безопасного системного пользователя без прав root
RUN groupadd -r syncuser && useradd -r -g syncuser -m -d /home/syncuser syncuser

# Копируем установленные библиотеки из builder-этапа
COPY --from=builder /root/.local /home/syncuser/.local
COPY --chown=syncuser:syncuser . /app/

# Создаем структуры папок под persistence-данные и выдаем права
RUN mkdir -p /app/data /app/logs /app/backups \
    && chown -R syncuser:syncuser /app/data /app/logs /app/backups

# Прокидываем пути к бинарникам пользователя в ENV
ENV PATH=/home/syncuser/.local/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Проверка работоспособности контейнера (Healthcheck)
HEALTHCHECK --interval=60s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import aiohttp; import pydantic; import pandas" || exit 1

USER syncuser

CMD ["python", "main.py"]
