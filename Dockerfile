# Multi-stage build: compile stage + runtime stage
# Minimizes final image size and improves security

# ============================================================================
# Stage 1: Builder (compile dependencies)
# ============================================================================
FROM python:3.11-slim as builder

WORKDIR /tmp/build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    libssl-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python packages
COPY requirements.txt .
RUN pip install --user --no-cache-dir --compile -r requirements.txt

# ============================================================================
# Stage 2: Runtime (minimal image)
# ============================================================================
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Create non-root user for security
RUN groupadd -r syncuser && useradd -r -g syncuser syncuser

# Create necessary directories with proper permissions
RUN mkdir -p \
    /app/data \
    /app/logs \
    /app/backups \
    && chown -R syncuser:syncuser /app

# Copy Python dependencies from builder stage
COPY --from=builder --chown=syncuser:syncuser /root/.local /home/syncuser/.local

# Copy application code
COPY --chown=syncuser:syncuser . /app/

# Set environment variables
ENV PATH=/home/syncuser/.local/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Health check (optional)
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import asyncio; asyncio.run(__import__('asyncio').sleep(0))" || exit 1

# Switch to non-root user
USER syncuser

# Run the application
CMD ["python", "-m", "main"]
