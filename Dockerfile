FROM python:3.12-slim

WORKDIR /app

# Install system dependencies (e.g., build-essential, git if needed, curl for health checks)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy pyproject.toml and README.md (setuptools needs README.md as specified in pyproject.toml)
COPY pyproject.toml README.md ./

# Copy empty source to allow caching dependencies install
RUN mkdir -p src/constructsync && touch src/constructsync/__init__.py
RUN pip install --no-cache-dir .

# Copy the actual code
COPY src/ ./src
COPY tests/ ./tests

# Reinstall the package to include actual source files
RUN pip install --no-cache-dir .

EXPOSE 8000

CMD ["uvicorn", "constructsync.main:app", "--host", "0.0.0.0", "--port", "8000"]
