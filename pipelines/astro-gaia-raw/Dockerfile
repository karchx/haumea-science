
# 1. Use an official Python base image
FROM python:3.12-slim

# 2. Copy the uv binary from the official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# 3. Set work directory
WORKDIR /app

# 4. Copy project files
COPY pyproject.toml uv.lock ./

# 5. Sync dependencies (boto3 must be in pyproject.toml)
# Using --mount=type=cache speeds up builds by caching uv's cache
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project

# 6. Copy application code
COPY . .

# 7. Run the application using 'uv run'
CMD ["uv", "run", "python", "main.py"]
