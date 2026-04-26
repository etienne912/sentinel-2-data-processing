FROM python:3.12-slim AS base

# Install system dependencies required by rasterio/GDAL
RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    libgdal-dev \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency files first for better layer caching
COPY pyproject.toml uv.lock ./

# Install dependencies (no dev deps, frozen lockfile)
RUN uv sync --frozen --no-dev

# Copy source code
COPY src/ ./src/

ENTRYPOINT ["uv", "run", "python", "-m", "src.main"]
CMD ["--help"]
