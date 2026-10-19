FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-venv fuse3 libfuse2t64 \
    && rm -rf /var/lib/apt/lists/*
RUN python3 -m venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1
RUN pip install --no-cache-dir grpcio grpcio-tools fusepy pyjwt cryptography pytest
WORKDIR /app
COPY . .
RUN mkdir -p /mnt/dfsha