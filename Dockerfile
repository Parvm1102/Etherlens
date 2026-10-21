# Stage 1: Foundry binaries (from official image, avoids GitHub download in build network)
FROM ghcr.io/foundry-rs/foundry:latest AS foundry

# Stage 2: Project image
FROM python:3.11-slim-bookworm

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    make \
    curl \
    openssh-client \
    git \
    tmux \
    procps \
    gnupg \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Google Cloud CLI
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
    > /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
       | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && apt-get update && apt-get install -y --no-install-recommends google-cloud-cli \
    && rm -rf /var/lib/apt/lists/*

# Copy Foundry binaries from the official image
RUN mkdir -p /root/.foundry/bin
COPY --from=foundry /usr/local/bin/forge /root/.foundry/bin/forge
COPY --from=foundry /usr/local/bin/anvil /root/.foundry/bin/anvil
COPY --from=foundry /usr/local/bin/cast  /root/.foundry/bin/cast
COPY --from=foundry /usr/local/bin/chisel /root/.foundry/bin/chisel

# Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Python/Spark environment
ENV PYTHONPATH=/work/src
ENV SPARK_LOCAL_IP=127.0.0.1
ENV PYTHONUNBUFFERED=1
ENV PATH="/root/.foundry/bin:${PATH}"

WORKDIR /work

# Install Python dependencies (separate layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Solidity compiler for forge, pre-installed so contract builds and tests work
# offline (forge would otherwise download it on every fresh container)
RUN mkdir -p /root/.svm/0.8.24 \
    && curl -fsSL "https://binaries.soliditylang.org/linux-amd64/solc-linux-amd64-v0.8.24+commit.e11b9ed9" \
       -o /root/.svm/0.8.24/solc-0.8.24 \
    && chmod +x /root/.svm/0.8.24/solc-0.8.24 \
    && git config --global --add safe.directory /work
