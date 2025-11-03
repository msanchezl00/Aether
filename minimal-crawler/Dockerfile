# Etapa build
FROM golang:1.23-bullseye AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o crawler

# Etapa final
FROM ubuntu:22.04
WORKDIR /app

# Librerías mínimas para Chromium headless
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libx11-6 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxi6 \
    libxrandr2 \
    libxrender1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnss3 \
    libatk1.0-0 \
    libasound2 \
    fonts-liberation \
    libgbm1 \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Copiar binario del builder y config
COPY --from=builder /app/crawler /usr/local/bin/crawler
COPY --from=builder /app/config.json /app/config.json

CMD ["crawler"]
