# Etapa build
FROM golang:1.23-bullseye AS builder
WORKDIR /app

# Copiar módulos y descargar dependencias
COPY go.mod go.sum ./
RUN go mod download

# Copiar todo el código y config.json
COPY . .

# Construir binario
RUN go build -o crawler

# Etapa final
FROM ubuntu:22.04
WORKDIR /app

# Dependencias necesarias
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Copiar binario
COPY --from=builder /app/crawler /usr/local/bin/crawler

# Copiar config.json desde builder
COPY --from=builder /app/config.json /app/config.json

CMD ["crawler"]
