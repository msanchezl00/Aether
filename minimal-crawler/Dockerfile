# --- Etapa de Construcción (Builder) ---
FROM golang:1.24-bullseye AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
# Compilamos la aplicación
RUN go build -o crawler

# --- Etapa Final (Runtime) ---
# Usamos Debian Bookworm (versión 12) en lugar de Ubuntu.
# Debian maneja Chromium nativamente sin snaps.
FROM debian:bookworm-slim

WORKDIR /app

# Instalamos Chromium y certificados.
# En Debian Bookworm, el paquete 'chromium' es un binario real, compatible con ARM64.
RUN apt-get update && apt-get install -y \
    chromium \
    ca-certificates \
    dumb-init \
    && rm -rf /var/lib/apt/lists/*

# Copiamos el binario y la config
COPY --from=builder /app/crawler /usr/local/bin/crawler
COPY --from=builder /app/config.json /app/config.json

# Variable para asegurar que Rod encuentre el binario (por si acaso)
ENV ROD_BROWSER_BIN=/usr/bin/chromium

# Usamos dumb-init para manejar correctamente los procesos zombies que a veces genera Chrome
ENTRYPOINT ["/usr/bin/dumb-init", "--"]

CMD ["crawler"]