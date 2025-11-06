# --- Fase 1: Construcción (Build) ---
FROM node:20-alpine AS build
WORKDIR /app

COPY package*.json ./
# 1. Instala TODAS las dependencias (dev y prod)
RUN npm install

COPY . .
# 2. Construye el servidor
RUN npm run build

# --- Fase 2: Ejecución (Run) ---
FROM node:20-alpine AS run
WORKDIR /app

# 3. Copia el servidor construido desde la fase anterior
COPY --from=build /app/dist ./dist

# 4. 🔥 LA SOLUCIÓN: Copia la carpeta node_modules completa
#    Esto garantiza que paquetes como 'kleur' estén presentes
COPY --from=build /app/node_modules ./node_modules

# 5. (Opcional pero recomendado) Copia el package.json
COPY --from=build /app/package.json ./package.json

ENV PORT=5050
ENV HOST=0.0.0.0
EXPOSE 5050

CMD ["node", "./dist/server/entry.mjs"]