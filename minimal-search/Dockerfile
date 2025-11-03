FROM node:20-alpine AS build

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .
RUN npm run build

FROM node:20-alpine AS run

WORKDIR /app

# Copiamos los archivos necesarios para ejecutar el build
COPY --from=build /app/dist ./dist
COPY --from=build /app/package*.json ./

# Instalar dependencias necesarias solo para ejecución
RUN npm install --omit=dev

# Puerto estándar de Astro
EXPOSE 5050

# Comando de ejecución
CMD ["npx", "astro", "preview", "--host", "0.0.0.0", "--port", "5050"]