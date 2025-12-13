ssh i0913738@nogal.usal.es -L 5050:212.128.144.169:5050


docker compose up -d        # Lanza (o relanza) los contenedores en segundo plano
docker compose down         # Detiene y elimina contenedores, redes y volúmenes anónimos
docker compose restart      # Reinicia todos los contenedores del proyecto
docker compose stop         # Detiene los contenedores sin eliminarlos
docker compose start        # Inicia los contenedores detenidos
docker compose ps           # Muestra el estado de los contenedores


docker compose up -d <servicio>     # Levanta solo un servicio
docker compose restart <servicio>  # Reinicia solo un servicio
docker compose stop <servicio>     # Detiene solo un servicio
docker compose start <servicio>    # Inicia solo un servicio
docker compose logs -f <servicio>  # Sigue los logs en tiempo real

docker compose down -v              # Borra contenedores + volúmenes
docker compose down --rmi all       # Borra también las imágenes
docker compose build --no-cache     # Fuerza reconstrucción limpia
docker compose up -d --build        # Reconstruye y levanta en una sola línea

docker compose down -v --rmi all
sudo docker compose up -d --build --force-recreate <service>