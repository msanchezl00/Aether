 
# dar permisos de escritura en todo el HDFS (no recomendado para producción)
hdfs dfs -chmod -R 777 /

# reiniciar el conector para que inserte los nuevos archivos en HDFS, ya que de primeras al no tener permisos no los inserta
curl -X POST http://localhost:8083/connectors/hdfs-sink/tasks/0/restart
