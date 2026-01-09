#!/bin/bash

# ==============================================================================
# 0. AJUSTE DE LOCALE (IMPORTANTE PARA EVITAR ERROR PRINTF)
# ==============================================================================
# Esto fuerza a que los decimales sean puntos (.) y no comas, para que bc y printf se entiendan
export LC_NUMERIC="C"

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
HADOOP_CTR="aether-hdfs-namenode-1"
SPARK_CTR="aether-spark-master-1"
KAFKA_CTR="aether-kafka-broker-1"
TIMESTAMP=$(date +%Y-%m-%d)
CONFIGURATIONHDFS="1_namenode_2_datanodes"
CONFIGURATIONSPARK="1_sparkmaster_2_workers"
CONFIGURATIONKAFKA="1_zookeeper_2_broker"

# Salidas
HDFS_CSV="M2_hdfs_${TIMESTAMP}_${CONFIGURATIONHDFS}.csv"
SPARK_CSV="M2_spark_${TIMESTAMP}_${CONFIGURATIONSPARK}.csv"
KAFKA_CSV="M2_kafka_${TIMESTAMP}_${CONFIGURATIONKAFKA}.csv"

# Cabeceras
echo "Timestamp,Test_Type,File_Size_MB,Duration_Sec,Throughput_MB_s" > $HDFS_CSV
echo "Timestamp,Load_Level,Iterations,Real_Execution_Time_sec" > $SPARK_CSV
echo "Timestamp,Load_Level,Records,Records_Per_Sec,MB_Per_Sec,Avg_Latency_ms,Max_Latency_ms,50th_Percentile_ms" > $KAFKA_CSV

echo "🚀 INICIANDO BENCHMARK (LOCALE FIXED)"
echo "--------------------------------------------------------"

# ==============================================================================
# 1. HDFS (MÉTODO DIRECT I/O)
# ==============================================================================
echo "🐘 [HADOOP] Ejecutando pruebas de Escritura/Lectura Directa..."

# Salir de Safe Mode (silencioso)
docker exec $HADOOP_CTR hdfs dfsadmin -safemode leave > /dev/null 2>&1

declare -a sizes=("10" "25" "50" "75" "100" "150" "200" "250" "300" "400" "500" "600" "750" "1000" "1250" "1500" "2000" "2500" "3000" "3500" "4000" "4500" "5000") 

for SIZE in "${sizes[@]}"; do
    echo "   👉 Probando con archivo de ${SIZE}MB..."

    # 1. Generar archivo dummy
    docker exec $HADOOP_CTR sh -c "dd if=/dev/zero of=/tmp/test_data_${SIZE}M bs=1M count=$SIZE 2>/dev/null"

    # 2. TEST ESCRITURA (PUT)
    START=$(date +%s.%N)
    # 2>/dev/null silencia los logs de SASL/Java
    docker exec $HADOOP_CTR hdfs dfs -put -f /tmp/test_data_${SIZE}M /bench_test_${SIZE}M > /dev/null 2>&1
    END=$(date +%s.%N)
    
    DURATION=$(echo "$END - $START" | bc)
    # Protección contra división por cero
    if (( $(echo "$DURATION < 0.01" | bc -l) )); then DURATION=0.01; fi
    SPEED=$(echo "$SIZE / $DURATION" | bc -l)
    
    # Formatear
    DURATION_F=$(printf "%.2f" "$DURATION")
    SPEED_F=$(printf "%.2f" "$SPEED")

    echo "$(date +%T),WRITE,$SIZE,$DURATION_F,$SPEED_F" >> $HDFS_CSV
    echo "      📝 Write: $SPEED_F MB/s ($DURATION_F sec)"

    # 3. TEST LECTURA (GET)
    START=$(date +%s.%N)
    docker exec $HADOOP_CTR hdfs dfs -cat /bench_test_${SIZE}M > /dev/null 2>&1
    END=$(date +%s.%N)

    DURATION=$(echo "$END - $START" | bc)
    if (( $(echo "$DURATION < 0.01" | bc -l) )); then DURATION=0.01; fi
    SPEED=$(echo "$SIZE / $DURATION" | bc -l)
    
    DURATION_F=$(printf "%.2f" "$DURATION")
    SPEED_F=$(printf "%.2f" "$SPEED")

    echo "$(date +%T),READ,$SIZE,$DURATION_F,$SPEED_F" >> $HDFS_CSV
    echo "      📖 Read:  $SPEED_F MB/s ($DURATION_F sec)"

    # Limpieza
    docker exec $HADOOP_CTR rm /tmp/test_data_${SIZE}M
    docker exec $HADOOP_CTR hdfs dfs -rm -skipTrash /bench_test_${SIZE}M > /dev/null 2>&1
done
echo "--------------------------------------------------------"

# ==============================================================================
# 2. SPARK (SparkPi)
# ==============================================================================
echo "✨ [SPARK] Buscando JAR..."
JAR_SPARK=$(docker exec $SPARK_CTR find /opt/bitnami/spark/examples/jars -name "spark-examples_*.jar" | head -n 1)

if [ -z "$JAR_SPARK" ]; then
    echo "❌ ERROR: No JAR found for Spark."
else
    declare -a spark_loads=("LOW 25" "LOW 50" "LOW 100" "LOW 150" "LOW 200" "LOW 250" "LOW 500" "LOW 750" "MEDIUM 1000" "MEDIUM 1250" "HIGH 1500" "HIGH 2000")

    for load in "${spark_loads[@]}"; do
        read -r LEVEL ITERS <<< "$load"
        echo "   👉 SparkPi $LEVEL ($ITERS iters)..."

        START_TIME=$(date +%s)
        docker exec $SPARK_CTR spark-submit \
            --master spark://spark-master:7077 \
            --class org.apache.spark.examples.SparkPi \
            $JAR_SPARK $ITERS > /dev/null 2>&1
        END_TIME=$(date +%s)
        
        DURATION=$((END_TIME - START_TIME))
        echo "$(date +%T),$LEVEL,$ITERS,$DURATION" >> $SPARK_CSV
        echo "      ✅ Duración: $DURATION sec"
    done
fi
echo "--------------------------------------------------------"

# ==============================================================================
# 3. KAFKA (Producer Perf Test)
# ==============================================================================
echo "🔥 [KAFKA] Test de Rendimiento..."

# Definición de Arrays (Tu configuración completa)
declare -a kafka_levels=("LOW" "LOW" "HIGH" "VERY_HIGH" "EXTREME" "INSANE" "EXTREME" "INSANE" "EXTREME" "INSANE" "EXTREME")
declare -a kafka_records=("100000" "500000" "2000000" "5000000" "10000000" "20000000" "50000000" "100000000" "200000000" "500000000" "1000000000")

for i in "${!kafka_levels[@]}"; do
    LEVEL=${kafka_levels[$i]}
    RECORDS=${kafka_records[$i]}
    
    echo "   👉 [$i] Kafka $LEVEL - Records: $RECORDS"

    # Capturamos la salida
    OUTPUT=$(docker exec $KAFKA_CTR kafka-producer-perf-test \
      --topic "bench-$LEVEL-$i" \
      --num-records $RECORDS \
      --record-size 100 \
      --throughput -1 \
      --producer-props bootstrap.servers=localhost:9092 2>&1)

    echo "$OUTPUT" \
      | grep "records sent" \
      | tail -n 1 \
      | sed 's/(/,/g' | sed 's/)//g' \
      | awk -F',' -v date="$(date +%T)" -v lvl="$LEVEL" -v recs="$RECORDS" \
        '{print date","lvl","recs","$2","$3","$4","$5","$6}' \
      | sed -E 's/ records\/sec//g' | sed -E 's/ MB\/sec//g' | sed -E 's/ ms avg latency//g' | sed -E 's/ ms max latency//g' | sed -E 's/ ms 50th//g' \
      | sed 's/ //g' \
      >> $KAFKA_CSV
      
    echo "      ✅ Completado."
done

echo "========================================================"
echo "🏁 Resultados corregidos en: $KAFKA_CSV"

echo "✅ TODO LISTO."