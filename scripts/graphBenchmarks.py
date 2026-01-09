import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import glob
import os

# Configuración visual
sns.set_theme(style="whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)

def leer_datos(patron_archivo):
    """
    Busca todos los archivos que coincidan con el patrón (ej: M2_hdfs_*.csv)
    y los une en un solo DataFrame, añadiendo una columna con la configuración.
    """
    archivos = glob.glob(patron_archivo)
    dfs = []
    
    if not archivos:
        print(f"⚠️ No se encontraron archivos con el patrón: {patron_archivo}")
        return None

    for archivo in archivos:
        try:
            df = pd.read_csv(archivo)
            # Extraemos una etiqueta limpia del nombre del archivo para la leyenda
            # Asumimos formato: M2_sistema_FECHA_configuracion.csv
            nombre_limpio = os.path.basename(archivo).replace(".csv", "")
            parts = nombre_limpio.split('_')
            # Intentamos tomar solo la parte de la configuración (después de la fecha)
            # Si el formato varía, tomamos todo el nombre
            config_label = "_".join(parts[3:]) if len(parts) > 3 else nombre_limpio
            
            df['Configuracion'] = config_label
            dfs.append(df)
            print(f"-> Cargado: {archivo}")
        except Exception as e:
            print(f"Error leyendo {archivo}: {e}")
    
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return None

def graficar_hdfs():
    print("\n--- Generando Gráfica HDFS ---")
    # Busca archivos HDFS de cualquier fecha y configuración
    df = leer_datos("M2_hdfs_*.csv")
    
    if df is not None:
        plt.figure(figsize=(10, 6))
        
        # Graficamos Throughput vs File Size
        # 'hue' permite comparar las diferentes configuraciones (1 nodo vs 2 nodos)
        # 'style' permite diferenciar Write vs Read si ambos existen en Test_Type
        sns.barplot(
            data=df, 
            x="File_Size_MB", 
            y="Throughput_MB_s", 
            hue="Configuracion",
            palette="viridis"
        )
        
        plt.title("HDFS Benchmark: Rendimiento de Entrada/Salida")
        plt.ylabel("Throughput (MB/s)")
        plt.xlabel("Tamaño del Archivo (MB)")
        plt.legend(title="Topología", loc='upper left')
        plt.tight_layout()
        plt.savefig("Grafica_HDFS_Throughput.png")
        print("Guardado: Grafica_HDFS_Throughput.png")

def graficar_spark():
    print("\n--- Generando Gráfica Spark ---")
    df = leer_datos("M2_spark_*.csv")
    
    if df is not None:
        plt.figure(figsize=(10, 6))
        
        # Graficamos Tiempo de Ejecución vs Nivel de Carga
        # Menor tiempo es mejor
        sns.lineplot(
            data=df, 
            x="Iterations", 
            y="Real_Execution_Time_sec", 
            hue="Configuracion", 
            marker="o",
            linewidth=2.5
        )
        
        plt.title("Spark Benchmark: Tiempo de Ejecución")
        plt.ylabel("Tiempo (segundos) [Menor es mejor]")
        plt.xlabel("Nivel de Carga / Iteraciones")
        plt.legend(title="Topología")
        plt.grid(True, which='minor', linestyle='--')
        plt.tight_layout()
        plt.savefig("Grafica_Spark_ExecutionTime.png")
        print("Guardado: Grafica_Spark_ExecutionTime.png")

def graficar_kafka():
    print("\n--- Generando Gráfica Kafka ---")
    df = leer_datos("M2_kafka_*.csv")
    
    if df is not None:
        # Para Kafka hacemos 2 subgráficas: Throughput y Latencia
        fig, axes = plt.subplots(2, 1, figsize=(12, 10), sharex=True)
        
        # Gráfica 1: Throughput (Registros por segundo)
        sns.barplot(
            ax=axes[0],
            data=df,
            x="Load_Level",
            y="Records_Per_Sec",
            hue="Configuracion",
            palette="magma"
        )
        axes[0].set_title("Kafka: Throughput (Registros/seg)")
        axes[0].set_ylabel("Records/sec [Mayor es mejor]")
        axes[0].legend(loc='upper left')

        # Gráfica 2: Latencia Promedio
        sns.lineplot(
            ax=axes[1],
            data=df,
            x="Load_Level",
            y="Avg_Latency_ms",
            hue="Configuracion",
            style="Configuracion",
            markers=True,
            dashes=False,
            linewidth=2.5,
            palette="magma"
        )
        axes[1].set_title("Kafka: Latencia Promedio")
        axes[1].set_ylabel("Latencia (ms) [Menor es mejor]")
        axes[1].set_xlabel("Nivel de Carga")
        axes[1].get_legend().remove() # La leyenda de arriba ya sirve

        plt.tight_layout()
        plt.savefig("Grafica_Kafka_Performance.png")
        print("Guardado: Grafica_Kafka_Performance.png")

if __name__ == "__main__":
    graficar_hdfs()
    graficar_spark()
    graficar_kafka()
    print("\nProceso finalizado. Revisa los archivos .png generados.")