import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_contains, concat_ws, lit, explode, lower, count, exists, row_number, desc
from pyspark.sql.window import Window
from py4j.java_gateway import java_import

# Cargar configuración
with open("config/spark_config.json") as f:
    cfg = json.load(f)

# Inicializar SparkSession
spark = (
    SparkSession.builder
    .appName(cfg["app_name"])
    .master(cfg["master"])
    .config("spark.executor.memory", cfg["executor_memory"])
    .config("spark.driver.memory", cfg["driver_memory"])
    .config("spark.executor.cores", cfg["executor_cores"])
    .config("spark.hadoop.fs.defaultFS", cfg["hdfs_base_path"])
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.1")
    .config("spark.sql.catalogImplementation", "in-memory")
    .config("spark.network.timeout", "600s")
    .config("spark.executor.heartbeatInterval", "100s")
    .getOrCreate()
)

# Acceso al FileSystem de HDFS
sc = spark.sparkContext
java_import(sc._gateway.jvm, "org.apache.hadoop.fs.FileSystem")
java_import(sc._gateway.jvm, "org.apache.hadoop.fs.Path")
fs = sc._gateway.jvm.FileSystem.get(sc._jsc.hadoopConfiguration())

# Leer todos los Avro recursivamente
df = (
    spark.read.format("avro")
    .option("recursiveFileLookup", "true")
    .load(cfg["hdfs_base_path"])
)

def search_uris(query: str, max_results: int = 1000):
    # Separar palabras clave
    keywords = [w.lower() for w in query.split() if w.strip()]

    # Construir filtro dinámico con OR entre todas las palabras
    cond = None
    for kw in keywords:
        kw_cond = (
            array_contains(col("tags"), kw)
            | exists(col("content.texts.h1"), lambda x: lower(x).contains(kw))
            | exists(col("content.texts.h2"), lambda x: lower(x).contains(kw))
            | exists(col("content.texts.p"), lambda x: lower(x).contains(kw))
        )
        cond = kw_cond if cond is None else (cond | kw_cond)

    # Filtrar por coincidencias en cualquiera de las palabras
    df_filtered = (
        df.filter(cond)
        .withColumn("url", concat_ws("", lit("https://"), col("domain"), col("real_path")))
        .select("url")
        .distinct()
    )

    # Traer external_refs ya precalculado
    df_score = df.select("url", "external_refs").distinct()

    # Unir y ordenar
    df_result = (
        df_filtered.join(df_score, "url", "left")
        .fillna(0, subset=["external_refs"])
        .orderBy(col("external_refs").desc())
    )

    # Iterar resultados
    urls_with_score = []
    for row in df_result.toLocalIterator():
        urls_with_score.append({"url": row["url"], "score": row["external_refs"]})
        if len(urls_with_score) >= max_results:
            break

    return urls_with_score


def get_crawled_data(url: str):
    """
    Obtiene toda la información crawleada para una URL específica (dominio + path).
    Retorna una lista de diccionarios con los datos encontrados.
    """
    # Normalizar entrada si falta protocolo (asumimos https por defecto según el esquema logico)
    target_url = url.strip()
    if not target_url.startswith("http"):
        target_url = "https://" + target_url
    
    # Manejar posible "/" al final (probar con y sin slash)
    candidates = [target_url]
    if target_url.endswith("/"):
        candidates.append(target_url.rstrip("/"))
    else:
        candidates.append(target_url + "/")

    # Filtrar por la url construida
    res_df = (
        df.withColumn("url", concat_ws("", lit("https://"), col("domain"), col("real_path")))
        .filter(col("url").isin(candidates))
        .orderBy(col("date").desc())
    )
    
    # Recolectar y convertir a diccionario
    data = []
    for row in res_df.collect():
        row_dict = row.asDict(recursive=True)
        # Limpiar keys internas de spark/avro si molestan, o devolver todo
        data.append(row_dict)
        
    return data


def load_hdfs_parquet_data():
    """
    Carga datos desde HDFS, calcula external_refs y sobrescribe el Avro original
    manteniendo la estructura de directorios domain/date.
    """
    global df

    spark.catalog.clearCache()

    # Cargar datos originales
    raw_df = (
        spark.read.format("avro")
        .option("recursiveFileLookup", "true")
        .load(cfg["hdfs_base_path"])
    )

    # --- INICIO DEDUPLICACIÓN (Mantenemos histórico diario) ---
    # Creamos la URL para poder identificar duplicados
    raw_df = raw_df.withColumn("url", concat_ws("", lit("https://"), col("domain"), col("real_path")))
    
    # 1. Limpieza Intra-día: Nos quedamos solo con 1 copia por (URL, Fecha)
    # Si se crawleó 5 veces hoy, queda 1. Si se crawleó ayer y hoy, quedan 2.
    windowDaily = Window.partitionBy("url", "date").orderBy(lit(1)) # Orden arbitrario si no hay hora
    
    df_history = (
        raw_df.withColumn("rn", row_number().over(windowDaily))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    # --- FIN DEDUPLICACIÓN ---

    # --- CÁLCULO DE RANKING (Solo basado en la foto MÁS RECIENTE) ---
    # Para no inflar los votos, calculamos los links usando solo la última versión de cada página.
    windowLatest = Window.partitionBy("url").orderBy(col("date").desc())
    
    df_latest_snapshot = (
        df_history.withColumn("rn_latest", row_number().over(windowLatest))
        .filter(col("rn_latest") == 1) # Solo la última versión de cada URL
        .select("url", "content") # Solo necesitamos URL y contenido para sacar links
    )

    # Crear DataFrame de links usando solo el snapshot actual
    df_links = (
        df_latest_snapshot.select(explode(col("content.links.https")).alias("link"))
        .union(df_latest_snapshot.select(explode(col("content.links.http")).alias("link")))
    )

    # Contar votos (external_refs) basándonos en el presente
    df_ref_count = (
        df_links.groupBy("link")
        .agg(count("*").alias("external_refs"))
    )

    # --- UNIÓN Y ESCRITURA ---
    # Unimos los scores calculados a TODO el histórico.
    # Así, las versiones antiguas también tendrán el score "actual" (o 0), lo cual es útil para búsquedas.
    
    # Limpiamos columna previa si existe
    if "external_refs" in df_history.columns:
        df_history = df_history.drop("external_refs")

    df_final = (
        df_history.join(df_ref_count, df_history.url == df_ref_count.link, "left")
        .drop(df_ref_count.link)
        .fillna(0, subset=["external_refs"])
    )
    
    # Actualizar la referencia global para las búsquedas en memoria
    df = df_final

    # Escribir en un path temporal con particionado
    temp_path = cfg["hdfs_base_path"] + "_tmp"
    
    # Duplicamos columas para particionar
    df_write = df_final.withColumn("domain_p", col("domain")).withColumn("date_p", col("date"))

    (
        df_write.write
        .partitionBy("domain_p", "date_p")
        .format("avro")
        .mode("overwrite")
        .save(temp_path)
    )

    # Reemplazar el path original por el temporal ajustando nombres de directorios
    sc = spark.sparkContext
    java_import(sc._gateway.jvm, "org.apache.hadoop.fs.FileSystem")
    java_import(sc._gateway.jvm, "org.apache.hadoop.fs.Path")
    java_import(sc._gateway.jvm, "org.apache.hadoop.conf.Configuration")

    fs = sc._gateway.jvm.FileSystem.get(sc._jsc.hadoopConfiguration())

    tmp_path_obj = sc._gateway.jvm.Path(temp_path)
    original_path_obj = sc._gateway.jvm.Path(cfg["hdfs_base_path"])

    # Renombrar carpetas de estilo Hive (domain_p=x) a estilo Connector (x)
    if fs.exists(tmp_path_obj):
        for status in fs.listStatus(tmp_path_obj):
            path = status.getPath()
            name = path.getName()
            if name.startswith("domain_p="):
                new_name = name.replace("domain_p=", "")
                new_path = sc._gateway.jvm.Path(path.getParent(), new_name)
                fs.rename(path, new_path)
                
                # Entrar a subdirectorios (date)
                path = new_path
                if fs.isDirectory(path):
                    for sub_status in fs.listStatus(path):
                        sub_path = sub_status.getPath()
                        sub_name = sub_path.getName()
                        if sub_name.startswith("date_p="):
                            new_sub_name = sub_name.replace("date_p=", "")
                            new_sub_path = sc._gateway.jvm.Path(sub_path.getParent(), new_sub_name)
                            fs.rename(sub_path, new_sub_path)

    if fs.exists(original_path_obj):
        fs.delete(original_path_obj, True)
    fs.rename(tmp_path_obj, original_path_obj)

    spark.catalog.clearCache()

    # Leer todos los Avro recursivamente de nuevo
    df = (
        spark.read.format("avro")
        .option("recursiveFileLookup", "true")
        .load(cfg["hdfs_base_path"])
    )

    return f"topic {cfg['hdfs_base_path']} loaded and updated with external_refs successfully preserving structure"