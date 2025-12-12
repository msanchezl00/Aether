import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_contains, concat_ws, lit
from py4j.java_gateway import java_import
from pyspark.sql.functions import col, array_contains, explode, lower, concat_ws, lit, count, exists, explode, col, count, concat_ws, lit

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
        df.withColumn("constructed_url", concat_ws("", lit("https://"), col("domain"), col("real_path")))
        .filter(col("constructed_url").isin(candidates))
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
    df = (
        spark.read.format("avro")
        .option("recursiveFileLookup", "true")
        .load(cfg["hdfs_base_path"])
    )

    # Crear DataFrame con todos los links salientes
    df_links = (
        df.select(explode(col("content.links.https")).alias("link"))
        .union(df.select(explode(col("content.links.http")).alias("link")))
    )

    # Contar cuántas veces cada link aparece referenciado
    df_ref_count = (
        df_links.groupBy("link")
        .agg(count("*").alias("external_refs"))
    )

    # Crear URL canónica por página
    df = df.withColumn("url", concat_ws("", lit("https://"), col("domain"), col("real_path")))

    # Unir conteo con el dataset original
    if "external_refs" in df.columns:
        df = df.drop("external_refs")

    df = (
        df.join(df_ref_count, df.url == df_ref_count.link, "left")
        .drop(df_ref_count.link)
        .fillna(0, subset=["external_refs"])
    )

    # Escribir en un path temporal con particionado
    # Usamos particionado para mantener la estructura, luego corregimos nombres de carpetas
    temp_path = cfg["hdfs_base_path"] + "_tmp"
    
    # Duplicamos columas para particionar sin perderlas del contenido del archivo
    df_write = df.withColumn("domain_p", col("domain")).withColumn("date_p", col("date"))

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