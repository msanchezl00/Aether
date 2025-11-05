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
    q = query.lower()

    # URLs encontradas según tags/textos
    df_filtered = df.filter(
        array_contains(col("tags"), q) |
        exists(col("content.texts.h1"), lambda x: lower(x).contains(q)) |
        exists(col("content.texts.h2"), lambda x: lower(x).contains(q)) |
        exists(col("content.texts.p"), lambda x: lower(x).contains(q))
    ).withColumn(
        "url", concat_ws("", lit("https://"), col("domain"), col("real_path"))
    ).select("url").distinct()

    # Usar external_refs ya precalculado
    df_score = df.select("url", "external_refs").distinct()

    #  Unir con las URLs encontradas y ordenar por referencias
    df_result = (
        df_filtered.join(df_score, df_filtered.url == df_score.url, "left")
        .fillna(0, subset=["external_refs"])
        .orderBy(col("external_refs").desc())
    )

    # Iterar localmente sin saturar driver
    urls_with_score = []
    for row in df_result.toLocalIterator():
        urls_with_score.append({"url": row["url"], "score": row["external_refs"]})
        if len(urls_with_score) >= max_results:
            break

    return urls_with_score


def load_hdfs_parquet_data():
    """
    Carga datos desde HDFS, calcula external_refs y escribe Avro actualizado.
    """
    global df
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
    df = df.join(df_ref_count, df.url == df_ref_count.link, "left") \
           .drop("link") \
           .fillna(0, subset=["external_refs"])

    # Guardar versión actualizada en nuevo path HDFS
    df.write.format("avro").mode("overwrite").save(cfg["hdfs_base_path"])

    return f"topic {cfg['hdfs_base_path']} loaded and updated with external_refs successfully"