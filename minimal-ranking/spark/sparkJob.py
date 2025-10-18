import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_contains, concat_ws, lit
from py4j.java_gateway import java_import
from pyspark.sql.functions import col, array_contains, explode, lower, concat_ws, lit, count, exists


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

    # 1️⃣ URLs encontradas según tags/textos
    df_filtered = df.filter(
        array_contains(col("tags"), q) |
        exists(col("content.texts.h1"), lambda x: lower(x).contains(q)) |
        exists(col("content.texts.h2"), lambda x: lower(x).contains(q)) |
        exists(col("content.texts.p"), lambda x: lower(x).contains(q))
    ).withColumn(
        "url", concat_ws("", lit("https://"), col("domain"), col("real_path"))
    ).select("url").distinct()

    urls_tags = [row["url"] for row in df_filtered.collect()]

    # 2️⃣ Explode links para contar referencias
    df_links = df.select(
        explode(col("content.links.https")).alias("link")
    ).union(
        df.select(explode(col("content.links.http")).alias("link"))
    )

    # 3️⃣ Contar cuántas veces cada URL de tags aparece en links
    df_score = df_links.filter(col("link").isin(urls_tags)) \
                       .groupBy("link").agg(count("*").alias("ref_count"))

    # 4️⃣ Unir con las URLs encontradas y ordenar por referencias
    df_result = df_filtered.join(df_score, df_filtered.url == df_score.link, "left") \
                           .fillna(0, subset=["ref_count"]) \
                           .orderBy(col("ref_count").desc())

    # 5️⃣ Iterar localmente sin saturar driver
    urls_with_score = []
    for row in df_result.toLocalIterator():
        urls_with_score.append({"url": row["url"], "score": row["ref_count"]})
        if len(urls_with_score) >= max_results:
            break

    return urls_with_score


def load_hdfs_parquet_data():
    """
    Carga datos desde HDFS y devuelve mensaje de éxito.
    """
    global df
    df = (
        spark.read.format("avro")
        .option("recursiveFileLookup", "true")
        .load(cfg["hdfs_base_path"])
    )

    return "topic" + cfg["hdfs_base_path"] + "loaded from hdfs successfully"