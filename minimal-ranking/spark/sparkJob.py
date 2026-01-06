import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_contains, concat_ws, lit, explode, lower, count, exists, row_number, desc, when
from functools import reduce
from pyspark.sql.window import Window
from py4j.java_gateway import java_import
from pyspark.sql.functions import hash as spark_hash, split, transform, trim, array, array_union, collect_set, regexp_replace
from pyspark.sql.functions import (
    col, lit, concat_ws, row_number, explode, count, 
    lower, split, collect_list, struct, when, col, lit, 
    concat_ws, row_number, explode, split, trim, lower, 
    regexp_replace, collect_list, struct, count, size, log,
    col, lit, explode, sum as _sum, count, when, broadcast, lower
)
from pyspark.sql.functions import coalesce
from pyspark.sql.types import StructType, StructField, StringType
import math
import re
from functools import reduce

# Variables globales para mantener el estado
spark = None
sc = None
fs = None
df = None
cfg = None

def _load_raw_data(allow_missing_columns=True):
    """
    Helper function to load raw data from HDFS handling both Avro and Parquet formats.
    Returns a DataFrame containing the union of both formats if present.
    """
    df_parquet = None
    df_avro = None
    
    # 1. Intentar leer Parquet
    try:
        df_parquet = spark.read.format("parquet") \
            .option("pathGlobFilter", "*.parquet") \
            .option("recursiveFileLookup", "true") \
            .load(cfg["hdfs_base_path"])
    except Exception:
        pass 

    # 2. Intentar leer Avro
    try:
        df_avro = spark.read.format("avro") \
            .option("pathGlobFilter", "*.avro") \
            .option("recursiveFileLookup", "true") \
            .load(cfg["hdfs_base_path"])
    except Exception:
        pass 

    # 3. Unir resultados
    if df_parquet is not None and df_avro is not None:
        print("Loaded mixed Parquet and Avro data.")
        return df_parquet.unionByName(df_avro, allowMissingColumns=allow_missing_columns)
    elif df_parquet is not None:
        print("Loaded only Parquet data.")
        return df_parquet
    elif df_avro is not None:
        print("Loaded only Avro data.")
        return df_avro
    else:
        print("Warning: Could not load any data. Returning empty DataFrame.")
        schema = StructType([
            StructField("url", StringType(), True),
            StructField("domain", StringType(), True),
            StructField("real_path", StringType(), True),
            StructField("date", StringType(), True),
            StructField("content", StringType(), True), 
            StructField("external_refs", StringType(), True)
        ])
        return spark.createDataFrame([], schema=schema)

def init_spark():
    """
    Inicializa la sesión de Spark, el contexto, el sistema de archivos y carga los datos.
    Se ejecuta de manera perezosa (lazy) cuando se necesita.
    """
    global spark, sc, fs, df, cfg

    if spark is not None:
        return

    print("Initializing Spark Session...")

    # Cargar configuración
    try:
        with open("config/spark_config.json") as f:
            cfg = json.load(f)
    except Exception as e:
        print(f"Error loading config: {e}")
        raise

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
        .config("spark.sql.parquet.mergeSchema", "true")
        .getOrCreate()
    )

    # Acceso al FileSystem de HDFS
    sc = spark.sparkContext
    java_import(sc._gateway.jvm, "org.apache.hadoop.fs.FileSystem")
    java_import(sc._gateway.jvm, "org.apache.hadoop.fs.Path")
    fs = sc._gateway.jvm.FileSystem.get(sc._jsc.hadoopConfiguration())

    # Cargar datos usando el helper robusto
    df = _load_raw_data()


def search_uris(query: str, max_results: int = 100):
    """
    Busca URLs usando TF-IDF.
    Ranking:
    1. Match Exacto de URL (Navegacional)
    2. Cantidad de palabras encontradas (Coordination Factor)
    3. Score TF-IDF (Relevancia de contenido)
    4. Popularidad (External Refs)
    """
    init_spark()
    
    # Paths
    base_path = cfg["hdfs_base_path"]
    index_path = base_path + "_inverted_index"
    idf_path = base_path + "_idf_index"      # <--- NUEVO PATH
    mappings_path = base_path + "_inverted_index_mappings"
    
    print(f"Searching index at: {index_path}")

    # 1. Tokenizar query
    tokens = [w.lower() for w in re.split(r'\s+', query) if w.strip()]
    if not tokens:
        return []
    
    # 2. Cargar Datos
    try:
        df_index = spark.read.parquet(index_path)
        df_idf = spark.read.parquet(idf_path) # <--- Cargar IDF
        df_mappings = spark.read.parquet(mappings_path)
    except Exception as e:
        print(f"Error loading data: {e}")
        return []

    # 3. Filtrar Índice (Búsqueda de candidatos)
    # Usamos startswith para permitir "co" -> "coche"
    conditions = [col("token").startswith(t) for t in tokens]
    combined_condition = reduce(lambda a, b: a | b, conditions)

    # Filtramos el índice
    df_matches = df_index.filter(combined_condition)

    # 4. EXPLODE Y EXTRACCIÓN DE TF
    # El campo 'docs' ahora es un array de estructuras: [{url_hash, tf}, ...]
    df_exploded = (
        df_matches
        .select(
            col("token"),
            explode(col("docs")).alias("doc_info") 
        )
        .select(
            col("token"),
            col("doc_info.url_hash").alias("url_hash"),
            col("doc_info.tf").alias("tf") # <--- Aquí recuperamos el TF
        )
    )

    # 5. JOIN CON IDF Y CÁLCULO DE SCORE
    # Unimos con la tabla IDF para saber cuánto vale cada palabra encontrada
    # Usamos broadcast porque la tabla IDF suele ser pequeña comparada con los docs
    df_scored_tokens = (
        df_exploded.join(broadcast(df_idf), "token", "inner")
        .withColumn("term_score", col("tf") * col("idf")) # TF * IDF
    )

    # 6. AGREGACIÓN POR DOCUMENTO
    # Sumamos los scores de todas las palabras encontradas para cada URL
    df_candidates = (
        df_scored_tokens
        .groupBy("url_hash")
        .agg(
            _sum("term_score").alias("content_score"),    # Suma de relevancia (TF-IDF)
            count("token").alias("matches_count")         # Cuántas palabras distintas de la query aparecieron
        )
    )

    # 7. JOIN FINAL CON METADATOS (Título, URL, Popularidad)
    df_result_raw = df_mappings.join(df_candidates, "url_hash", "inner")

    # 8. LÓGICA DE RANKING FINAL
    clean_query = query.strip()
    
    df_ranked = (
        df_result_raw
        # --- Flags de URL ---
        .withColumn("is_exact_match", 
            when((col("url") == clean_query) | (col("url") == "https://" + clean_query), lit(1))
            .otherwise(lit(0))
        )
        .withColumn("is_url_partial_match",
            when(col("url").contains(clean_query), lit(1))
            .otherwise(lit(0))
        )
        # --- ORDENAMIENTO MULTI-NIVEL ---
        .orderBy(
            col("is_exact_match").desc(),    # 1. Si buscó la URL exacta, sale primero sí o sí.
            col("content_score").desc(),     # 3. El que tenga mejor TF-IDF (Contenido más relevante)
            col("matches_count").desc(),     # 2. El que tenga MÁS palabras de la query (Tu lógica "Strict")
            col("external_refs").desc(),     # 4. El más popular (Desempate)
            col("is_url_partial_match").desc()
        )
        .limit(max_results)
    )

    # 9. Formatear salida
    output = []
    for row in df_ranked.collect():
        output.append({
            "url": row["url"],
            "title": row["title"],
            "score": round(row["content_score"], 2), # Devolvemos el score TF-IDF
            "matches": row["matches_count"],
            "popularity": row["external_refs"]
        })

    return output


def get_crawled_data(url: str):
    """
    Obtiene toda la información crawleada para una URL específica (dominio + path).
    Retorna una lista de diccionarios con los datos encontrados.
    """
    init_spark()
    
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

    print(f"Getting crawled data for candidates: {candidates}")

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
        data.append(row_dict)
        
    return data


def process_hdfs_avro_data():
    """
    Carga datos (Parquet/Avro), calcula external_refs y sobrescribe en formato PARQUET
    manteniendo la estructura de directorios domain/date.
    """
    init_spark()
    global df

    spark.catalog.clearCache()

    # Cargar datos originales usando helper
    raw_df = _load_raw_data()
    
    # Cachear para evitar errores de lectura si borramos el origen luego
    raw_df.cache()
    print(f"Loaded {raw_df.count()} rows.")

    # --- INICIO DEDUPLICACIÓN (Mantenemos histórico diario) ---
    # Creamos la URL para poder identificar duplicados
    raw_df = raw_df.withColumn("url", concat_ws("", lit("https://"), col("domain"), col("real_path")))
    
    # 1. Limpieza Intra-día: Nos quedamos solo con 1 copia por (URL, Fecha)
    windowDaily = Window.partitionBy("url", "date").orderBy(lit(1))
    
    df_history = (
        raw_df.withColumn("rn", row_number().over(windowDaily))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    # --- FIN DEDUPLICACIÓN ---

    # --- CÁLCULO DE RANKING (Solo basado en la foto MÁS RECIENTE) ---
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
    if "external_refs" in df_history.columns:
        df_history = df_history.drop("external_refs")

    df_final = (
        df_history.join(df_ref_count, df_history.url == df_ref_count.link, "left")
        .drop(df_ref_count.link)
        .fillna(0, subset=["external_refs"])
    )
    
    # Actualizar la referencia global
    df = df_final

    # Escribir en un path temporal con particionado PARQUET
    temp_path = cfg["hdfs_base_path"] + "_tmp"
    
    # Duplicamos columas para particionar (Si usamos dynamic partition overwrite podria ser mas limpio, pero rename es seguro)
    df_write = df_final.withColumn("domain_p", col("domain")).withColumn("date_p", col("date"))

    (
        df_write.write
        .partitionBy("domain_p", "date_p")
        .format("parquet") # CAMBIO A PARQUET
        .mode("overwrite")
        .save(temp_path)
    )

    # Reemplazar el path original por el temporal
    sc = spark.sparkContext
    java_import(sc._gateway.jvm, "org.apache.hadoop.fs.FileSystem")
    java_import(sc._gateway.jvm, "org.apache.hadoop.fs.Path")

    fs = sc._gateway.jvm.FileSystem.get(sc._jsc.hadoopConfiguration())

    tmp_path_obj = sc._gateway.jvm.Path(temp_path)
    original_path_obj = sc._gateway.jvm.Path(cfg["hdfs_base_path"])

    # Renombrar carpetas
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

    # Borrar original y mover temp a original
    if fs.exists(original_path_obj):
        fs.delete(original_path_obj, True)
    fs.rename(tmp_path_obj, original_path_obj)

    spark.catalog.clearCache()

    # Recargar DF global usando helper robusto
    df = _load_raw_data()

    return f"topic {cfg['hdfs_base_path']} processed (Avro->Parquet) and updated with external_refs successfully"


def create_invert_index(output_path: str = None):
    """
    Crea un índice invertido guardando TF (Frecuencia del término).
    Output Schema: Token -> [{url_hash: 123, tf: 5}, {url_hash: 456, tf: 1}]
    """
    init_spark()
    
    if not output_path:
        output_path = cfg["hdfs_base_path"] + "_inverted_index"

    raw_df = _load_raw_data()

    # --- Preprocesamiento (Igual que tu código) ---
    df_with_url = raw_df.withColumn("url", concat_ws("", lit("https://"), col("domain"), col("real_path")))

    windowLatest = Window.partitionBy("url").orderBy(col("date").desc())
    df_latest = (
        df_with_url.withColumn("rn", row_number().over(windowLatest))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    df_tokens = df_latest.withColumn("all_text", 
        concat_ws(" ", 
            col("domain"), 
            regexp_replace(col("real_path"), "[/-]", " "),
            col("url"),
            concat_ws(" ", col("tags")),
            concat_ws(" ", col("content.texts.h1")),
            concat_ws(" ", col("content.texts.h2")),
            concat_ws(" ", col("content.texts.p"))
        )
    )

    df_clean_text = df_tokens.withColumn("clean_text", regexp_replace(lower(col("all_text")), "[^a-z0-9áéíóúñ]+", " "))
    
    # 1. Explotar para tener 1 fila por cada ocurrencia de palabra
    df_exploded = (
        df_clean_text.select(
            col("url"),
            spark_hash(col("url")).alias("url_hash"), # spark_hash
            explode(split(trim(col("clean_text")), "\\s+")).alias("token")
        )
    ).filter(col("token") != "")

    # 2. CALCULAR TF: Contamos cuántas veces aparece cada token en cada URL
    # GroupBy (Token, URL) -> Count
    df_tf = (
        df_exploded
        .groupBy("token", "url_hash")
        .count()
        .withColumnRenamed("count", "tf") # Term Frequency
    )

    # 3. Agrupar final para el índice invertido
    # Ahora guardamos una lista de ESTRUCTURAS: [{url: hash, tf: count}, ...]
    df_inverted_index = (
        df_tf
        .groupBy("token")
        .agg(collect_list(struct(col("url_hash"), col("tf"))).alias("docs"))
    )

    # Guardar Índice Invertido
    (
        df_inverted_index
        .orderBy("token")
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    # Guardar Mappings (Igual que antes, necesario para recuperar titulos)
    df_mappings = df_latest.select(
        spark_hash(col("url")).alias("url_hash"), 
        col("url"),
        coalesce(col("content.texts.h1")[0], col("content.texts.h2")[0], col("domain")).alias("title"),
        col("external_refs")
    ).distinct()
    
    (
        df_mappings.write
        .mode("overwrite")
        .parquet(output_path + "_mappings")
    )

    return f"Inverted index with TF created at {output_path}"


def get_inverted_index_sample(limit: int = 20, token: str = None):
    """
    Devuelve una muestra del índice invertido para inspección.
    Permite filtrar por un token específico para verificar integridad.
    """
    init_spark()
    
    index_path = cfg["hdfs_base_path"] + "_inverted_index"
    
    try:
        df_index = spark.read.parquet(index_path)
        
        if token:
            df_index = df_index.filter(col("token") == token)
            
        # Convertir a lista de dicts
        # Ojo: 'docs' es una lista de hashes.
        rows = df_index.limit(limit).collect()
        result = [row.asDict() for row in rows]
        return result
    except Exception as e:
        return {"error": f"Could not read index at {index_path}: {str(e)}"}
    
def create_idf_index(index_path: str = None):
    """
    Calcula el IDF global leyendo el índice invertido ya procesado.
    Output: Token -> IDF Score
    """
    init_spark()
    
    if not index_path:
        base_path = cfg["hdfs_base_path"]
        index_path = base_path + "_inverted_index"
        mappings_path = base_path + "_inverted_index_mappings"
        output_path = base_path + "_idf_index"
    else:
        # Asumimos convenciones de nombres si se pasa path custom
        mappings_path = index_path + "_mappings"
        output_path = index_path.replace("_inverted_index", "") + "_idf_index"

    print(f"Reading index from: {index_path}")

    # 1. Obtener N (Número total de documentos)
    try:
        df_mappings = spark.read.parquet(mappings_path)
        total_docs = df_mappings.count()
        print(f"Total documents (N): {total_docs}")
    except Exception as e:
        print(f"Error reading mappings for count: {e}")
        return

    # 2. Cargar Índice Invertido
    df_index = spark.read.parquet(index_path)

    # 3. Calcular IDF
    # DF (Document Frequency) = size(docs) -> En cuántas URLs aparece el token
    # IDF = log( (N + 1) / (DF + 1) )
    
    df_idf_calc = (
        df_index
        .select(
            col("token"),
            size(col("docs")).alias("doc_freq")
        )
        .withColumn("idf", log((lit(total_docs) + 1) / (col("doc_freq") + 1)))
    )

    # 4. Guardar tabla pequeña de IDF
    # Esta tabla es muy ligera (solo 1 fila por palabra única)
    (
        df_idf_calc
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    return f"IDF Index created at {output_path}"