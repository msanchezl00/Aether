import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_contains, concat_ws, lit, explode, lower, count, exists, row_number, desc, when
from functools import reduce
from pyspark.sql.window import Window
from py4j.java_gateway import java_import
from pyspark.sql.functions import hash as spark_hash, split, transform, trim, array, array_union, collect_set, regexp_replace

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
        from pyspark.sql.types import StructType, StructField, StringType
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
    Busca URLs usando el índice invertido (Token -> URL Hash) y luego filtra el contenido.
    """
    init_spark()
    
    # Path del índice y mappings
    index_path = cfg["hdfs_base_path"] + "_inverted_index"
    mappings_path = cfg["hdfs_base_path"] + "_inverted_index_mappings"
    print(f"Searching index at: {index_path}")

    
    # 1. Tokenizar query
    # Estandarización manual simple alineada con el indexer
    import re
    tokens = [w.lower() for w in re.split(r'\s+', query) if w.strip()]
    
    if not tokens:
        return []

    # 2. Cargar Índice y filtrar candidatos
    try:
        df_index = spark.read.parquet(index_path)
    except Exception as e:
        print(f"Error loading inverted index: {e}. Fallback to full scan?")
        return [] # O implementar fallback a scan completo

    # Filtrar solo rows que coincidan con los tokens
    # GroupBy y contar coincidencias para ranking básico pre-filtro?
    # Por ahora: obtener TODOS los hashes que contengan AL MENOS UNO de los tokens (OR logic)
    # Si quisieramos AND logic, haríamos intersection.
    
    # Explotamos los docs para tener: token | url_hash
    # CAMBIO: Usamos 'startswith' para permitir búsqueda parcial (ej: "co" -> "coche")
    conditions = [col("token").startswith(t) for t in tokens]
    combined_condition = reduce(lambda a, b: a | b, conditions)

    df_matches = (
        df_index.filter(combined_condition)
        .select(explode(col("docs")).alias("url_hash"))
    )

    df_candidates = (
        df_matches
        .groupBy("url_hash")
        .count() 
        .withColumnRenamed("count", "match_count")
    )

    total_keywords = len(tokens)

    df_candidates_lax = (
        df_candidates
        .filter(col("match_count") < total_keywords)
    )
    
    df_candidates_strict = (
        df_candidates
        .filter(col("match_count") >= total_keywords)
    )

    # Recolectar candidatos (Si son pocos, es rápido. Si son millones, mejor join)
    # Asumimos que para un buscador "minimal", collect es manejable o usamos join broadcast.
    # Usaremos Join para escalar.

    # 3. Cargar Mappings (Lightweight: Hash -> Title, URL, Score)
    try:
        df_mappings = spark.read.parquet(mappings_path)
    except Exception as e:
        print(f"Error loading mappings: {e}")
        return []

    # 4. Join con Mappings (RÁPIDO)
    # Solo nos quedamos con las páginas que aparecieron en el índice
    df_filtered_lax = df_mappings.join(df_candidates_lax, "url_hash", "inner")
    df_filtered_strict = df_mappings.join(df_candidates_strict, "url_hash", "inner")

    # 2. Unimos ambos DataFrames (STRICT y LAX)
    df_combined = df_filtered_strict.unionByName(df_filtered_lax)

    # 3. Lógica de Match de URL (Exacto y Parcial)
    clean_query = query.strip()

    df_result = (
        df_combined
            # --- Lógica de URL Exacta ---
            .withColumn("is_exact_match", 
                when((col("url") == clean_query) | (col("url") == "https://" + clean_query), lit(1))
                .otherwise(lit(0))
            )
            .withColumn("is_url_partial_match",
                when(col("url").contains(clean_query), lit(1))
                .otherwise(lit(0))
            )
            # --- ORDENAMIENTO GRADUAL ---
            .orderBy(
                col("is_exact_match").desc(),      # 1. URL Exacta (El rey)
                col("match_count").desc(),         # 2. CANTIDAD DE COINCIDENCIAS (Aquí está tu lógica gradual)
                col("external_refs").desc(),       # 3. Popularidad (Desempate si tienen mismos aciertos)
                col("is_url_partial_match").desc() # 4. Coincidencia visual de URL
            )
            .limit(max_results)
    )

    # Iterar resultados
    urls_with_score = []
    for row in df_result.collect():
        urls_with_score.append({
            "url": row["url"], 
            "title": row["title"], # Added Title
            "score": row["external_refs"] if "external_refs" in row else 0
        })

    return urls_with_score


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
    Crea un índice invertido (Token -> [Lista de Hashes de URL]) a partir de la última versión
    de cada página crawleada.
    Se normaliza el texto (h1, h2, p), se tokeniza y se agrupa.
    """
    init_spark()
    
    if not output_path:
        output_path = cfg["hdfs_base_path"] + "_inverted_index"

    # Cargar datos originales del helper
    raw_df = _load_raw_data()

    # Construir URL
    df_with_url = raw_df.withColumn("url", concat_ws("", lit("https://"), col("domain"), col("real_path")))

    # Ventana para quedarse con la última fecha
    windowLatest = Window.partitionBy("url").orderBy(col("date").desc())

    df_latest = (
        df_with_url.withColumn("rn", row_number().over(windowLatest))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # ... Resto igual ...
    
    # AÑADIDO: domain, real_path (split por / y -), tags
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

    # Tokenizar: Limpiar texto (dejar solo letras y números) y luego separar
    # Reemplazamos todo lo que NO sea alfanumérico por espacio
    df_clean_text = df_tokens.withColumn("clean_text", regexp_replace(lower(col("all_text")), "[^a-z0-9áéíóúñ]+", " "))

    df_exploded = (
        df_clean_text.select(
            col("url"),
            spark_hash(col("url")).alias("url_hash"),
            explode(split(trim(col("clean_text")), "\\s+")).alias("token")
        )
    )

    df_clean_tokens = df_exploded.filter(col("token") != "")

    # Agrupar
    df_inverted_index = (
        df_clean_tokens.groupBy("token")
        .agg(collect_set("url_hash").alias("docs"))
    )

    # Guardar en HDFS (Parquet)
    # OPTIMIZACIÓN: Ordenar por token.
    # Esto permite que Parquet use "predicate pushdown" eficiente.
    # Aunque no es una Hash Table (O(1)), al estar ordenado actúa como un B-Tree (O(log n)),
    # lo cual es perfecto para búsquedas exactas Y prefix ("co" -> "coche").
    (
        df_inverted_index
        .orderBy("token")
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    # Guardar mapping (Hash -> URL, Title, Score) para visualización rápida
    # Extraemos el título (Primer H1, o H2, o Dominio)
    from pyspark.sql.functions import coalesce
    
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

    return f"Inverted index created at {output_path}"


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