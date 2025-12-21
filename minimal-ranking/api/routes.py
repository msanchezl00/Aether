from fastapi import APIRouter
from pydantic import BaseModel
from spark.sparkJob import search_uris, process_hdfs_avro_data, get_crawled_data, create_invert_index, init_spark, get_inverted_index_sample

router = APIRouter()

class Query(BaseModel):
    text: str

@router.post("/search")
def search(q: Query):
    try:
        urls = search_uris(q.text)
        return {"urls": urls}
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return {"error": str(e), "traceback": traceback.format_exc()}

@router.get("/process_hdfs_avro_data")
def process_data():
    try:
        result = process_hdfs_avro_data()
        return {"message": result}
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return {"error": str(e), "traceback": traceback.format_exc()}

@router.get("/get_crawled_data")
def get_crawled_data_route(url: str):
    try:
        result = get_crawled_data(url)
        return result
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return {"error": str(e), "traceback": traceback.format_exc()}

@router.post("/create_invert_index")
def create_index(output_path: str = None):
    result = create_invert_index(output_path)
    return {"message": result}

@router.get("/get_inverted_index")
def get_index_sample(limit: int = 20):
    try:
        result = get_inverted_index_sample(limit)
        return result
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return {"error": str(e), "traceback": traceback.format_exc()}

@router.get("/init_spark")
def init_spark_route():
    init_spark()
    return {"message": "Spark session initialized successfully"}