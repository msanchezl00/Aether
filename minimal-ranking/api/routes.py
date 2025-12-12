from fastapi import APIRouter
from pydantic import BaseModel
from spark.sparkJob import search_uris, load_hdfs_parquet_data, get_crawled_data

router = APIRouter()

class Query(BaseModel):
    text: str

@router.post("/search")
def search(q: Query):
    urls = search_uris(q.text)
    return {"urls": urls}

@router.get("/load_hdfs_parquet_data")
def load_data():
    result = load_hdfs_parquet_data()
    return {"message": result}

@router.get("/get_crawled_data")
def get_crawled_data_route(url: str):
    result = get_crawled_data(url)
    return result