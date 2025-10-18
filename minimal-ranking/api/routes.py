from fastapi import APIRouter
from pydantic import BaseModel
from spark.sparkJob import search_uris, load_hdfs_parquet_data

router = APIRouter()

class Query(BaseModel):
    text: str

@router.post("/search")
def search(q: Query):
    urls = search_uris(q.text)
    return {"urls": urls}

@router.get("/load_hdfs_parquet_data")
def search():
    result = load_hdfs_parquet_data()
    return {result}