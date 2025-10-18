from fastapi import FastAPI
from api.routes import router
import sys
print(sys.path)
app = FastAPI()
app.include_router(router)

# uvicorn main:app --host 0.0.0.0 --port 8000
