FROM openjdk:8-jdk-slim

# Instalar Python y pip
RUN apt-get update && apt-get install -y python3 python3-pip && rm -rf /var/lib/apt/lists/*

# Instalar PySpark y FastAPI
RUN pip3 install --no-cache-dir pyspark==3.5.1 fastapi uvicorn[standard] py4j

WORKDIR /app
COPY . /app
EXPOSE 8000
CMD ["bash", "-c", "PYTHONPATH=/app uvicorn main:app --host 0.0.0.0 --port 8000"]
