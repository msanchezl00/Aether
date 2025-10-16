
curl -X POST -H "Content-Type: application/vnd.kafka.json.v2+json" \
  --data '{"records":[{"value":{"domain":"example.com","path":"/home","date":"2025-10-04","tags":["tag1","tag2"],"content":{"links":{"http":["http://a.com"],"https":["https://b.com"],"internal":["/home"]},"metadata":{"title":["Example Title"]},"imgs":{"imgs":["img1.png"]},"texts":{"h1":["H1 text"],"h2":["H2 text"],"p":["Paragraph text"]}}}}]}' \
  http://localhost:8082/topics/parquet_data



curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" \
  --data '{"name": "my_consumer", "format": "json", "auto.offset.reset": "earliest"}' \
  http://localhost:8082/consumers/parquet_data


hdfs dfs -chmod -R 777 /

curl -X POST http://localhost:8083/connectors/hdfs-sink/tasks/0/restart
