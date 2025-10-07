echo '{"domain":"example.com","path":"/home","date":"2025-10-04","tags":["tag1","tag2"],"content":{"links":{"http":["http://a.com"],"https":["https://b.com"],"internal":["/home"]},"metadata":{"title":["Example Title"]},"imgs":{"imgs":["img1.png"]},"texts":{"h1":["H1 text"],"h2":["H2 text"],"p":["Paragraph text"]}}}' \
| kafka-console-producer.sh --broker-list localhost:9092 --topic your-topic-name --property "parse.key=true" --property "key.separator=:"


kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic your-topic-name --from-beginning --max-messages 1
