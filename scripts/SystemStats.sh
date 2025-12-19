hadoop jar hadoop-mapreduce-client-jobclient-*.jar TestDFSIO \
  -write -nrFiles 20 -fileSize 1GB 2>&1 \
  | grep -E "Throughput mb/sec|Test exec time sec" \
  | tr -d ' ' \
  | tr ':' ',' \
  > hadoop_results.csv

  spark-submit \
  --master yarn \
  --class org.apache.spark.examples.SparkPi \
  spark-examples*.jar 5000 2>&1 \
  | grep "Job 0 finished" \
  | awk '{print "spark_duration_sec," $(NF-1)}' \
  > spark_results.csv

  kafka-producer-perf-test.sh \
  --topic test \
  --num-records 2000000 \
  --record-size 1000 \
  --throughput -1 \
  --producer-props bootstrap.servers=broker1:9092 \
  | grep "records sent" \
  | sed 's/[()]//g' \
  | awk '{print NR","$4","$6}' \
  > kafka_producer.csv