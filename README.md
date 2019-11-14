# SF-Crime-Statistics-With-SparkStreaming
In this project, I am dealing with real-world dataset, extracted from Kaggle, on San Francisco crime incidents. I have done statistical analysis of the data using Apache Spark Structured Streaming. Various steps involved are:

1. The first step is to build a simple Kafka server
2. To see if I have correctly implemented the server, used the command:

   bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <your-topic-name> --from-beginning
3. Implemented features in data_stream.py
4. Did a spark-submit using this command:
   
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 --master local[4] data_stream.py
