# spark.Dockerfile

FROM apache/spark:3.5.1

USER root

# Create jars directory
RUN mkdir -p /opt/spark/jars

# Install Python dependencies for ASP
RUN pip install pandas pyarrow

# Spark Kafka connector
RUN curl -L -o /opt/spark/jars/spark-sql-kafka.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar

# Kafka client
RUN curl -L -o /opt/spark/jars/kafka-clients.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar

# Spark token provider
RUN curl -L -o /opt/spark/jars/spark-token-provider.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar

# Required dependency
RUN curl -L -o /opt/spark/jars/commons-pool2.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

USER spark