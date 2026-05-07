FROM apache/spark:3.5.1

USER root

RUN mkdir -p /opt/spark/jars

# Spark Kafka connector
RUN curl -L -o /opt/spark/jars/spark-sql-kafka.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar

# Kafka client
RUN curl -L -o /opt/spark/jars/kafka-clients.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar

# Token provider
RUN curl -L -o /opt/spark/jars/spark-token-provider.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar

# ✅ MISSING DEPENDENCY (THIS CAUSED YOUR ERROR)
RUN curl -L -o /opt/spark/jars/commons-pool2.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

USER sparkFROM apache/spark:3.5.1

USER root

# Create jars directory (if not exists)
RUN mkdir -p /opt/spark/jars

# Download Kafka connector + dependencies
RUN curl -L -o /opt/spark/jars/spark-sql-kafka.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar

RUN curl -L -o /opt/spark/jars/kafka-clients.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar

RUN curl -L -o /opt/spark/jars/spark-token-provider.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar

# Switch back to spark user
USER spark