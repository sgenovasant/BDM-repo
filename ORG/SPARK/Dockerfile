FROM bitnami/spark:3.2.4

USER root

# Update and install necessary packages
RUN apt-get update && apt-get install -y \
    iputils-ping \
    curl

# Copy and install Python requirements
COPY requirements.txt .
RUN pip install -r requirements.txt

# Install MongoDB connectors
USER 1001
ARG MONGO_SPARK_CONNECTOR_VERSION=10.2.2
RUN curl -L https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/${MONGO_SPARK_CONNECTOR_VERSION}/mongo-spark-connector_2.12-${MONGO_SPARK_CONNECTOR_VERSION}.jar -o /opt/bitnami/spark/jars/mongo-spark-connector_2.12-${MONGO_SPARK_CONNECTOR_VERSION}.jar \
    && curl -L https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/5.1.0/mongodb-driver-sync-5.1.0.jar -o /opt/bitnami/spark/jars/mongodb-driver-sync-5.1.0.jar \
    && curl -L https://repo1.maven.org/maven2/org/mongodb/bson/5.1.0/bson-5.1.0-sources.jar -o /opt/bitnami/spark/jars/bson-5.1.0-sources.jar \
    && curl -L https://repo1.maven.org/maven2/org/mongodb/bson/5.1.0/bson-5.1.0.jar -o /opt/bitnami/spark/jars/bson-5.1.0.jar \
    && curl -L https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/5.1.0/mongodb-driver-core-5.1.0.jar -o /opt/bitnami/spark/jars/mongodb-driver-core-5.1.0.jar \
    && curl -L https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.6/slf4j-api-1.7.6.jar -o /opt/bitnami/spark/jars/slf4j-api-1.7.6.jar \
    && curl -L https://repo1.maven.org/maven2/org/mongodb/bson-record-codec/5.1.0/bson-record-codec-5.1.0.jar -o /opt/bitnami/spark/jars/bson-record-codec-5.1.0.jar

# Restore default user
USER 1001