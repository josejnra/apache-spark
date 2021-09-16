FROM openjdk:8-jdk-alpine

ARG SPARK_VERSION=3.1.2

RUN wget -P /opt https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.2.tgz && \
    tar zxf /opt/spark-${SPARK_VERSION}-bin-hadoop3.2.tgz -C /opt && \
    rm /opt/spark-${SPARK_VERSION}-bin-hadoop3.2.tgz

RUN apk add --update --no-cache python3 py3-pip bash vim

ENV PATH=$PATH:/opt/spark-${SPARK_VERSION}-bin-hadoop3.2/bin
ENV SPARK_HOME=/opt/spark-${SPARK_VERSION}-bin-hadoop3.2
ENV PYSPARK_PYTHON=python3

EXPOSE 4040 7077 8080

ENTRYPOINT ["tail", "-f", "/dev/null"]
