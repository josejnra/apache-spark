FROM openjdk:11

#############################
# SETUP HADOOP
#############################

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y software-properties-common \
                       ssh \
                       pdsh \
                       openssh-server \
                       rsync \
                       net-tools \
                       vim \
                       python3-pip

ENV HADOOP_VERSION 3.3.6

WORKDIR /opt

# create ssh
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

RUN wget https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar zxf hadoop-${HADOOP_VERSION}.tar.gz && \
    rm -r hadoop-${HADOOP_VERSION}.tar.gz

# set java version to hadoop env
RUN echo "export JAVA_HOME=$(echo $JAVA_HOME)" >> /opt/hadoop-${HADOOP_VERSION}/etc/hadoop/hadoop-env.sh

ENV PATH=$PATH:/opt/hadoop-${HADOOP_VERSION}/bin

# Hadoop env
ENV HDFS_NAMENODE_USER root
ENV HDFS_DATANODE_USER root
ENV HDFS_SECONDARYNAMENODE_USER root
ENV YARN_RESOURCEMANAGER_USER root
ENV YARN_NODEMANAGER_USER root
ENV YARN_CONF_DIR /opt/hadoop-${HADOOP_VERSION}/etc/hadoop
ENV HADOOP_HOME /opt/hadoop-${HADOOP_VERSION}
ENV HADOOP_LOG_DIR $HADOOP_HOME/logs
ENV HADOOP_CONF_DIR /opt/hadoop-${HADOOP_VERSION}/etc/hadoop
ENV LD_LIBRARY_PATH /opt/hadoop-${HADOOP_VERSION}/lib/native:$LD_LIBRARY_PATH

# expose several ports
EXPOSE 9871 9870 9869 9868 9867 9866 9865 9864 9820 9000 8088

#############################
# SETUP SPARK
#############################

ENV SPARK_VERSION 3.5.0

RUN wget -P /opt https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-without-hadoop.tgz && \
    tar zxf /opt/spark-${SPARK_VERSION}-bin-without-hadoop.tgz -C /opt && \
    rm /opt/spark-${SPARK_VERSION}-bin-without-hadoop.tgz

ENV SPARK_HOME /opt/spark-${SPARK_VERSION}-bin-without-hadoop
ENV PYSPARK_PYTHON python3
ENV PATH $PATH:/opt/spark-${SPARK_VERSION}-bin-without-hadoop/bin

RUN echo "export JAVA_HOME=$JAVA_HOME" >> /opt/spark-${SPARK_VERSION}-bin-without-hadoop/sbin/spark-config.sh && \
    echo "export SPARK_DIST_CLASSPATH=$(hadoop classpath)" >> ~/.bashrc

EXPOSE 7077 4040

#############################
# SETUP LIVY
#############################

ENV LIVY_VERSION 0.7.1

RUN wget -P /opt https://dlcdn.apache.org/incubator/livy/${LIVY_VERSION}-incubating/apache-livy-${LIVY_VERSION}-incubating-bin.zip && \
    unzip /opt/apache-livy-${LIVY_VERSION}-incubating-bin.zip -d /opt && \
    mv /opt/apache-livy-${LIVY_VERSION}-incubating-bin /opt/apache-livy && \
    rm /opt/apache-livy-*

EXPOSE 8998

#############################
# SETUP PYTHON DEPS
#############################

RUN python3 -m pip install jupyterlab \
                           ipython \
                           pyspark==${SPARK_VERSION} \
                           delta-spark==3.0.0 \
                           pandas==2.1.2 \
                           spylon-kernel

RUN python3 -m spylon_kernel install

EXPOSE 8888

ENTRYPOINT ["/bin/bash"]
