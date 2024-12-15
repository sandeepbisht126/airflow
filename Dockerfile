FROM apache/airflow:2.7.3
# RUN pip install apache-airflow-providers-apache-spark
RUN pip install "apache-airflow[celery,postgres,redis]==2.7.3"

USER root
# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get install vim -y && \
    apt-get -y install vim nano && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow