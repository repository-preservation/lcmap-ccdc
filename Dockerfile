FROM centos:7
LABEL maintainer="USGS EROS LCMAP http://eros.usgs.gov http://github.com/usgs-eros"
LABEL description="CentOS based Mesos Image for LCMAP"

ENV SPARK_HOME=/opt/spark
ENV SPARK_NO_DAEMONIZE=true
ENV PYSPARK_PYTHON=/home/firebird/miniconda3/bin/python3
ENV MESOS_NATIVE_JAVA_LIBRARY=/usr/lib/libmesos.so
ENV PATH=/home/firebird/miniconda3/bin:$SPARK_HOME/bin:${PATH}
ENV PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$SPARK_HOME/python/lib/pyspark.zip

ENV LC_ALL=en_US.UTF-8 \
    LANG=en_US.UTF-8

EXPOSE 8081 4040 8888

RUN yum update -y && \
    yum install -y bzip2 java-1.8.0-openjdk-devel.i686 && \
    yum -y install http://repos.mesosphere.io/el/7/noarch/RPMS/mesosphere-el-repo-7-1.noarch.rpm && \
    yum install -y mesos  && \
    yum -y downgrade mesos-1.1.0

RUN cd /opt && \
    curl https://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz -o spark.tgz && \
    tar -zxf spark.tgz && \
    rm -rf spark.tgz && \
    ln -s spark-* spark

RUN adduser -ms /bin/bash firebird
USER firebird
WORKDIR /home/firebird

RUN curl https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o mc.sh && \
    chmod 755 mc.sh && \
    ./mc.sh -b && \
    rm -rf mc.sh && \
    conda config --add channels conda-forge && \
    conda install cython gdal --yes && \
    conda clean --all

COPY firebird .
COPY notebooks notebook
COPY resources .
COPY test .
COPY Makefile .
COPY pom.xml .
COPY README.md .
COPY setup.py .
COPY version.py .

USER root
RUN cd /home/firebird && \
    yum install -y maven && \
    mvn dependency:copy-dependencies -DoutputDirectory=$SPARK_HOME/jars && \
    yum erase -y maven && \
    yum clean all


WORKDIR /home/firebird
