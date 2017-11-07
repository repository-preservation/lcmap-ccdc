FROM centos:7.3.1611

LABEL maintainer="USGS EROS LCMAP http://eros.usgs.gov http://github.com/usgs-eros/lcmap-firebird"
LABEL description="CentOS based Spark-Mesos image for LCMAP"
LABEL mode="operator"
LABEL org.apache.mesos.version=1.4.0
LABEL org.apache.spark.version=2.2.0
LABEL net.java.openjdk.version=1.8.0
LABEL org.python.version=3.6
LABEL org.centos=7.3.1611

EXPOSE 8081 4040 8888

RUN yum update -y && \
    yum install -y sudo java-1.8.0-openjdk-devel.x86_64 && \
    yum install -y http://repos.mesosphere.io/el/7/noarch/RPMS/mesosphere-el-repo-7-3.noarch.rpm && \
    yum install -y mesos && \
    yum -y downgrade mesos-1.4.0 && \
    yum -y install https://centos7.iuscommunity.org/ius-release.rpm && \
    yum -y install python36u && \
    ln -s /usr/bin/python3.6 /usr/local/bin/python && \
    yum -y install python36u-pip python36u-devel && \
    ln -s /usr/bin/pip3.6 /usr/local/bin/pip && \
    yum -y install gcc Cython && \
    yum -y erase gcc && \
    yum clean all && \
    rm -rf /var/cache/yum

RUN localedef -i en_US -f UTF-8 en_US.UTF-8

##########################################################################
# Add a user to run as inside the container.  This will prevent accidental
# foo while mounting volumes.  To enable access between external and
# internal users on mounted volume files, set 'other' perms appropriately.
##########################################################################
RUN adduser -ms /bin/bash lcmap && \
    echo "lcmap ALL=(root) NOPASSWD:ALL" > /etc/sudoers.d/lcmap && \
    chmod 0440 /etc/sudoers.d/lcmap

ENV HOME=/home/lcmap
ENV USER=lcmap
USER $USER
WORKDIR $HOME
##########################################################################

ENV SPARK_HOME=/opt/spark
ENV SPARK_NO_DAEMONIZE=true
ENV PYSPARK_PYTHON=/usr/local/bin/python
ENV MESOS_NATIVE_JAVA_LIBRARY=/usr/lib/libmesos.so
ENV PATH=$SPARK_HOME/bin:${PATH}
ENV PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$SPARK_HOME/python/lib/pyspark.zip
ENV LC_ALL=en_US.UTF-8
ENV LANG=en_US.UTF-8

RUN cd /opt && \
    sudo curl https://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz -o spark.tgz && \
    sudo tar -zxf spark.tgz && \
    sudo rm -f spark.tgz && \
    sudo ln -s spark-* spark

# Install spark-cassandra-connector
COPY pom.xml .
RUN cd $HOME && \
    sudo yum install -y maven && \
    sudo mvn dependency:copy-dependencies -DoutputDirectory=$SPARK_HOME/jars && \
    sudo yum erase -y maven bzip2

COPY firebird firebird
COPY notebook notebook
COPY resources resources
COPY test test
COPY .test_env .test_env
COPY test.sh test.sh
COPY setup.py .
COPY version.txt .
COPY log4j.properties $SPARK_HOME/conf/log4j.properties
COPY Makefile .
COPY README.rst .
RUN sudo chown -R lcmap:lcmap .

# Do not install the test or dev profiles in this image, control image size
RUN sudo /usr/local/bin/pip install -e .[test,dev]

RUN sudo yum erase -y gcc && sudo yum clean all
