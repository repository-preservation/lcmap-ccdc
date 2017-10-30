FROM centos:7.3.1611

LABEL maintainer="USGS EROS LCMAP http://eros.usgs.gov http://github.com/usgs-eros/lcmap-firebird"
LABEL description="CentOS based Spark-Mesos image for LCMAP"
LABEL org.apache.mesos.version=1.4.0
LABEL org.apache.spark.version=2.2.0
LABEL net.java.openjdk.version=1.8.0
LABEL org.python.version=3.6
LABEL org.centos=7.3.1611

EXPOSE 8081 4040 8888

RUN yum update -y && \
    yum install -y sudo gcc bzip2 java-1.8.0-openjdk-devel.x86_64 && \
    yum install -y http://repos.mesosphere.io/el/7/noarch/RPMS/mesosphere-el-repo-7-3.noarch.rpm && \
    yum install -y mesos && \
    yum -y downgrade mesos-1.4.0

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
ENV PYSPARK_PYTHON=$HOME/miniconda3/bin/python3
ENV MESOS_NATIVE_JAVA_LIBRARY=/usr/lib/libmesos.so
ENV PATH=$HOME/miniconda3/bin:$SPARK_HOME/bin:${PATH}
ENV PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$SPARK_HOME/python/lib/pyspark.zip
ENV LC_ALL=en_US.UTF-8
ENV LANG=en_US.UTF-8

RUN cd /opt && \
    sudo curl https://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz -o spark.tgz && \
    sudo tar -zxf spark.tgz && \
    sudo rm -f spark.tgz && \
    sudo ln -s spark-* spark

RUN curl https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o mc.sh && \
    chmod 755 mc.sh && \
    ./mc.sh -b && \
    rm -rf mc.sh && \
    conda config --add channels conda-forge && \
    conda install python=3.6.2 pyzmq cython gdal --yes

COPY firebird firebird
COPY notebook notebook
COPY resources resources
COPY test test
COPY Makefile .
COPY pom.xml .
COPY README.rst .
COPY setup.py .
COPY version.txt .
COPY log4j.properties $SPARK_HOME/conf/log4j.properties
RUN sudo chown -R lcmap:lcmap .

RUN pip install -e .[test,dev]

RUN cd $HOME && \
    sudo yum install -y maven && \
    sudo mvn dependency:copy-dependencies -DoutputDirectory=$SPARK_HOME/jars && \
    sudo yum erase -y maven && \
    sudo yum clean all && \
    conda clean -all
