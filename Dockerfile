FROM centos:7.3.1611

LABEL maintainer="USGS EROS LCMAP http://eros.usgs.gov http://github.com/usgs-eros/lcmap-firebird" \
      description="CentOS based Spark-Mesos image for LCMAP" \
      org.apache.mesos.version=1.4.0 \
      org.apache.spark.version=2.2.0 \
      net.java.openjdk.version=1.8.0 \
      org.python.version=3.6 \
      org.centos=7.3.1611

EXPOSE 8081 4040 8888

RUN yum update -y && \
    yum install -y sudo java-1.8.0-openjdk-devel.x86_64 && \
    yum install -y http://repos.mesosphere.io/el/7/noarch/RPMS/mesosphere-el-repo-7-3.noarch.rpm && \
    yum install -y mesos && \
    yum -y downgrade mesos-1.4.0 && \
    sudo yum clean all && \
    sudo rm -rf /var/cache/yum && \
    localedef -i en_US -f UTF-8 en_US.UTF-8

ENV HOME=/home/lcmap \
    USER=lcmap \
    SPARK_HOME=/opt/spark \
    SPARK_NO_DAEMONIZE=true \
    PYSPARK_PYTHON=python3 \
    MESOS_NATIVE_JAVA_LIBRARY=/usr/lib/libmesos.so \    
    LC_ALL=en_US.UTF-8 \
    LANG=en_US.UTF-8

ENV PATH=$SPARK_HOME/bin:${PATH}:$HOME/miniconda3/bin \
    PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$SPARK_HOME/python/lib/pyspark.zip
    
##########################################################################
# Add a user to run as inside the container.  This will prevent accidental
# foo while mounting volumes.  To enable access between external and
# internal users on mounted volume files, set 'other' perms appropriately.
##########################################################################
RUN adduser -ms /bin/bash lcmap && \
    echo "lcmap ALL=(root) NOPASSWD:ALL" > /etc/sudoers.d/lcmap && \
    chmod 0440 /etc/sudoers.d/lcmap

USER $USER
WORKDIR $HOME
##########################################################################

# Install Spark
RUN cd /opt && \
    sudo curl https://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz -o spark.tgz && \
    sudo tar -zxf spark.tgz && \
    sudo rm -f spark.tgz && \
    sudo ln -s spark-* spark

# Install miniconda.
# Install numpy from default repo for mkl based implementation.
RUN sudo yum install -y bzip2 && \
    curl https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o mc.sh && \
    chmod 755 mc.sh && \
    ./mc.sh -b && \
    rm -rf mc.sh && \
    sudo yum erase -y bzip2 && \
    sudo yum clean all && \
    conda install python=3.6 pip numpy cython scipy scikit-learn=0.18 --yes

# Copy firebird project artifacts into image
RUN mkdir -p firebird
COPY pom.xml .test_env test.sh setup.py version.txt Makefile README.rst ./firebird/
COPY firebird firebird/firebird
COPY notebook firebird/notebook
COPY test firebird/test
COPY log4j.properties $SPARK_HOME/conf/log4j.properties

# Install spark-cassandra-connector
RUN cd $HOME/firebird && \
    sudo yum install -y maven  && \
    sudo mvn dependency:copy-dependencies -DoutputDirectory=$SPARK_HOME/jars && \
    sudo yum erase -y maven && \
    sudo yum clean all && \
    sudo rm -rf /var/cache/yum && \
    sudo rm -rf /root/.cache /root/.m2

# Do not install the test or dev profiles in this image, control image size
#RUN sudo /usr/local/bin/pip install -e .[test,dev]
RUN sudo chown -R lcmap:lcmap . && \
    sudo yum -y install gcc Cython && \
    sudo $HOME/miniconda3/bin/pip install -e firebird/.[test,dev] && \
    sudo sh -c 'find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf' && \
    sudo yum erase -y gcc && \
    sudo yum clean all && \
    sudo rm -rf /var/cache/yum
