FROM mesosphere/spark:1.1.0-2.1.1-hadoop-2.7
MAINTAINER USGS LCMAP http://eros.usgs.gov

RUN apt-get update && apt-get install -y wget make maven --fix-missing

# preposition numpy with conda to avoid compiling from scratch
RUN wget -O Miniconda3-latest.sh https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh; chmod 755 Miniconda3-latest.sh;
RUN ./Miniconda3-latest.sh -b;
ENV PATH=/root/miniconda3/bin:${PATH}
RUN conda config --add channels conda-forge;
RUN conda install python=3.6 numpy scipy pandas jupyter --yes

ENV PYSPARK_PYTHON /root/miniconda3/bin/python3
ENV SPARK_NO_DAEMONIZE true
ENV MESOS_NATIVE_JAVA_LIBRARY /usr/lib/libmesos.so
ENV SPARK_HOME /opt/spark/dist
ENV PATH $SPARK_HOME/bin:$PATH
ENV PYTHONPATH $PYTHONPATH:$SPARK_HOME/python/
ENV PYTHONPATH $PYTHONPATH:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip
ENV PYTHONPATH $PYTHONPATH:$SPARK_HOME/python/lib/pyspark.zip

# these are for click to work with Python3
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

RUN mkdir -p /algorithms
WORKDIR /algorithms
RUN wget -O pyccd-v2017.06.20.zip https://github.com/USGS-EROS/lcmap-pyccd/archive/v2017.06.20.zip

EXPOSE 8081
EXPOSE 4040
EXPOSE 8888

RUN mkdir /app
WORKDIR /app

COPY firebird /app/firebird
COPY notebooks /app/notebooks
COPY resources /app/resources
COPY test /app/test
COPY .test_env .
COPY Makefile .
COPY pom.xml .
COPY README.md .
COPY setup.py .
COPY version.py .

# Install Cassandra Spark Connector
RUN mvn dependency:copy-dependencies -DoutputDirectory=$SPARK_HOME/jars

RUN make clean
RUN pip install -e .[test]
RUN pip install -e .[dev]
