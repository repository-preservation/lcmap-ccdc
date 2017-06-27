#  Base this image off of Mesosphere instead of USGS 
FROM usgseros/mesos-spark:latest
MAINTAINER USGS LCMAP http://eros.usgs.gov

# Question of whether Firebird actually needs to be installed in this container
# If its sent as a Spark job dependency (on the SparkContext) then
# updated code can be dynamically sent into Spark without rebuilding
# the Docker image.  Probably best to just install all of Firebirds dependencies
# into the Docker image instead.  Consider dynamically sending in pyccd as
# as well so updated code can be run.
# To enable this, Firebird and pyccd will both need to have .eggs or .zips
# generated and then supplied to the SparkContext.
RUN mkdir /app
WORKDIR /app
COPY firebird /app/firebird
COPY README.md /app
COPY resources /app/resources
COPY setup.py /app
COPY version.py /app
RUN mkdir -p /app/test/resources/data
COPY test/resources/data/chip-specs /app/test/resources/data/chip-specs
COPY test/resources/data/chips /app/test/resources/data/chips
COPY test/fixtures.py /app/test
COPY test/__init__.py /app/test
COPY test/shared.py /app/test
#COPY test /app/test
COPY notebooks /app/notebooks

# Install dependencies
RUN apt-get update && apt-get -y install wget maven --fix-missing

RUN mvn -DgroupId=com.datastax.spark -DartifactId=spark-cassandra-connector_2.11 -Dversion=2.0.2 dependency:get

RUN wget -O Miniconda3-latest.sh https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh; chmod 755 Miniconda3-latest.sh;
RUN ./Miniconda3-latest.sh -b;
ENV PATH="/root/miniconda3/bin:${PATH}"
RUN conda config --add channels conda-forge;
RUN conda install python=3.5 numpy pandas jupyter --yes

RUN pip install -e .
RUN pip install -e .[test]
