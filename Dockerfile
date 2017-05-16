FROM usgseros/mesos-spark:latest                                                                                                                            
MAINTAINER USGS LCMAP http://eros.usgs.govRUN apt-get update && apt-get install -y wget --fix-missing
RUN mkdir /app
WORKDIR /app
COPY firebird /app/firebird
COPY README.md /app
COPY resources /app/resources
COPY setup.py /app
COPY version.py /app
RUN mkdir -p /app/test/resources/data
COPY test/resources/data/chip-specs app/test/resources/data/chip-specs
COPY test/resources/data/chips app/test/resources/data/chips
COPY test/fixtures.py app/test
COPY test/__init__.py app/test
COPY test/shared.py app/test
#COPY test /app/test
COPY notebooks /app/notebooks
#preposition numpy with conda to avoid compiling from scratch
RUN apt-get update && apt-get -y install wget
RUN wget -O Miniconda3-latest.sh https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh; chmod 755 Miniconda3-latest.sh;
RUN ./Miniconda3-latest.sh -b;
ENV PATH="/root/miniconda3/bin:${PATH}"
RUN conda config --add channels conda-forge;
RUN conda install python=3.5 numpy pandas jupyter --yes
RUN pip install -e .
RUN pip install -e .[test]
