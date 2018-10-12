FROM usgseros/lcmap-spark:1.2.0-develop

RUN sudo yum install -y git
RUN sudo /usr/local/bin/conda install cython numpy scipy scikit-learn --yes
RUN mkdir -p ccdc
RUN sudo localedef -i en_US -f UTF-8 en_US.UTF-8
ENV LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8 PYTHONWARNINGS="ignore"
COPY .test_env test.sh setup.py version.txt requirements-dev.txt Makefile README.rst ./ccdc/
COPY ccdc ccdc/ccdc
COPY test ccdc/test
COPY resources/log4j.properties $SPARK_HOME/conf/log4j.properties

RUN sudo chown -R lcmap:lcmap . && \
    sudo /usr/local/bin/pip install -r ccdc/requirements-dev.txt && \
    sudo /usr/local/bin/pip install -e ccdc/.[test,dev] && \
    sudo sh -c 'find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf' && \
    sudo yum clean all && \
    sudo rm -rf /var/cache/yum
