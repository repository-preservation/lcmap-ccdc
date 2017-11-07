FROM firebird_base
LABEL mode="developer"

RUN conda install pyzmq --yes

# Install the test and dev profiles
RUN pip install -e .[test,dev]
