# pull the tag from version.py
TAG=`cat version.py | grep '__version__ = ' | sed -e 's/__version__ = //g' | sed -e "s/'//g"`
WORKERIMAGE:=lcmap-firebird:$(TAG)

vertest:
	@echo TAG:$(TAG)
	@echo WORKERIMAGE:$(WORKERIMAGE)

docker-build:
	docker build -t $(WORKERIMAGE) $(PWD)

docker-shell:
	docker run -it --entrypoint=/bin/bash usgseros/$(WORKERIMAGE)

docker-deps-up:
	docker-compose -f test/resources/docker-compose.yml up -d

docker-deps-up-nodaemon:
	docker-compose -f test/resources/docker-compose.yml up

docker-db-test-schema:
	docker cp test/resources/test.schema.setup.cql worker-cassandra:/
	docker exec -u root worker-cassandra cqlsh localhost -f test.schema.setup.cql

docker-deps-down:
	docker-compose -f test/resources/docker-compose.yml down

spark-lib:
	@rm -rf lib
	@mkdir lib
	# wget -P lib https://d3kbcqa49mib13.cloudfront.net/spark-2.1.1-bin-hadoop2.7.tgz
	wget -P lib https://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz
	gunzip lib/*gz
	tar -C lib -xvf lib/spark-2.2.0-bin-hadoop2.7.tar
	rm lib/*tar
	ln -s spark-2.2.0-bin-hadoop2.7 lib/spark
	mvn dependency:copy-dependencies -DoutputDirectory=lib/spark/jars

tests:
	./test.sh

clean:
	@rm -rf dist build lcmap_firebird.egg-info test/coverage lib/ derby.log spark-warehouse
	@find . -name '*.pyc' -delete
	@find . -name '__pycache__' -delete
