# pull the tag from version.txt
TAG:=`cat version.txt`
OPERATOR_IMAGE:=usgseros/lcmap-firebird
DEVELOPER_IMAGE:=$(OPERATOR_IMAGE)-developer

vertest:
	@echo TAG:$(TAG)
	@echo OPERATORIMAGE:$(OPERATOR_IMAGE)
	@echo DEVELOPERIMAGE:$(DEVELOPER_IMAGE)

docker-build:
	docker build -f docker/operator/Dockerfile -t $(OPERATOR_IMAGE):$(TAG) -t $(OPERATOR_IMAGE):latest $(PWD)
	docker tag $(OPERATOR_IMAGE):$(TAG) firebird_base
	docker build -f docker/developer/Dockerfile -t $(DEVELOPER_IMAGE):$(TAG) -t $(DEVELOPER_IMAGE):latest $(PWD)

docker-push:
	docker login
	docker push $(OPERATOR_IMAGE):$(TAG)
	docker push $(OPERATOR_IMAGE):latest
	docker push $(DEVELOPER_IMAGE):$(TAG)
	docker push $(DEVELOPER_IMAGE):latest

docker-shell:
	docker run -it --entrypoint=/bin/bash usgseros/$(DEVELOPER_IMAGE):latest

deps-up:
	docker-compose -f test/resources/docker-compose.yml up

deps-down: 
	docker-compose -f test/resources/docker-compose.yml down

db-schema:
	docker cp test/resources/schema.setup.cql firebird-cassandra:/
	docker exec -u root firebird-cassandra cqlsh localhost -f schema.setup.cql

spark-lib:
	@rm -rf lib
	@mkdir lib
	wget -P lib https://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz
	gunzip lib/*gz
	tar -C lib -xvf lib/spark-2.2.0-bin-hadoop2.7.tar
	rm lib/*tar
	ln -s spark-2.2.0-bin-hadoop2.7 lib/spark
	mvn dependency:copy-dependencies -f resources/pom.xml -DoutputDirectory=lib/spark/jars

tests:
	./test.sh

clean:
	@rm -rf dist build lcmap_firebird.egg-info test/coverage lib/ derby.log spark-warehouse
	@find . -name '*.pyc' -delete
	@find . -name '__pycache__' -delete
