.DEFAULT_GOAL := docker-build
VERSION    := `cat version.txt`
IMAGE      := usgseros/lcmap-ccdc
BRANCH     := $(or $(TRAVIS_BRANCH),`git rev-parse --abbrev-ref HEAD`)
BRANCH     := $(shell echo $(BRANCH) | tr / -)
BUILD_TAG  := $(IMAGE):build
LATEST_TAG := $(IMAGE):latest
TAG        := $(shell if [ "$(BRANCH)" = "master" ];\
                         then echo "$(IMAGE):$(VERSION)";\
                         else echo "$(IMAGE):$(VERSION)-$(BRANCH)";\
                      fi)
KEYSPACE:=`head -1 resources/schema.cql |cut -d " " -f6`

vertest:
	@echo TAG:$(TAG)
	@echo IMAGE:$(IMAGE)

sleep:
	sleep 20

docker-build:
	@docker build -t $(BUILD_TAG) --rm=true --compress $(PWD)

docker-tag:
	@docker tag $(BUILD_TAG) $(TAG)
	@docker tag $(BUILD_TAG) $(LATEST_TAG)

docker-push:
	docker login
	docker push $(TAG)
	docker push $(LATEST_TAG)

docker-shell:
	docker run -it --entrypoint=/bin/bash $(IMAGE):latest

deps-up:
	docker-compose -f resources/docker-compose.yml up

deps-up-d:
	docker-compose -f resources/docker-compose.yml up -d

deps-down: 
	docker-compose -f resources/docker-compose.yml down

db-schema:
	docker cp resources/schema.cql ccdc-cassandra:/
	docker exec -u root ccdc-cassandra cqlsh localhost -f schema.cql

db-drop:
	docker exec -u root ccdc-cassandra cqlsh localhost -e "DROP KEYSPACE $(KEYSPACE)";

spark-lib:
	@rm -rf resources/spark
	@mkdir -p resources
	@wget -P resources http://mirrors.ocf.berkeley.edu/apache/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz
	tar -C resources -zxf resources/spark-2.3.1-bin-hadoop2.7.tgz
	mv resources/spark-2.3.1-bin-hadoop2.7 resources/spark
	rm resources/spark-2.3.1-bin-hadoop2.7.tgz
	mvn dependency:copy-dependencies -f resources/pom.xml -DoutputDirectory=spark/jars
	cp resources/log4j.properties resources/spark/conf
	cp resources/spark-defaults.conf resources/spark/conf

spark-config:
	cp resources/log4j.properties resources/spark/conf
	cp resources/spark-defaults.conf resources/spark/conf

tests:  db-schema
	./test.sh

clean:
	@rm -rf dist build lcmap_ccdc.egg-info test/coverage derby.log metastore_db spark-warehouse
	@find . -name '*.pyc' -delete
	@find . -name '__pycache__' -delete
