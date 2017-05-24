# pull the tag from version.py
TAG=`cat version.py | grep '__version__ = ' | sed -e 's/__version__ = //g' | sed -e "s/'//g"`
WORKERIMAGE=lcmap-firebird:$(TAG)

vertest:
	echo $(TAG)
	echo $(WORKERIMAGE)

docker-build:
	docker build -t $(WORKERIMAGE) $(PWD)

docker-shell:
	docker run -it --entrypoint=/bin/bash usgseros/$(WORKERIMAGE)

docker-deps-up:
	docker-compose -f resources/docker-compose.yml up -d

docker-deps-up-nodaemon:
	docker-compose -f resources/docker-compose.yml up

docker-db-test-schema:
	docker cp test/resources/test.schema.setup.cql ${TEST_CASS_CONTAINER}:/
	docker exec -u root ${TEST_CASS_CONTAINER} cqlsh localhost -f test.schema.setup.cql

docker-deps-down:
	docker-compose -f resources/docker-compose.yml down

clean:
	@rm -rf dist build lcmap_firebird.egg-info test/coverage
	@find . -name '*.pyc' -delete
	@find . -name '__pycache__' -delete


