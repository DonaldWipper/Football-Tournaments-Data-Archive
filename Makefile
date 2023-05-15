IMAGE_NAME := prefect-orion
IMAGE_TAG := 2.8.7
IMAGE := ${IMAGE_NAME}:${IMAGE_TAG}



.PHONY: docker
docker:
	@echo "building docker image ...";
	docker build --no-cache -f Dockerfile -t ${IMAGE} .


.PHONY: register-test-flow
register-test-flow:
	docker-compose exec prefect-server /bin/bash -c 'cd /opt/prefect/flows/test_flow && python deployment.py'

.PHONY: init-spark-paths
init-spark-paths:
	docker-compose exec prefect-server /bin/bash -c 'export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip:${PYTHONPATH}" && export PYTHONPATH="${SPARK_HOME}/python:${PYTHONPATH}"'
