IMAGE_NAME := prefect-orion
IMAGE_TAG := 2.10.7
IMAGE := ${IMAGE_NAME}:${IMAGE_TAG}



.PHONY: docker
docker:
	@echo "building docker image ...";
	docker build --no-cache -f Dockerfile -t ${IMAGE} .


.PHONY: register-test-flow
register-test-flow:
	docker-compose exec prefect-server /bin/bash -c 'cd /opt/prefect/flows/test_flow && python deployment.py'
