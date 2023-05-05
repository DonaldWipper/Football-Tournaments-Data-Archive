IMAGE_NAME := pyspark-aws
IMAGE_TAG := 10
IMAGE := ${IMAGE_NAME}:${IMAGE_TAG}



.PHONY: docker
docker:
	@echo "building docker image ...";
	docker build --no-cache -f Dockerfile --rm -t ${IMAGE} .
