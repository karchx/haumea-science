ifneq (,$(wildcard ./.env))
	include .env
	export
endif

ENV ?= aries
PIPELINE ?= aries
VERSION ?= 0.1.0
NAMESPACE ?= de
SBT_DIR ?= aries
MINIO_ENDPOINT ?= http://localhost:9000

JAR_NAME := spark-$(ENV)-assembly-$(VERSION).jar
JAR_PATH := $(SBT_DIR)/target/scala-2.12/$(JAR_NAME)
S3_BUCKET := s3://pipelines/haumea
MANIFEST_PATH := $(ENV)/manifests/$(PIPELINE).yaml
APP_NAME := $(PIPELINE)-spark-app

.PHONY: clean build upload deploy redeploy logs

build:
	cd $(SBT_DIR) && sbt assembly

upload:
	aws s3 cp $(JAR_PATH) $(S3_BUCKET)/$(JAR_NAME) --endpoint-url $(MINIO_ENDPOINT)

deploy: 
	kubectl apply -f $(MANIFEST_PATH) -n $(NAMESPACE)

redeploy:
	kubectl delete -f $(MANIFEST_PATH) -n $(NAMESPACE)
	kubectl apply -f $(MANIFEST_PATH) -n $(NAMESPACE)

logs:
	kubectl logs -l app=$(APP_NAME) -n $(NAMESPACE) --tail

