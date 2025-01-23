# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

##############################################  HELP  ##################################################################

define HELP_BODY
To run unit tests, run:
	make test

usage: make [target] [options]
TARGETS
help
	Description:
		Prints this help message

init
	Requirements:
		- Python 3.7-3.10
		- Golang 1.21

	Description:
		Installs grpc-tools with pip and builds the proto files for the serving and metadata connections for the
		Python client and Golang libraries. It then builds and installs the Python SDK and CLI.


test
	Requirements:
		- Python 3.7-3.10
		- Golang 1.21
	Description:
		Runs 'init' then runs the Python and Golang Unit tests

test_offline
	Requirements:
		- Golang 1.21

	Description:
		Runs offline store integration tests. Requires credentials if not using the memory provider

	Options:
		- provider (memory | postgres | snowflake | redshift | bigquery | spark | clickhouse )
			Description:
				Runs specified provider. If left blank or not included, runs all providers
			Usage:
				make test_offline provider=memory

test_online
	Requirements:
		- Golang 1.21

	Description:
		Runs online store integration tests. Requires credentials if not using the memory or redis_mock provider

	Options:
		- provider (memory | redis_mock | redis_insecure | redis_secure | cassandra | firestore | dynamo )
			Description:
				Runs specified provider. If left blank or not included, runs all providers
			Usage:
				make test_online provider=memory


test_go_unit
	Requirements:
		- Golang 1.21

	Description:
		Runs golang unit tests

test_metadata
	Requirements:
		- Golang 1.21
		- ETCD installed and added to path (https://etcd.io/docs/v3.4/install/)

	Description:
		Runs metadata tests

	Flags:
		- ETCD_UNSUPPORTED_ARCH
			Description:
				This flag must be set to run on M1/M2 Macs
			Usage:
				make test_metadata flags=ETCD_UNSUPPORTED_ARCH=arm64


test_helpers
	Requirements:
		- Golang 1.21

	Description:
		Runs helper tests

test_serving
	Requirements:
		- Golang 1.21

	Description:
		Runs serving tests

test_runner
	Requirements:
		- Golang 1.21
		- ETCD installed and added to path (https://etcd.io/docs/v3.4/install/)

	Description:
		Runs coordinator runner tests

	Flags:
		- ETCD_UNSUPPORTED_ARCH
			Description:
				This flag must be set to run on M1/M2 Macs
			Usage:
				make test_metadata flags=ETCD_UNSUPPORTED_ARCH=arm64

test_api
	Requirements:
		- Golang 1.21
		- Python3.6-3.10

	Description:
		Starts an API Server instance and checks that the serving and metadata clients can connect

test_typesense
	Requirements:
		- Golang 1.21
		- Docker

	Description:
		Starts a typesense instance and tests the typesense package

test_coordinator
	Requirements:
		- Golang 1.21
		- ETCD installed and added to path (https://etcd.io/docs/v3.4/install/)
		- Docker

	Description:
		Starts ETCD, Postgres, and Redis to test the coordinator

	Flags:
		- ETCD_UNSUPPORTED_ARCH
			Description:
				This flag must be set to run on M1/M2 Macs
			Usage:
				make test_metadata flags=ETCD_UNSUPPORTED_ARCH=arm64

test_filestore
	Requirements:
		- Golang 1.21

	Description:
		Runs golang unit tests

get_secrets
	Requirements:
		- python 3.7-3.10
		- AWS Credentials with access to Secrets Manager
		- boto3 > 1.34 (needs `SecretsManager.Client.batch_get_secret_value`)

	Description:
		Gets secrets from AWS Secrets Manager and writes them to .env file in root of repository

update_secrets
	Requirements:
		- python 3.7-3.10
		- AWS Credentials with access to Secrets Manager
		- boto3 > 1.34 (needs `SecretsManager.Client.batch_get_secret_value`)

	Description:
		Updates secrets from AWS Secrets Manager and writes them to .env file in root of repository


setup_e2e_core:
	Requirements:
		- python 3.7-3.10
		- Secrets in .env file
		- activated virtual environment
		- docker running

	Description:
		Installs featureform client, packages for pytest, as well as a redis docker container


teardown_e2e_core:
	Requirements:
		- docker running with featureform_redis container

	Description:
		Removes the featureform_redis running container

setup_e2e_standalone:
	Requirements:
		- Golang 1.21

	Description:
		Runs the standalone featureform script in the background and logs to featureform.log

teardown_e2e_standalone:
	Requirements:
		- standalone featureform script running

	Description:
		Teardown the standalone featureform script

setup_e2e_docker:
	Requirements:
		- Docker running

	Description:
		Builds the featureform single docker image and deploys it to a container, featureform_e2e

teardown_e2e_docker:
	Requirements:
		- Featureform container, featureform_e2e, running

	Description:
		Teardown the featureform_e2e docker container

test_e2e_pytest:
	Requirements:
		- python 3.7-3.10
		- Secrets in .env file
		- activated virtual environment
		- featureform running either in docker or standalone script

	Description:
		Runs the pytest tests for the end-to-end tests

test_e2e_behave:
	Requirements:
		- python 3.7-3.10
		- Secrets in .env file
		- activated virtual environment
		- featureform running either in docker or standalone script

	Description:
		Runs the behave tests for the end-to-end tests

test_e2e:
	Requirements:
		- python 3.7-3.10
		- Secrets in .env file
		- activated virtual environment
		- featureform running either in docker or standalone script

	Description:
		Runs the behave & pytests tests for the end-to-end tests


setup_all_docker:
	Requirements:
		- python 3.7-3.10
		- activated virtual environment
		- AWS Credentials with access to Secrets Manager
		- Docker running

	Description:
		Creates a new environment with client installed as well as all the setup needed to run any test using Docker

teardown_all_docker:
	Requirements:
		- setup_all_docker

	Description:
		Tears down the environment created by setup_all_docker

setup_all_standalone:
	Requirements:
		- python 3.7-3.10
		- activated virtual environment
		- AWS Credentials with access to Secrets Manager
		- Docker running

	Description:
		Creates a new environment with client installed as well as all the setup needed to run any test using Docker and Featureform standalone script

teardown_all_standalone:
	Requirements:
		- setup_all_standalone

	Description:
		Tears down the environment created by setup_all_standalone

endef
export HELP_BODY

help:
	@echo "$$HELP_BODY"  |  less

##############################################  UNIT TESTS #############################################################

init: update_python

test: init pytest test_go_unit

##############################################  SETUP ##################################################################

gen_grpc:						## Generates GRPC Dependencies
	python3 -m pip install grpcio-tools

	-mkdir client/src/featureform/proto/
	cp metadata/proto/metadata.proto client/src/featureform/proto/metadata.proto
	cp proto/serving.proto client/src/featureform/proto/serving.proto

	protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     ./proto/serving.proto
	python3 -m grpc_tools.protoc -I ./client/src --python_out=./client/src  --mypy_out=./client/src --grpc_python_out=./client/src/ ./client/src/featureform/proto/serving.proto

	protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     ./metadata/proto/metadata.proto
	python3 -m grpc_tools.protoc -I ./client/src --python_out=./client/src/ --mypy_out=./client/src --grpc_python_out=./client/src/ ./client/src/featureform/proto/metadata.proto

	protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     ./scheduling/proto/scheduling.proto

update_python: gen_grpc 				## Updates the python package locally
	pip3 install pytest
	pip3 install build
	pip3 uninstall featureform  -y
	-rm -r client/dist/*
	python3 -m build ./client/
	pip3 install client/dist/*.whl
	pip3 install -r provider/scripts/spark/requirements.txt

etcdctl: 						## Installs ETCDCTL. Required for reset_e2e
	-git clone -b v3.4.16 https://github.com/etcd-io/etcd.git
	cd etcd && ./build
	export PATH=$PATH:"`pwd`/etcd/bin"
	etcdctl version

credentials:
	-mkdir ~/credentials
	aws secretsmanager get-secret-value --secret-id bigquery.json --region us-east-1 |   jq -r '.SecretString' > ~/credentials/bigquery.json
	aws secretsmanager get-secret-value --secret-id firebase.json --region us-east-1 |   jq -r '.SecretString' > ~/credentials/firebase.json
	aws secretsmanager get-secret-value --secret-id .env --region us-east-1 |   jq -r '.SecretString' |   jq -r "to_entries|map(\"\(.key)=\\\"\(.value|tostring)\\\"\")|.[]" > .env

start_postgres:
	-docker kill postgres
	-docker rm postgres
	docker run -d -p 5432:5432 --name postgres -e POSTGRES_PASSWORD=password postgres

stop_postgres:
	docker kill postgres
	docker rm postgres

##############################################  PYTHON TESTS ###########################################################
pytest:
	python -m pytest --ignore tests/end_to_end/pytest
	-rm -r .featureform
	-rm -f transactions.csv

pytest_coverage:
	-rm -r .featureform
	curl -C - https://featureform-demo-files.s3.amazonaws.com/transactions_short.csv -o transactions.csv
	python -m pytest -v -s -m 'local' --cov=client/src/featureform client/tests/ --cov-report=xml --ignore tests/end_to_end/pytest
	-rm -r .featureform
	-rm -f transactions.csv

jupyter: update_python
	pip3 install jupyter nbconvert matplotlib pandas scikit-learn requests
	jupyter nbconvert --to notebook --execute notebooks/Fraud_Detection_Example.ipynb

test_pyspark:
	@echo "Requires Java to be installed"
	pytest -v -s --cov=offline_store_spark_runner provider/scripts/spark/tests/ --cov-report term-missing

test_pandas:
	pytest -v -s --cov=offline_store_pandas_runner provider/scripts/k8s/tests/ --cov-report term-missing


##############################################  GO TESTS ###############################################################
test_offline: gen_grpc 					## Run offline tests. Run with `make test_offline provider=(memory | postgres | snowflake | redshift | spark | clickhouse)`
	@echo "These tests require a .env file. Please Check .env-template for possible variables"
	-mkdir coverage
	go test -v -parallel 1000 -timeout 60m -coverpkg=./... -coverprofile coverage/cover.out.tmp ./provider --tags=offline,filepath --provider=$(provider)

test_offline_spark: gen_grpc 					## Run spark tests.
	@echo "These tests require a .env file. Please Check .env-template for possible variables"
	-mkdir coverage
	go test -v -parallel 1000 -timeout 60m -coverpkg=./... -coverprofile coverage/cover.out.tmp ./provider --tags=spark

test_offline_k8s:  					## Run k8s tests.
	@echo "These tests require a .env file. Please Check .env-template for possible variables"
	-mkdir coverage
	go test -v -parallel 1000 -timeout 60m -coverpkg=./... -coverprofile coverage/cover.out.tmp ./provider/... --tags=k8s

test_filestore:
	@echo "These tests require a .env file. Please Check .env-template for possible variables"
	-mkdir coverage
	go test -v -timeout 60m -coverpkg=./... -coverprofile coverage/cover.out.tmp ./provider/... --tags=filestore


test_online: gen_grpc 					## Run offline tests. Run with `make test_online provider=(memory | redis_mock | redis_insecure | redis_secure | cassandra | firestore | dynamo )`
	@echo "These tests require a .env file. Please Check .env-template for possible variables"
	-mkdir coverage
	go test -v -coverpkg=./... -coverprofile coverage/cover.out.tmp ./provider --tags=online,provider --provider=$(provider)

test_go_unit:
	-mkdir coverage
	go test ./... -tags=*,offline,provider --short   -coverprofile coverage/cover.out.tmp

test_go_integration:
	-mkdir coverage
	docker-compose -f integration-tests-docker-compose.yml up -d
	go test ./... -tags=online, provider, filestore, spark, filepath, health, importable_online -coverprofile coverage/cover.out.tmp
	docker-compose -f integration-tests-docker-compose.yml down

test_metadata:							## Requires ETCD to be installed and added to path
	-mkdir coverage
	$(flags) etcd &
	while ! echo exit | nc localhost 2379; do sleep 1; done
	go test -coverpkg=./... -coverprofile coverage/cover.out.tmp ./metadata/

test_helpers:
	-mkdir coverage
	go test -v -coverpkg=./... -coverprofile coverage/cover.out.tmp ./helpers/...

test_serving:
	-mkdir coverage
	go test -v -coverpkg=./... -coverprofile coverage/cover.out.tmp ./serving/...

test_runner:							## Requires ETCD to be installed and added to path
	-mkdir coverage
	$(flags) etcd &
	while ! echo exit | nc localhost 2379; do sleep 1; done
	go test -v -coverpkg=./... -coverprofile coverage/cover.out.tmp ./runner/...

test_api: update_python
	pip3 install -U pip
	pip3 install python-dotenv pytest
	go run api/main.go & echo $$! > server.PID;
	while ! echo exit | nc localhost 7878; do sleep 1; done
	pytest client/tests/connection_test.py
	kill -9 `cat server.PID`

test_typesense:
	-docker kill typesense
	-docker rm typesense
	-mkdir coverage
	-mkdir /tmp/typesense-data
	docker run -d --name typesense -p 8108:8108 -v/tmp/typesense-data:/data typesense/typesense:0.23.1 --data-dir /data --api-key=xyz --enable-cors
	go test -v -coverpkg=./... -coverprofile ./coverage/cover.out.tmp ./metadata/search/...
	docker kill typesense
	docker rm typesense

test_coordinator: cleanup_coordinator
	-mkdir coverage
	docker run -d --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=password postgres
	docker run -d --name redis -p 6379:6379 redis
	$(flags) etcd &
	while ! echo exit | nc localhost 2379; do sleep 1; done
	while ! echo exit | nc localhost 5432; do sleep 1; done
	while ! echo exit | nc localhost 6379; do sleep 1; done
	go test -v -coverpkg=./... -coverprofile coverage/cover.out.tmp ./coordinator/...
	$(MAKE) cleanup_coordinator

cleanup_coordinator:
	-docker kill postgres
	-docker rm postgres
	-docker kill redis
	-docker rm redis

test_healthchecks: ## Run health check tests. Run with `make test_healthchecks provider=(redis | postgres | snowflake | dynamo | spark | clickhouse)`
	@echo "These tests require a .env file. Please Check .env-template for possible variables"
	-mkdir coverage
	go test -v -coverpkg=./... -coverprofile coverage/cover.out.tmp ./health --tags=health --provider=$(provider)


test_importable_online: ## Run importable online table tests. Run with `make test_importable_online provider=( dynamo )`
	@echo "These tests require a .env file. Please Check .env-template for possible variables"
	-mkdir coverage
	go test -v -coverpkg=./... -coverprofile coverage/cover.out.tmp ./provider --tags=importable_online --provider=$(provider)


#############################################  SECRETS ################################################################
get_secrets:
	python scripts/secret_manager.py

update_secrets:
	python scripts/secret_manager.py --update


#########################################  End To End Tests ###########################################################
setup_e2e_core:
	pip install -e client/
	pip install -r pytest-requirements.txt
	pip install -r tests/end_to_end/requirements.txt

	docker run --name featureform_redis -d -p 6379:6379 redis
	docker run --name featureform_postgres -d -p 5432:5432 -e POSTGRES_PASSWORD=password postgres

teardown_e2e_core:
	-docker kill featureform_redis
	-docker rm featureform_redis

	-docker kill featureform_postgres
	-docker rm featureform_postgres

setup_e2e_standalone: setup_e2e_core
	nohup go run main/main.go > featureform.log 2>&1 &

teardown_e2e_standalone: teardown_e2e_core
	kill -9 $$(ps aux | grep 'exe/main' | grep -v grep | awk '{print $$2}')

# Target for setting up e2e Docker environment
setup_e2e_docker: setup_e2e_core
	docker build . -t featureform:e2e_test

	@ARCH=$$(uname -m); \
	if [ "$$ARCH" = "arm64" ]; then \
		echo "Running on Apple Silicon (ARM64 architecture)."; \
		docker run -d --name featureform_e2e -p 7878:7878 -p 80:80 \
			-e ETCD_UNSUPPORTED_ARCH="arm64" \
			-e FF_AUTH_PROVIDER="disabled" \
			-e NEXT_PUBLIC_FF_AUTH_PROVIDER="disabled" \
			featureform:e2e_test; \
	else \
		echo "Running on Intel (x86_64 architecture)."; \
		docker run -d --name featureform_e2e -p 7878:7878 -p 80:80 \
			-e FF_AUTH_PROVIDER="disabled" \
			-e NEXT_PUBLIC_FF_AUTH_PROVIDER="disabled" \
			featureform:e2e_test; \
	fi

	@echo ""
	@echo "Featureform is now running in a Docker container. To run the end-to-end tests, you will need to set the following environment variables:"
	@echo "export REDIS_HOST=host.docker.internal"
	@echo "export POSTGRES_HOST=host.docker.internal"

# Target for tearing down e2e Docker environment
teardown_e2e_docker: teardown_e2e_core
	-docker kill featureform_e2e
	-docker rm featureform_e2e

test_e2e_pytest:
	pytest -vv -s -n 5 tests/end_to_end/pytest

test_e2e_behave: 
	echo "Starting end to end tests"
	pytest -vv -s tests/end_to_end/pytest

	export FF_TIMESTAMP_VARIANT="false" && export FF_GET_EQUIVALENT_VARIANTS: "false"
	behavex -t '~@wip' -t '~@long' -t '~@av' --no-capture --no-logcapture --no-capture-stderr --parallel-processes 5 --parallel-scheme scenario

	export FF_TIMESTAMP_VARIANT="true" && export FF_GET_EQUIVALENT_VARIANTS: "true"
	behavex -t '~@wip' -t '~@long' -t '@av' --no-capture --no-logcapture --no-capture-stderr --parallel-processes 5 --parallel-scheme scenario

test_e2e: test_e2e_pytest test_e2e_behave

#########################################  Setup Everything  ###########################################################
setup_all_docker: init update_secrets setup_e2e_docker

teardown_all_docker: teardown_e2e_docker

setup_all_standalone: init update_secrets setup_e2e_standalone

teardown_all_standalone: teardown_e2e_standalone


##############################################  MINIKUBE ###############################################################
MINIKUBE_VERSION ?= v1.23.12

minikube_build_images: gen_grpc   ## Build Docker images for Minikube
	minikube image build -f ./api/Dockerfile -t local/api-server:stable . & \
	minikube image build -f ./dashboard/Dockerfile -t local/dashboard:stable . & \
	minikube image build -f ./coordinator/Dockerfile --build-opt=build-arg=TESTING=True -t local/coordinator:stable . & \
	minikube image build -f ./metadata/Dockerfile -t local/metadata:stable . & \
	minikube image build -f ./metadata/dashboard/Dockerfile -t local/metadata-dashboard:stable . & \
	minikube image build -f ./serving/Dockerfile -t local/serving:stable . & \
	minikube image build -f ./runner/Dockerfile --build-opt=build-arg=TESTING=True -t local/worker:stable . & \
	minikube image build -f ./provider/scripts/k8s/Dockerfile -t local/k8s_runner:stable . & \
	minikube image build -f ./provider/scripts/k8s/Dockerfile.scikit -t local/k8s_runner:stable-scikit . & \
	minikube image build -f ./search_loader/Dockerfile -t local/search-loader:stable . & \
	minikube image build -f ./streamer/Dockerfile -t local/iceberg-streamer:stable . & \
	minikube image build -f ./streamer_proxy/Dockerfile -t local/iceberg-proxy:stable . & \
	minikube image build -f ./db/Dockerfile -t local/db-migration-up:stable . & \
	wait; \
	echo "Build Complete"


minikube_start:	##Starts Minikube
	minikube start --kubernetes-version=$(MINIKUBE_VERSION)

minikube_delete: ##Deletes Minikube
	minikube delete

minikube_reset: minikube_delete minikube_start	##Resets Minikube
	
minikube_install_featureform: minikube_start minikube_build_images	## Configures Featureform on Minikube
	helm repo add jetstack https://charts.jetstack.io
	helm repo update
	helm install certmgr jetstack/cert-manager \
		--set installCRDs=true \
		--version v1.8.0 \
		--namespace cert-manager \
		--create-namespace
	helm install quickstart ./charts/quickstart --wait --timeout=15s
	helm install featureform ./charts/featureform \
		--set repository=local \
		--set pullPolicy=Never \
		--set versionOverride=stable
	kubectl get secret featureform-ca-secret -o=custom-columns=':.data.tls\.crt' | base64 -d > tls.crt