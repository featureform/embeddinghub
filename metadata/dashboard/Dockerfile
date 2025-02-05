#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

# docker buildx build -f ./metadata/dashboard/Dockerfile . -t featureformcom/metadata-dashboard:latest -o type=image --platform=linux/arm64,linux/amd64 --push
FROM golang:1.22-alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./

COPY ./metadata/proto/metadata.proto ./metadata/proto/metadata.proto
COPY ./scheduling/proto/scheduling.proto ./scheduling/proto/scheduling.proto
COPY ./proto/ ./proto/
RUN apk update && apk add protobuf-dev && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest && go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
ENV PATH /go/bin:$PATH
RUN protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./metadata/proto/metadata.proto \
    && protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     ./proto/serving.proto \
    && protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     ./scheduling/proto/scheduling.proto

COPY ./fferr ./fferr
COPY ./ffsync ./ffsync
COPY ./scheduling ./scheduling
COPY ./kubernetes ./kubernetes
COPY ./schema ./schema
COPY ./storage ./storage
COPY ./lib/ ./lib/
COPY ./filestore/ ./filestore/
COPY ./metadata/*.go ./metadata/
COPY ./helpers/ ./helpers/
COPY ./integrations/ ./integrations/
COPY ./lib/ ./lib/
COPY ./metrics/ ./metrics/
COPY ./logging/ ./logging/
COPY ./serving/ ./serving/
COPY ./kubernetes/ ./kubernetves/
COPY ./types/ ./types/
COPY ./provider/ ./provider/
COPY ./metadata/dashboard/ ./metadata/dashboard/
COPY ./metadata/search/ ./metadata/search/
COPY ./metadata/equivalence/ ./metadata/equivalence/
COPY ./metadata/common/ ./metadata/common/
COPY ./provider/provider_config/ ./provider/provider_config/
COPY ./provider/provider_type/ ./provider/provider_type/
COPY ./config/ ./config/

RUN go build ./metadata/dashboard/main/main.go

FROM alpine

COPY --from=0 ./app/main ./main

ARG SPARK_FILEPATH=/app/provider/scripts/spark/offline_store_spark_runner.py
ARG SPARK_PYTHON_PACKAGES=/app/provider/scripts/spark/python_packages.sh
ARG SPARK_REQUIREMENTS=/app/provider/scripts/spark/requirements.txt
ARG MATERIALIZE_NO_TIMESTAMP_QUERY_PATH=/app/provider/queries/materialize_no_ts.sql
ARG MATERIALIZE_TIMESTAMP_QUERY_PATH=/app/provider/queries/materialize_ts.sql

COPY provider/scripts/spark/offline_store_spark_runner.py $SPARK_FILEPATH
COPY provider/scripts/spark/python_packages.sh $SPARK_PYTHON_PACKAGES
COPY provider/scripts/spark/requirements.txt $SPARK_REQUIREMENTS
COPY provider/queries/materialize_no_ts.sql $MATERIALIZE_NO_TIMESTAMP_QUERY_PATH
COPY provider/queries/materialize_ts.sql $MATERIALIZE_TIMESTAMP_QUERY_PATH

ENV SPARK_SCRIPT_PATH="/app/provider/scripts/spark/offline_store_spark_runner.py"
ENV PYTHON_INIT_PATH="/app/provider/scripts/spark/python_packages.sh"
ENV MATERIALIZE_NO_TIMESTAMP_QUERY_PATH="/app/provider/queries/materialize_no_ts.sql"
ENV MATERIALIZE_TIMESTAMP_QUERY_PATH="/app/provider/queries/materialize_ts.sql"

ARG SPARK_FILEPATH=/app/provider/scripts/spark/offline_store_spark_runner.py
ARG SPARK_PYTHON_PACKAGES=/app/provider/scripts/spark/python_packages.sh
ARG SPARK_REQUIREMENTS=/app/provider/scripts/spark/requirements.txt
ARG MATERIALIZE_NO_TIMESTAMP_QUERY_PATH=/app/provider/queries/materialize_no_ts.sql
ARG MATERIALIZE_TIMESTAMP_QUERY_PATH=/app/provider/queries/materialize_ts.sql

COPY provider/scripts/spark/offline_store_spark_runner.py $SPARK_FILEPATH
COPY provider/scripts/spark/python_packages.sh $SPARK_PYTHON_PACKAGES
COPY provider/scripts/spark/requirements.txt $SPARK_REQUIREMENTS
COPY provider/queries/materialize_no_ts.sql $MATERIALIZE_NO_TIMESTAMP_QUERY_PATH
COPY provider/queries/materialize_ts.sql $MATERIALIZE_TIMESTAMP_QUERY_PATH

# Take the MD5 hash of the Spark runner script and store it in a file for use by the config package
# when determining the remove filepath in cloud object storage (e.g. S3). By adding the hash as a suffix
# to the file, we ensure that different versions of the script are uploaded to cloud object storage
# without overwriting previous or future versions.
RUN cat $SPARK_FILEPATH | md5sum \
    | awk '{print $1}' \
    | xargs echo -n > /app/provider/scripts/spark/offline_store_spark_runner_md5.txt

ENV PYTHON_INIT_PATH="/app/provider/scripts/spark/python_packages.sh"
ENV MATERIALIZE_NO_TIMESTAMP_QUERY_PATH="/app/provider/queries/materialize_no_ts.sql"
ENV MATERIALIZE_TIMESTAMP_QUERY_PATH="/app/provider/queries/materialize_ts.sql"

EXPOSE 8080
ENTRYPOINT ["./main"]
