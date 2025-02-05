# docker buildx build -f ./api/Dockerfile . -t featureformcom/api-server:latest -o type=image --platform=linux/arm64,linux/amd64 --push
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
COPY ./storage ./storage
COPY ./schema ./schema
COPY ./lib/ ./lib/
COPY ./filestore/ ./filestore/
COPY ./metadata/*.go ./metadata/
COPY ./integrations/ ./integrations/
COPY ./lib/ ./lib/
COPY ./metadata/search/ ./metadata/search/
COPY ./metadata/proto/ ./metadata/proto/
COPY ./metadata/equivalence/ ./metadata/equivalence/
COPY ./metadata/common/ ./metadata/common/
COPY ./proto/ ./proto/
COPY ./helpers/ ./helpers/
COPY ./logging/ ./logging/
COPY ./types ./types
COPY ./kubernetes ./kubernetes
COPY ./provider/ ./provider/
COPY ./provider/provider_config/ ./provider/provider_config/
COPY ./provider/provider_type/ ./provider/provider_type/
COPY ./config/ ./config/
COPY ./health ./health
COPY ./scheduling ./scheduling
COPY ./api/ ./api/

WORKDIR ./api/main/

RUN go build main.go


FROM alpine

WORKDIR /app

COPY --from=0 ./app/api/main/main ./main
COPY --from=0 ./app/provider/scripts/ /app/provider/scripts/
COPY --from=0 ./app/provider/queries/ /app/provider/queries/

ENV SPARK_SCRIPT_PATH="/app/provider/scripts/spark/offline_store_spark_runner.py"
ENV PYTHON_INIT_PATH="/app/provider/scripts/spark/python_packages.sh"
ENV MATERIALIZE_NO_TIMESTAMP_QUERY_PATH="/app/provider/queries/materialize_no_ts.sql"
ENV MATERIALIZE_TIMESTAMP_QUERY_PATH="/app/provider/queries/materialize_ts.sql"

COPY provider/scripts/spark/offline_store_spark_runner.py $SPARK_FILEPATH

# Take the MD5 hash of the Spark runner script and store it in a file for use by the config package
# when determining the remove filepath in cloud object storage (e.g. S3). By adding the hash as a suffix
# to the file, we ensure that different versions of the script are uploaded to cloud object storage
# without overwriting previous or future versions.
RUN cat $SPARK_SCRIPT_PATH | md5sum \
    | awk '{print $1}' \
    | xargs echo -n > /app/provider/scripts/spark/offline_store_spark_runner_md5.txt

EXPOSE 8080
ENTRYPOINT ["./main"]
