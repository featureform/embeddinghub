# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

mkdir -p client/src/featureform/proto/
set -e
cp metadata/proto/metadata.proto client/src/featureform/proto/metadata.proto
cp proto/serving.proto client/src/featureform/proto/serving.proto

protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     ./proto/serving.proto
python3 -m grpc_tools.protoc -I ./client/src --python_out=./client/src --grpc_python_out=./client/src/ ./client/src/featureform/proto/serving.proto

protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     ./metadata/proto/metadata.proto
python3 -m grpc_tools.protoc -I ./client/src --python_out=./client/src/ --grpc_python_out=./client/src/ ./client/src/featureform/proto/metadata.proto