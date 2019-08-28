# README

## Generate Protobuf go code and out.pb

    go get github.com/golang/protobuf/protoc-gen-go
    mkdir -p proto/google/api/
    curl -sSL https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/annotations.proto --output proto/google/api/annotations.proto
    curl -sSL https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/http.proto --output proto/google/api/http.proto

    protoc \
    --proto_path=proto \
    --include_source_info \
    --descriptor_set_out deployment/out.pb \
    --go_out=plugins=grpc:proto \
    proto/mixer.proto

## Run grpc server and examples locally

    go run server/main.go \
      --bq_dataset=google.com:datcom-store-dev.dc_v3_clustered \
      --bt_table=dc7 \
      --bt_project=google.com:datcom-store-dev \
      --bt_instance=prophet-cache \
      --project_id=datcom-mixer \
      --schema_path=deployment/mapping

    cd examples
    ./run_all.sh

## Build and deploy

    gcloud builds submit --tag gcr.io/datcom-mixer/go-grpc-mixer:<TAG> .
    cd deployment
    ./gcp.sh <PROJECT_ID>
    ./gke.sh <PROJECT_ID> <TAG> <DOMAIN>

## (Optional) Use custom domain

    Verify your domain as described in https://cloud.google.com/endpoints/docs/openapi/verify-domain-name

    Visit your domain provider account and edit your domain settings. You must create an A record that contains your API name, for example, myapi.example.com, with the external IP address in deployment.yaml.
