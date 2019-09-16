# DataCommons Mixer Development

## Generate Protobuf go code and out.pb

Install protoc by following [this](http://google.github.io/proto-lens/installing-protoc.html).

Run the following code to get the proto dependency.

    go get github.com/golang/protobuf/protoc-gen-go
    mkdir -p proto/google/api/
    curl -sSL https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/annotations.proto --output proto/google/api/annotations.proto
    curl -sSL https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/http.proto --output proto/google/api/http.proto

Generate protobuf code and out.pb (used for cloud endpoints deployment).

    protoc \
        --proto_path=proto \
        --include_source_info \
        --descriptor_set_out deployment/out.pb \
        --go_out=plugins=grpc:proto \
        proto/mixer.proto

## Run grpc server and examples locally

    go run server/main.go \
      --bq_dataset=google.com:datcom-store-dev.dc_v3_clustered \
      --bt_table=dc3 \
      --bt_project=google.com:datcom-store-dev \
      --bt_instance=prophet-cache \
      --project_id=datcom-mixer \
      --schema_path=deployment/mapping

    cd examples
    ./run_all.sh

## Build mixer docker image and submit to Google Cloud Registry

    gcloud builds submit --tag gcr.io/datcom-mixer/go-grpc-mixer:<TAG> .
