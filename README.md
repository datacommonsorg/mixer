# DataCommons Mixer Development

## Development Process

In https://github.com/datacommonsorg/mixer, click on "Fork" button to fork the repo.

Clone your forked repo to your desktop.

Add datacommonsorg/mixer repo as a remote.

```shell
git remote add dc https://github.com/datacommonsorg/mixer.git
```

Every time when you want to send a Pull Request, do the following steps:

```shell
git checkout master
git pull dc master
git checkout -b new_branch_name
# Make some code change
git add .
git commit -m "commit message"
git push -u origin new_branch_name
```

Then in your forked repo, can send a Pull Request.

Wait for approval of the Pull Request and merge the change.

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

    gcloud auth application-default login

    go run server/main.go \
      --bq_dataset=google.com:datcom-store-dev.dc_v3_clustered \
      --bt_table=dc28 \
      --bt_project=google.com:datcom-store-dev \
      --bt_instance=prophet-cache \
      --project_id=datcom-mixer \
      --schema_path=deployment/mapping

    cd examples
    ./run_all.sh

## Build mixer docker image and submit to Google Cloud Registry

    gcloud builds submit --tag gcr.io/datcom-mixer/go-grpc-mixer:<TAG> .
