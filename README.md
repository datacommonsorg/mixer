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

```bash
go get -u google.golang.org/protobuf/cmd/protoc-gen-go
go get -u google.golang.org/grpc/cmd/protoc-gen-go-grpc
mkdir -p proto/google/api/
curl -sSL https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/annotations.proto --output proto/google/api/annotations.proto
curl -sSL https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/http.proto --output proto/google/api/http.proto
```

Generate protobuf code and out.pb (used for cloud endpoints deployment).

```bash
protoc \
    --proto_path=proto \
    --include_source_info \
    --go_out=. \
    --go-grpc_out=. \
    --go-grpc_opt=requireUnimplementedServers=false \
    proto/mixer.proto
```

## E2E test locally.

Install `cloud-build-local` following [the guide](https://cloud.google.com/cloud-build/docs/build-debug-locally), then run:

```bash
cloud-build-local --config=cloudbuild.yaml --dryrun=false .
```

## Run grpc server and examples locally

```bash
gcloud auth application-default login

go run main.go \
    --bq_dataset=google.com:datcom-store-dev.dc_kg_2020_04_13_02_32_53 \
    --bt_table=borgcron_2020_04_13_02_32_53 \
    --bt_project=google.com:datcom-store-dev \
    --bt_instance=prophet-cache \
    --project_id=datcom-mixer \
    --schema_path=deployment/mapping

# Open a new shell
cd examples/get_place_obs
go run main.go
```