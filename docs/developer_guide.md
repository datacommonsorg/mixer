# Data Commons Mixer Developer Guide

The developement uses [Kustomize](https://kubectl.docs.kubernetes.io/guides/introduction/kustomize/)
to manage yaml templating and composition. Detailed deploy folder structure can be
found [here](../deploy/README.md).

Local development uses [Skaffold](https://skaffold.dev) to manage the build, deploy and
port forwarding.

## Prerequisite

- Contact DataCommons team to get data access to Cloud Bigtable and BigQuery.

- Install the following tools to develop mixer locally (with Docker):

  - [`Docker`](https://www.docker.com/products/docker-desktop)
  - [`Minikube`](https://minikube.sigs.k8s.io/docs/start/)
  - [`Skaffold`](https://skaffold.dev/docs/install/)
  - [`gcloud`](https://cloud.google.com/sdk/docs/install)
  - [`kubectl`](https://kubernetes.io/docs/tasks/tools/install-kubectl/)

- If you prefer to do it locally without Docker, then need to install the following:

  - [`Golang`](https://golang.org/doc/install)
  - [`protoc`](http://google.github.io/proto-lens/installing-protoc.html)

  Make sure to add `GOPATH` and update `PATH`:

  ```bash
  # Use the actual path of your Go installation
  export GOPATH=/Users/<USER>/go/
  export PATH=$PATH:$GOPATH
  ```

- Authenticate to GCP

  ```bash
  gcloud components update
  gcloud auth login
  gcloud auth application-default login
  ```

## Develop mixer locally with Docker and Kubernetes (Recommended)

Mixer and [ESP](https://cloud.google.com/endpoints/docs/grpc/running-esp-localdev)
is deployed on a local Minikube cluster.
To avoid using Endpoints API management and talking to GCP,
local deployment uses Json API configuration,
which is compiled using [API Compiler](https://github.com/googleapis/api-compiler).

### Start mixer in minikube

```bash
minikube start
minikube addons enable gcp-auth
eval $(minikube docker-env)
kubectl config use-context minikube
skaffold dev --port-forward -n mixer
```

This exposes the local mixer service at `localhost:8081`.

To verify the server serving request:

```bash
curl http://localhost:8081/node/property-labels?dcids=Class
```

After code edit, the container images are automatically rebuilt and re-deployed to the local cluster.

### Run Tests

```bash
./scripts/run_test.sh -d
```

### Update e2e test golden files

```bash
./scripts/update_golden.sh -d
```

## Develop mixer locally as a Go server (non-Docker)

**NOTE** This can only develop and test the gRPC server. Since the [ESP](https://cloud.google.com/endpoints/docs/grpc/running-esp-localdev) is not
brought up here, can not test the REST API.

### Generate Go proto files

Install the following packages as a one-time action.

```bash
cd ~/   # Be sure there is no go.mod in the local directory
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.23.0
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v0.0.0-20200824180931-410880dd7d91
```

Run the following command to generate Go proto files.

```bash
# In repo root directory
protoc \
  --proto_path=proto \
  --go_out=internal \
  --go-grpc_out=internal \
  --go-grpc_opt=requireUnimplementedServers=false \
  proto/*.proto
```

### Start Mixer as a gRPC server backed by Cloud BigTable (BigQuery)

Run the following code to start mixer gRPC server (without branch cache)

```bash
# In repo root directory
go run cmd/main.go \
    --mixer_project=datcom-mixer-staging \
    --store_project=datcom-store \
    --bq_dataset=$(head -1 deploy/storage/bigquery.version) \
    --import_group_tables=$(head -1 deploy/storage/bigtable_import_groups.version) \
    --schema_path=$PWD/deploy/mapping/ \
    --use_branch_bt=true

go run examples/main.go
```

### Start Mixer as a gRPC server backed by TMCF + CSV files

Mixer can load and serve TMCF + CSV files. This is used for a private Data Commons
instance. This requires to set the following flag

- `--use_tmcf_csv_data=true`
- `--tmcf_csv_bucket=<bucket-name>`
- `--tmcf_csv_folder=<folder-name>`

Prerequists:

- Create a GCS bucket <BUCKET_NAME>
- Create a folder in the bucket <FOLDER_NAME> to host all the data files
- Create a GCS PubSub notification:

  ```bash
  gsutil notification create -t tmcf-csv-reload -f json gs://BUCKET_NAME
  ```

Run the following code to start mixer gRPC server with TMCF + CSV files stored in GCS

```bash
# In repo root directory
go run cmd/main.go \
    --mixer_project=datcom-mixer-dev-316822 \
    --tmcf_csv_bucket=datcom-mixer-dev-resources \
    --tmcf_csv_folder=test \
    --use_tmcf_csv_data=true \
    --use_bigquery=false \
    --use_base_bt=false \
    --use_branch_bt=false
```

### Run Tests (Go)

```bash
./scripts/run_test.sh
```

### Update e2e test golden files (Go)

```bash
./scripts/update_golden.sh
```

### Run import group latency tests

In root directory, run:

```bash
./test/e2e/run_latency.sh
```

### Profile a program

Install [Graphgiz](https://graphviz.org/).

```bash
go test -v -parallel 1 -cpuprofile cpu.prof -memprofile mem.prof XXX_test.go
go tool pprof -png cpu.prof
go tool pprof -png mem.prof
```

## Update prod golden files

Run the following commands to update prod golden files from staging golden files

```bash
./scripts/update_golden_prod.sh
```
