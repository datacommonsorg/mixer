# Data Commons Mixer Developer Guide

The developement uses [Kustomize](https://kubectl.docs.kubernetes.io/guides/introduction/kustomize/)
to manage yaml templating and composition. Detailed deploy folder structure can be
found [here](../deploy/README.md).

Local development uses [Skaffold](https://skaffold.dev) to manage the build, deploy and
port forwarding.

## Prerequisit

* Contact DataCommons team to get data access to Cloud Bigtable and BigQuery.

* Install the following tools to develop mixer locally (with Docker):
  * [`Docker`](https://www.docker.com/products/docker-desktop)
  * [`Minikube`](https://minikube.sigs.k8s.io/docs/start/)
  * [`Skaffold`](https://skaffold.dev/docs/install/)
  * [`gcloud`](https://cloud.google.com/sdk/docs/install)
  * [`kubectl`](https://kubernetes.io/docs/tasks/tools/install-kubectl/)

* If you prefer to do it locally without Docker, then need to install the following:
  * [`Golang`](https://golang.org/doc/install)
  * [`protoc`](http://google.github.io/proto-lens/installing-protoc.html)

* Authenticate to GCP

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
minikube start --meory 6G
minikube addons enable gcp-auth
eval $(minikube docker-env)
skaffold dev --port-forward -n mixer
```

This exposes the local mixer service at `localhost:9090`.

To verify the server serving request:

```bash
curl http://localhost:9090/node/property-labels?dcids=Class
```

After code edit, the container images are automatically rebuilt and re-deployed to the local cluster.

### Run Tests

```bash
./script/run_test.sh -d
```

### Update e2e test golden files

```bash
./script/update_golden_staging.sh -d
```

## Develop mixer locally as a Go server (non-Docker)

**NOTE** This can only develop and test the gRPC server. Since the [ESP](https://cloud.google.com/endpoints/docs/grpc/running-esp-localdev) is not
brought up here, can not test the REST API.

### Start mixer as a gRPC server

Run the following code to generate Go proto files.

```bash
go get google.golang.org/protobuf/cmd/protoc-gen-go@v1.23.0
go get google.golang.org/grpc/cmd/protoc-gen-go-grpc@v0.0.0-20200824180931-410880dd7d91
protoc \
  --proto_path=proto \
  --go_out=internal \
  --go-grpc_out=internal \
  --go-grpc_opt=requireUnimplementedServers=false \
  proto/*.proto
```

Run the following code to start mixer gRPC server

```bash
# cd into repo root directory

go run cmd/main.go \
    --bq_dataset=$(head -1 deploy/storage/bigquery.version) \
    --bt_table=$(head -1 deploy/storage/bigtable.version) \
    --bt_project=google.com:datcom-store-dev \
    --bt_instance=prophet-cache \
    --project_id=datcom-mixer-staging

# In a new shell
cd examples && go run main.go
```

### Run Tests (Go)

```bash
./script/run_test.sh
```

### Update e2e test golden files (Go)

```bash
./script/update_golden_staging.sh
```

## Update prod golden files

Run the following commands to update prod golden files from staging golden files

```bash
./script/update_golden_prod.sh
```
