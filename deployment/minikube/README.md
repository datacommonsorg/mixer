# Bring up the Mixer and ESP locally in Minikube

Mixer is deployed in Google Kubenetes Engine (GKE) and exposed as REST API via
Google Cloud Endpoint. We can use [MiniKube](https://minikube.sigs.k8s.io/docs/) to develop and test locally.

## Prerequisit

* Install `Docker`, following [Docker Desktop](https://www.docker.com/products/docker-desktop) install guide. In the settting, set the memory usage to be 6G.

* Install `Minikube`, following the [Installation](https://minikube.sigs.k8s.io/docs/start/) guide.

* Install [`kubectl`](https://kubernetes.io/docs/tasks/tools/install-kubectl/).

* Install [`protoc`](http://google.github.io/proto-lens/installing-protoc.html).

* Install [`yq`](https://mikefarah.gitbook.io/yq/).

* If not done yet, run gcloud application credential and copy the auth key:

```bash
gcloud components update
gcloud auth login
gcloud auth application-default login

gcloud iam service-accounts keys create mixer-robot-key.json \
      --iam-account mixer-robot@datcom-mixer-staging.iam.gserviceaccount.com
```

## Run Minikube cluster

From a terminal, start a cluster:

```bash
minikube start
```

Use local docker image with Minikube.

**NOTE** Must run `docker build` below in the same terminal as this one,
so the docker image can be found.

```bash
eval $(minikube docker-env)
```

Start Minikube dashboard (in a different terminal)

```bash
minikube dashboard
```

Create a new namespace "mixer"

```bash
kubectl create namespace mixer
```

Mount the GCP credential

```bash
kubectl create secret generic mixer-robot-key --from-file=mixer-robot-key.json --namespace=mixer
```

## Generate YAML files

```bash
./generate_yaml.sh
```

## Prepare GRPC descriptor [Run after proto change]

```bash
cd ../..
prepare-proto.sh
protoc \
  --proto_path=proto \
  --include_source_info \
  --descriptor_set_out deployment/minikube/mixer-grpc.pb \
  --go_out=internal \
  --go-grpc_out=internal \
  --go-grpc_opt=requireUnimplementedServers=false \
  proto/*.proto
cd deployment/minikube
```

## Build Docker Image [Run after code change]

```bash
cd ../..
DOCKER_BUILDKIT=1 docker build --tag mixer:local .
cd deployment/minikube
```

## Deploy ESP configuration  [Run after proto change]

```bash
gcloud config set project datcom-mixer-staging
gcloud endpoints services deploy mixer-grpc.pb endpoints.yaml
. env.sh
gcloud services enable $SERVICE_NAME
```

## Deployment to Minikube

```bash
kubectl apply -f deployment.yaml -f service.yaml
```

## Access the service

Use kubectl to forward the port:

```bash
kubectl port-forward service/mixer-service 8080:80 -n mixer
```

Send request to API:

```bash
curl '127.0.0.1:8080/node/property-labels?dcids=geoId/05'
```
