# Deploy Data Commons Mixer

Data Commmons Mixer is a gRPC server deployed on Google Kubernetes Engine (GKE).
The server can be exposed as REST API via Google Cloud Endpoints by gRPC transcoding.

## Set environment variables

```bash
export PROJECT_ID="<project-id>"
export SERVICE_ACCOUNT_NAME="mixer-robot"
export SERVICE_ACCOUNT="$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"
export CLUSTER_NAME="mixer-cluster"
```

## Setup Google Cloud Project

* [Create a Google Cloud Project](https://cloud.google.com/resource-manager/docs/creating-managing-projects) with project id "project-id".

* Install the [Google Cloud SDK](https://cloud.google.com/sdk/install).

* Set the project id as enviornment variable and authenticate.

  ```bash
  gcloud auth login
  gcloud config set project $PROJECT_ID
  ```

* Create a service account that can interact with Cloud APIs.

  ```bash
  # Create service account
  gcloud beta iam service-accounts create $SERVICE_ACCOUNT_NAME \
    --description "Service account for mixer" \
    --display-name "mixer-robot"
  # Enable service account
  gcloud alpha iam service-accounts enable $SERVICE_ACCOUNT
  # Allow service account to access Bigtable and Bigquery
  gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member serviceAccount:$SERVICE_ACCOUNT \
    --role roles/bigtable.reader
  gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member serviceAccount:$SERVICE_ACCOUNT \
    --role roles/bigquery.user
  # This key will be used later by GKE
  gcloud iam service-accounts keys create /tmp/mixer-robot-key.json \
    --iam-account $SERVICE_ACCOUNT
  ```

* Create a GKE cluster

  ```bash
  gcloud components install kubectl
  gcloud services enable container.googleapis.com
  gcloud container clusters create $CLUSTER_NAME \
    --num-nodes=10 \
    --zone=us-central1-c \
    --machine-type=e2-highmem-4
  gcloud container clusters get-credentials $CLUSTER_NAME
  ```

* Setup GKE instance

  ```bash
  # Create namespace
  kubectl create namespace mixer
  # Mount service account secrete created above to the GKE instance
  kubectl create secret generic mixer-robot-key \
    --from-file=/tmp/mixer-robot-key.json --namespace=mixer
  ```

## Deploy Mixer gRPC service (Option 1)

This deploys mixer gRPC service standalone (without REST to gRPC transcoding)

```bash
perl -i -pe"s/PROJECT_ID/$PROJECT_ID/g" mixer-grpc.yaml
perl -i -pe"s/BIGTABLE/$(head -1 bigtable.txt)/g" mixer-grpc.yaml
perl -i -pe"s/BIGQUERY/$(head -1 bigquery.txt)/g" mixer-grpc.yaml
kubectl apply -f mixer-grpc.yaml
```

This will deploy mixer to GKE and exposed via an external load balancer service.
To get the external ip, run

```bash
kubectl get services --namespace=mixer
```

Look for the "EXTERNAL-IP" field in the output and then you can send gRPC
request via a client. One example is in "https://github.com/datacommonsorg/mixer/blob/master/examples/main.go".

Under "examples" folder, run

```bash
go run main.go --addr=<EXTERNAL-IP>:80
  ```

## Deploy Mixer gRPC service and Cloud Endpoints (Option 2)

To expose as an http(s) REST endpoints, use the Google Cloud Endpoints for gPPC transcoding.

The gRPC API is transcoded to the REST API vis Google [Cloud Endpoints](https://cloud.google.com/endpoints/docs/quickstart-endpoints).
The REST path is defined in `proto/` and is transcoded into [http method](https://cloud.google.com/endpoints/docs/grpc/transcoding#map_a_get_method).
By default, Cloud Endpoints converts protobuf snake case fields into camel case for the REST response.

* Mount nginx config

  ```bash
  kubectl create configmap nginx-config --from-file=nginx.conf --namespace=mixer
  ```

* Create a new managed Google Cloud Service

  ```bash
  # Enable Service Control API
  gcloud services enable servicecontrol.googleapis.com
  # Create a static IP address for the API
  gcloud compute addresses create mixer-ip --global
  # Record the IP address. This will be needed to set the endpointsapi.yaml
  IP=$(gcloud compute addresses list --global --filter='name:mixer-ip' --format='value(ADDRESS)')
  # Set the domain for endpoints. This could be a custom domain or default domain from Endpoints like xxx.endpoints.$PROJECT_ID.cloud.goog
  export DOMAIN="<replace-me-with-your-domain>"

  # Create a blank service
  cat <<EOT > endpointsapi.yaml
  type: google.api.Service
  config_version: 3
  name: $DOMAIN
  apis:
  - name: datacommons.Mixer
  endpoints:
  - name: $DOMAIN
    target: "$IP"
  usage:
    rules:
    # All methods can be called without an API Key.
    - selector: "*"
      allow_unregistered_calls: true
  EOT

  # Deploy the blank server
  gcloud endpoints services deploy endpointsapis.yaml
  ```

* Create SSL certificate for the Cloud Endpoints Domain.

  ```bash
  perl -i -pe's/DOMAIN/<replace-me-with-your-domain>/g' certificate.yaml
  # Deploy the certificate
  kubectl apply -f certificate.yaml
  ```

* Create the ingress for GKE.

  ```bash
  kubectl apply -f ingress-ssl.yaml
  ```

* Deploy the mixer deployment and service.

  ```bash
  kubectl apply -f mixer-grpc-esp.yaml
  ```

## Binary Registry

* Mixer docker image: gcr.io/datcom-ci/datacommons-mixer
* Mixer grpc descriptor: gs://artifacts.datcom-ci.appspot.com/mixer-grpc/
