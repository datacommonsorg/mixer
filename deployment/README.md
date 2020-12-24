# Deploy Data Commons Mixer

Data Commmons Mixer is a gRPC server deployed on Google Kubernetes Engine (GKE).
The server is exposed as REST API via Google Cloud Endpoints by gRPC transcoding.

## Setup a new GKE cluster

Follow the steps in GKE setup [README](gke/README.md)

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
