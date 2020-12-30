# Deployment Configuration

Mixer is deployed to Kubernetes cluster either locally on Minikube or in Google Kubernetes Engine.

## Folder structure

* `db`: BigQuery and Bigtable version config
* `base`: Base Kubernetes config yaml files.
* `overlays`: Kubernetes deployment yaml files extending the base yaml files on dev/staging/prod environment.
* `gke`: GKE cluster config.

## Generate kubetcl yaml file and deploy

A deployable yaml file from base and overlay using `Kustomize`. For example,
to build the staging yaml files and deploy to GKE:

```bash
# kubectl config use-context <CONTEXT_NAME>
kustomize build overlays/staging > staging.yaml
kubectl apply -f staging.yaml
```
