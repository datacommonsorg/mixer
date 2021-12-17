# Deployment Configuration

Mixer is deployed to Kubernetes cluster either locally on
[Minikube](https://minikube.sigs.k8s.io/docs/) or in [Google Kubernetes Engine](https://cloud.google.com/kubernetes-engine).

## Folder structure

* `storage`: BigQuery and Bigtable version config
* `base`: Base Kubernetes config yaml files.
* `overlays`: Kubernetes deployment yaml files extending the base yaml files on dev/autopush/staging/prod environment.
* `gke`: GKE cluster config.

## Overlays

* `dev`: Yaml patches for local minikube cluster.
* `autopush`: Yaml patches for autopush GKE cluster (autopush.api.datacommons.org)
* `staging`: Yaml patches for staging GKE cluster (staging.api.datacommons.org)
* `prod`: Yaml patches for prod GKE cluster (prod mixer that serves api.datacommons.org)

## Generate kubetcl yaml file and deploy

A deployable yaml file from base and overlay using `Kustomize`. For example,
to build the staging yaml files and deploy to GKE:

```bash
# Switch to the desired kubernetes cluster:
kubectl config use-context <CONTEXT_NAME>
kustomize build overlays/staging > staging.yaml
kubectl apply -f staging.yaml
```

test