# Deploy Custom Mixer

Mixer can be hosted in your own GCP project to serve SPARQL query for your data.

This tutorial lists the steps to set this up.

(TODO: Add more details to the following steps.)

## Create Project

Create a new GCP project and setup billing account.

## Import Dataset

* Create Bigquery Dataset and import tables from csv files.
* Create template MCF files (schema mapping) based on the Bigquery dataset.

## Setup GKE

* Follow the [instructions](../gke/README.md) to setup the GKE instance.

## Deployment

* Remove all the .mcf files from [mapping](../deploy/mapping) directory and add
  the custom template MCF files created above in that directory.

* Change the mapping customization [file](../deploy/mapping/kustomization.yaml)
  to use the custom MCF files.

* Update the bigquery version [file](../deploy/storage/bigquery.version) to use
  the custom dataset.

* Deploy mixer to GKE

  ```bash
  gcloud config set project <PROJECT_ID>
  kustomize build overlays/custom > custom.yaml
  kubectl apply -f custom.yaml
  ```

These steps are needed when new data is added or schema is changed.
