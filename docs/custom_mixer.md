# Deploy Custom Mixer

Mixer can be hosted in your own GCP project to serve SPARQL query for your data.

This tutorial lists the steps to set this up.

(TODO: Add more details to the following steps.)

## Fork code

For the [mixer code](https://github.com/datacommonsorg/mixer) to your own repo.
Then checkout the repo locally. After making changes following the steps below,
you can commit the changes back to your repo.

## Create Project

Create a new GCP project and setup billing account.

## Import Dataset

* Create Bigquery Dataset and import tables from csv files.
* Based on the Bigquery dataset, create a template MCF in [mapping.mcf](../deploy/overlays/custom/mapping.mcf).

## Setup GKE

* Follow the [instructions](../gke/README.md) to setup the GKE instance.

## Deployment

* Update the bigquery version [file](../deploy/overlays/custom/bigquery.version) to use
  the custom dataset.

* Deploy mixer to GKE

  ```bash
  gcloud config set project <PROJECT_ID>
  kustomize build overlays/custom > custom.yaml
  kubectl apply -f custom.yaml
  ```

These steps are needed when new data is added or schema is changed.
