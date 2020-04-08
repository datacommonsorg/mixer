# Deploy Data Commons Mixer to GKE and Google Endpoints

## Do you need to update BT and BQ?

If yes, update (prod|staging)_bt_table.txt and (prod|staging)_bq_dataset.txt.
And please don't forget to get these change into master repository.

## Build & Deploy

Create a Google Cloud Platform (GCP) project and run the following command where:

`{PROJECT_ID}` refers to the GCP project id.

`{DOMAIN}` is optional, and only need to be set if you want to expose the endpoints from your custom domain.

To deploy the project, run the command below.

```shell
./gcp.sh {PROJECT_ID}
./gke.sh {PROJECT_ID} {DOMAIN}
```
./gke.sh should be run even if new mixer image is not pushed. If new mixer image is not being pushed use the image_id that is in prod.

## Build New Image

Data Commons mixer Docker image registry url: `gcr.io/datcom-mixer/go-grpc-mixer:{TAG}`.
Ask Data Commons team to obtain the TAG number.
To build a new image, go to the top level of this git repo and un the command below.
Tag number should be larger than any existing one.
Then update the docker image url in docker_image.txt.

```
gcloud builds submit --tag gcr.io/datcom-mixer/go-grpc-mixer:<TAG>
```

## Update Bigtable or BigQuery dataset.
If you need to point to a different Bigtable table or BigQuery dataset, update the corresponding bt_table.txt or bq_dataset.txt.
Both of them have two versions, prefixed with prod_ or staging_.

## (Optional) Use custom domain

Verify your domain as described in <https://cloud.google.com/endpoints/docs/openapi/verify-domain-name>

Visit your domain provider account and edit your domain settings. You must create an A record that contains your API name, for example, myapi.example.com, with the external IP address in deployment.yaml.

## Accessing API

Once successfully deployed, the endpoints is available at: `http://datacommons.endpoints.{PROJECT_ID}.cloud.goog/`. If `{DOMAIN}` is used in the steps above, the endpoints will also be accessible in the custom domain.
