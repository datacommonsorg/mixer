# Deploy Data Commons Mixer to GKE and Google Endpoints

## Build and deploy

Create a Google Cloud Platform (GCP) project and run the following command where:

`{PROJECT_ID}` refers to the GCP project id.

`{IMAGE}` is Data Commons mixer Docker image registry url. It is in the form of `gcr.io/datcom-mixer/go-grpc-mixer:{TAG}`. Ask Data Commons team to obtain the TAG number.

`{BT_TABLE}` is the Bigtable table in use now, eg: dc22. If updated, please also update template_deployment.yaml.

`{DOMAIN}` is optional, and only need to be set if you want to expose the endpoints from your custom domain.

```shell
./gcp.sh {PROJECT_ID}
./gke.sh {PROJECT_ID} {IMAGE} {BT_TABLE} {DOMAIN}
```

## (Optional) Use custom domain

Verify your domain as described in <https://cloud.google.com/endpoints/docs/openapi/verify-domain-name>

Visit your domain provider account and edit your domain settings. You must create an A record that contains your API name, for example, myapi.example.com, with the external IP address in deployment.yaml.

## Accessing API

Once successfully deployed, the endpoints is available at: `http://datacommons.endpoints.{PROJECT_ID}.cloud.goog/`. If `{DOMAIN}` is used in the steps above, the endpoints will also be accessible in the custom domain.
