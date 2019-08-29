# Deploy DataCommons Mixer to GKE and Google Endpoints.


## Build and deploy

Create a Google Cloud Platform (GCP) project.

Run the following command.

`<PROJECT_ID>` refers to the GCP project id, where:

`<IMAGE>` is DataCommons mixer docer image registry url. It is in the form of `gcr.io/datcom-mixer/go-grpc-mixer:<TAG>`. Ask DataCommons team to obtain the TAG number.

`<DOMAIN>` is optional, and only need to be set if you want to expose the endpoints from your custom domain.

    ./gcp.sh <PROJECT_ID>
    ./gke.sh <PROJECT_ID> <IMAGE> <DOMAIN>

## (Optional) Use custom domain

    Verify your domain as described in https://cloud.google.com/endpoints/docs/openapi/verify-domain-name

    Visit your domain provider account and edit your domain settings. You must create an A record that contains your API name, for example, myapi.example.com, with the external IP address in deployment.yaml.


## Accessing API

Once successfully deployed, the endpoints is available at: `http://datacommons.endpoints.<PROJECT_ID>.cloud.goog/` and the
cusome domain if you specify that in the deployment steps above.
