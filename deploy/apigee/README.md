# Apigee for Data Commons APIs with Terraform

## Host project setup

1. Decide which GCP project will host your Apigee setup and associated external load balancer. This is $HOST_PROJECT_ID.
1. Create an Apigee organization in that project using [one-click provisioning](https://cloud.google.com/apigee/docs/api-platform/get-started/one-click).

## Backend project setup

### Mixer API

1. Deploy API to Cloud Endpoints
1. Make note of its public IP
1. Generate one valid API key and make note of it

### NL/LLM APIs

1. Publish a Private Service Connect service for each relevant internal load balancer. Make note of the service project and service name.

## Apigee resource deployment (local command line)

1. Install gcloud, terraform, yq
1. Set gcloud project to Apigee host project: `gcloud config set project $HOST_PROJECT_ID`
1. Create GCS bucket for Terraform state: `gsutil mb "gs://$HOST_PROJECT_ID-tf"`
1. Set `ENV_NAME=<nonprod|prod|something else>`
1. Set up env-specific files:
   - Proxy structure config `envs/$ENV_NAME.yaml`
   - Proxy variable substitution values `./$ENV_NAME.env`. Use a temp value for PSC host IP if you haven't created an endpoint attachment yet. Other values should have been noted during previous steps.
   - Apigee + load balancer Terraform `terraform/$ENV_NAME/*`
1. First time only: run `terraform init` from this directory.
1. From this directory, run `./deploy_apigee.sh $ENV_NAME`
1. If the deployment created a PSC endpoint attachment, more steps are needed:
   - Add the outputted endpoint attachment hosts as PSC IPs in .env file
   - Go to the PSC project and approve the connection.
   - TODO Firewall rules
   - Change tfvars `include_proxies` to true and run deploy script again.
1. If proxies were
