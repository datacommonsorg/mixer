# Apigee for Data Commons APIs with Terraform

## Host project setup

1. Decide which GCP project will host your Apigee setup and associated external load balancer.
1. Create an Apigee organization in that project using [one-click provisioning](https://cloud.google.com/apigee/docs/api-platform/get-started/one-click).

## Backend project setup

### Mixer API

1. Deploy API to Cloud Endpoints
1. Make note of its public IP
1. Generate one valid API key and make note of it

### NL/LLM APIs

1. Publish a Private Service Connect service for each relevant internal load balancer.

## Apigee resource deployment (local command line)

1. Install gcloud, terraform, yq
1. Set `ENV_NAME=<nonprod|prod|something else>`
1. Set up env-specific files:
   - Proxy structure config `envs/$ENV_NAME.yaml`
   - Proxy variable substitution values `./$ENV_NAME.env`.
   - Apigee + load balancer Terraform `terraform/$ENV_NAME/*`
1. Run `./deploy_apigee.sh $ENV_NAME`
1. If the deployment created a PSC endpoint attachment, go to the PSC project and approve the connection.
