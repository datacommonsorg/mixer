# Apigee for Data Commons APIs with Terraform

## Host project setup

1. Decide which GCP project will host your Apigee setup and associated external load balancer. This is $HOST_PROJECT_ID.
1. Create an Apigee organization in that project using [one-click provisioning](https://cloud.google.com/apigee/docs/api-platform/get-started/one-click).
   - If the dashboard stops showing a quick link to the setup flow, try hitting console.cloud.google.com/apigee/setup/payg?project_id=<your project ID> directly.
   - If the wizard runs into invalid state, use "Try this API" in the Apigee v1 API documentation to modify it.

## Backend project setup

### Mixer API

1. Deploy API to Cloud Endpoints or find an existing deployment.
1. Make note of its hostname.
1. Generate one valid API key and make note of it.

### NL API(s)

1. Publish a Private Service Connect service for each relevant internal load balancer. Create new subnets as needed. Make note of each service project and service name.

## Apigee resource deployment (local command line)

### Initial deployment

1. Install gcloud, terraform, yq
1. Set gcloud project to Apigee host project: `gcloud config set project $HOST_PROJECT_ID`
1. Create GCS bucket for Terraform state: `gsutil mb "gs://$HOST_PROJECT_ID-tf"`
1. Set `ENV_NAME=<nonprod|prod|something else>`
1. Set up env-specific files:
   - Proxy structure config `envs/$ENV_NAME.yaml`
   - Proxy variable substitution values `./$ENV_NAME.env`. Use a temp value for PSC host IP if you haven't created an endpoint attachment yet. Other values should have been noted during previous steps.
   - Apigee + load balancer Terraform `terraform/$ENV_NAME/*`
1. Create a destination in Secrets Manager for your `./$ENV_NAME.env` file. For example, for the file nonprod.env, you would run `gcloud secrets create nonprod-env --project=<project_id from YAML> --data-file=nonprod.env`
1. Configure references to resources created by Apigee one-click provisioning:
   - Set tfvars `apigee_lb_url_map_name` from `gcloud compute url-maps list`.
   - Set tfvars `apigee_backend_service_name` from `gcloud compute backend-services list`.
1. First time only: run `terraform init` from this directory.
1. From this directory, run `./deploy_apigee.sh $ENV_NAME`
1. Set up PSC southbound backends and create API proxies + products:

   - Add endpoint attachment hosts from Apigee UI as PSC IPs in .env file
   - Go to PSC projects and approve connections.
   - You may need to add firewall rules in PSC projects to allow ingress on TCP ports 80 and 443 for PSC subnets.
   - Change tfvars `include_proxies` to true and run deploy script again.

### Updates to existing deployment

From this directory, run `./deploy_apigee.sh $ENV_NAME`

## Known issues

- The Apigee analytics add-on cannot be enabled via Terraform. Go to console.cloud.google.com/apigee/addons/analytics?project=<your project ID> to enable it manually.
- It takes multiple runs to make all changes if more than one endpoint attachment is being modified.
