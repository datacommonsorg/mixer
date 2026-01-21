# **Apigee for Data Commons APIs with Terraform**

## **Host project setup**

1. Decide which GCP project will host your Apigee setup and associated external load balancer. This is $HOST_PROJECT_ID.  
2. Create an Apigee organization in that project using [one-click provisioning](https://cloud.google.com/apigee/docs/api-platform/get-started/one-click).  
   * If the dashboard stops showing a quick link to the setup flow, try hitting console.cloud.google.com/apigee/setup/payg?project_id=\<your project ID\> directly.  
   * If the wizard runs into invalid state, use "Try this API" in the Apigee v1 API documentation to modify it.

## **Backend project setup**

### **Mixer API**

1. Deploy API to Cloud Endpoints or find an existing deployment.  
2. Make note of its hostname.  
3. Generate one valid API key and make note of it.

### **NL API**

1. Publish a Private Service Connect service for each relevant internal load balancer. Create new subnets as needed. Make note of each service project and service name.

### **MCP Endpoints**

For MCP (Model Context Protocol) services running on Cloud Run, the Apigee proxy acts as a secure facade.

* **Routing**: Requests are routed to the MCP backend if the path suffix matches /mcp or /mcp/\*\*.  
* **Security**: Access is governed by an API Key (passed as x-api-key header or key query parameter).  
* **Identity**: Apigee uses a **Google ID Token (OIDC)** to authenticate with Cloud Run.  
  * **Audience**: The \<Audience\> field in the HTTPTargetConnection must exactly match the Cloud Run Service URL.  
  * **Service Account**: The DEPLOYMENT_SERVICE_ACCOUNT email must be configured in the .env file.  
* **Permissions**: The DEPLOYMENT_SERVICE_ACCOUNT must have:  
  1. roles/run.invoker on the MCP Cloud Run service (to "knock on the door").  
  2. roles/iam.serviceAccountTokenCreator in the Apigee host project (to allow Apigee to "mint" the ID token).  
* **Timeouts**: The connection is configured with a high io.timeout.millis (300,000ms / 5 minutes) and response.streaming.enabled set to true to handle long-running data fetches.

## **Apigee resource deployment (local command line)**

### **Prerequisites**

1. Install gcloud, yq, and terraform.
   * **Terraform**: You MUST use the HashiCorp tap. The default brew version is incompatible.
     ```bash
     brew tap hashicorp/tap
     brew install hashicorp/tap/terraform
     ```

### **Initial deployment**

0. Ensure the [prerequisites](#prerequisites) are installed.
1. Set gcloud project to Apigee host project: gcloud config set project $HOST_PROJECT_ID.  
1. Create GCS bucket for Terraform state: gsutil mb "gs://$HOST_PROJECT_ID-tf".  
1. Set ENV_NAME=\<nonprod|prod\>.  
1. Create a local $ENV_NAME.env file with your initial variables.  
1. Create the destination in Secret Manager:  
   gcloud secrets create $ENV_NAME-env --project=$HOST_PROJECT_ID --data-file=$ENV_NAME.env  
1. **IMPORTANT**: Manually edit terraform/$ENV_NAME/terraform.tf to configure the bucket for the GCS backend.  
1. Configure references to resources created by Apigee one-click provisioning:  
   * Set tfvars apigee_lb_url_map_name from gcloud compute url-maps list.  
   * Set tfvars apigee_backend_service_name from gcloud compute backend-services list.  
1. Run ./deploy_apigee.sh $ENV_NAME.

### **Updates to existing deployment**

0. Ensure the [prerequisites](#prerequisites) are installed.
1. Ensure you either do not have a local $ENV_NAME.env file or it has no *unintended* changes relative to the version in Secret Manager.
1. Run ./deploy_apigee.sh $ENV_NAME.  
1. Type "yes" to confirm the Terraform plan.  
1. **Auto-Sync Prompt**: After deployment completes, the script checks if your local .env matches Secret Manager. If they differ, it will prompt you to `--push` your local changes to the cloud. **Note**: Only say yes if you are certain your local file contains the most up-to-date versions of all shared variables.

#### **Adding or Updating Environment Variables & Secrets**

The project uses a **Pull-Edit-Push** workflow to manage sensitive configuration. This is the safest way to add new variables (like DEPLOYMENT_SERVICE_ACCOUNT) without overwriting teammate changes.

1. **Pull the current secrets**: Run ./sync_env.sh $ENV_NAME --pull. This ensures you have the latest "Master Record" from the cloud and prevents accidental deletion of existing variables.  
2. **Edit locally**: Open your local $ENV_NAME.env and add your new key-value pair.  
3. **Push to cloud**: Run ./sync_env.sh $ENV_NAME --push. This commits your changes to Secret Manager as the new "Source of Truth" for the team.  
4. **Deploy**: Run ./deploy_apigee.sh $ENV_NAME.

## **Known issues**

* **Analytics**: Cannot be enabled via Terraform. Enable manually at console.cloud.google.com/apigee/addons/analytics.  
* **Sequential Runs**: Modifying multiple endpoint attachments often requires multiple runs of the deploy script to resolve dependencies.