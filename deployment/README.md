# Deploy Data Commons Mixer to GKE and Google Endpoints

Data Commmons Mixer is an API server hosted on GKE and exposed via Cloud
Endpoints.

## One Time Setup

-   [Create a Google Cloud Project](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
    with project id "project-id". Install the
    [Google Cloud SDK](https://cloud.google.com/sdk/install).

-   Set the project id as enviornment variable and authenticate the following
    steps.

    ```bash
    export PROJECT_ID="project-id"
    gcloud auth login
    gcloud config set project $PROJECT_ID
    ```

-   Create a service account that can interact with Cloud APIs

    ```bash
    export SERVICE_ACCOUNT_NAME="mixer-robot"
    export SERVICE_ACCOUNT="$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"
    # Create service account
    gcloud beta iam service-accounts create $SERVICE_ACCOUNT_NAME \
      --description "Service account for mixer" \
      --display-name "mixer-robot"
    # Enable service account
    gcloud alpha iam service-accounts enable $SERVICE_ACCOUNT
    # Allow service account to access Bigtable and Bigquery
    gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member serviceAccount:$SERVICE_ACCOUNT \
      --role roles/bigtable.reader
    gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member serviceAccount:$SERVICE_ACCOUNT \
      --role roles/bigquery.user
    # This key will be used later by GKE
    gcloud iam service-accounts keys create /tmp/mixer-robot-key.json \
      --iam-account $SERVICE_ACCOUNT
    ```

-   Create a new managed Google Cloud Service

    ```bash
    # Enable Service Control API
    gcloud services enable servicecontrol.googleapis.com
    # Create a static IP address for the API
    gcloud compute addresses create mixer-ip --global
    # Record the IP address. This will be needed to set the endpointsapi.yaml
    IP=$(gcloud compute addresses list --global --filter='name:mixer-ip' --format='value(ADDRESS)')
    # Set the domain for endpoints. This could be a custom domain or default domain from Endpoints like xxx.endpoints.$PROJECT_ID.cloud.goog
    export DOMAIN="<replace-me-with-your-domain>"
    # Create a blank service
    cat <<EOT > endpointsapi.yaml
    type: google.api.Service
    config_version: 3
    name: $DOMAIN
    producer_project_id: $PROJECT_ID
    EOT
    # Deploy the blank server
    gcloud endpoints services deploy endpointsapis.yaml
    ```

-   Create a GKE cluster

    ```bash
    gcloud components install kubectl
    gcloud services enable container.googleapis.com
    export CLUSTER_NAME="mixer-cluster"
    gcloud container clusters create $CLUSTER_NAME \
      --zone=us-central1-c \
      --machine-type=custom-4-26624
    gcloud container clusters get-credentials $CLUSTER_NAME
    ```

-   Setup GKE instance

    ```bash
    # Create namespace
    kubectl create namespace mixer
    # Mount service account secrete created above to the GKE instance
    kubectl create secret generic mixer-robot-key \
      --from-file=/tmp/key.json --namespace=mixer
    # Mount nginx config
    kubectl create configmap nginx-config --from-file=nginx.conf --namespace=mixer
    ```

-   Create SSL certificate for the Cloud Endpoints Domain.

    ```bash
    perl -i -pe's/DOMAIN/<replace-me-with-your-domain>/g' certificate.yaml
    # Deploy the certificate
    kubectl apply -f certificate.yaml
    ```

-   Create the ingress for GKE.

    ```bash
    kubectl apply -f ingress-ssl.yaml
    ```

-   Deploy the GKE service.

    ```bash
    kubectl apply -f service.yaml
    ```
