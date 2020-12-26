# First Time GCP and GKE Setup

This instruction contains the steps to do the first time setup of the GCP project and GKE clusters for deploying Data Commons Mixer service.

## Steps

* [Create a Google Cloud Project](https://cloud.google.com/resource-manager/docs/creating-managing-projects). If working on an existing project, you should have owner/editor role to perform the tasks.

* Record the project id in config.yaml, `project` field.

* Install the [Google Cloud SDK](https://cloud.google.com/sdk/install)

* Authenticate `gcloud` operations locally

  ```bash
  gcloud auth login
  gcloud components update
  ```

* Create a global static IP in GCP.
  * Run:

    ```bash
    ./create_ip.sh
    ```

  * Record the IP address and update it in config.yaml, `ip` field.

* Record the domain in config.yaml, `domain` field. If you want to use the domain from Cloud Endpoints, it would be `mixer.endpoints.<PROJECT_ID>.cloud.goog`

* Enable the GCP services.

  ```bash
  ./enable_services.sh
  ```

* Create a service account.

  ```bash
  ./create_robot_account.sh
  ```

* Setup service account.
  * You may need to ask DataCommons team to do this, if you don't have the owner / editor permission for the data storage project.

  ```bash
  ./setup_robot_account.sh
  ```

* Deploy Extensive Service Proxy.
  * Put an API title in config.yaml, `api_title` field. If omit, the API title would be the same as the domain.
  * Run:

    ```bash
    ./setup_esp.sh
    ```

* Create Google Managed SSL Certificate.

  ```bash
  ./setup_ssl.sh
  ```

* Create the cluster.
  * Record the cluster region in config.yaml, `region` field.
  * Record the cluster nodes in config.yaml, `nodes` field.
  * Run:

    ```bash
    ./create_cluster.sh
    ```

* Make sure the managed SSL certificate is "ACTIVE" by checking ["Load balancing" in GCP](https://pantheon.corp.google.com/net-services/loadbalancing/advanced/sslCertificates/list?sslCertificateTablesize=50). This can take minutes up to hours.

* [Optional] DNS Setup for custom domain
  * [configure the DNS in the domain registrar](https://cloud.google.com/load-balancing/docs/ssl-certificates/google-managed-certs#update-dns)
