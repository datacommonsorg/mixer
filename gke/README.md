# First Time GCP and GKE Setup

Instructions of the initial setup of the GCP project and GKE clusters for deploying Data Commons Mixer service.

## Steps

* [Create a Google Cloud Project](https://cloud.google.com/resource-manager/docs/creating-managing-projects). If working on an existing project, you should have owner/editor role to perform the tasks.

* Install [yq](https://mikefarah.gitbook.io/yq/v/v3.x/). Make sure the version is 3.x

  ```bash
  yq --version
  ```

* Make a copy of the `config.yaml.tpl` as `config.yaml`. We will populate the fields in `config.yaml` as we progress through the walkthrough.

  ```bash
  cp config.yaml.tpl config.yaml
  ```

* Add the GCP project id used for deployment in `config.yaml`, **project** field.

* Add the GCP project that hosts the Bigquery table in `config.yaml`, **store** field.

* Install [Google Cloud SDK](https://cloud.google.com/sdk/install).

* Authenticate `gcloud` operations locally.

  ```bash
  gcloud auth login
  gcloud components update
  ```

* Create a global static IP in GCP.
  * Run:

    ```bash
    ./create_ip.sh
    ```

  * Record the IP address and update it in `config.yaml`, **ip** field.

* Add the domain in `config.yaml`, **domain** field. If you want to use the domain from Cloud Endpoints, it would be `mixer.endpoints.<PROJECT_ID>.cloud.goog`.

* Enable the GCP services.

  ```bash
  ./enable_services.sh
  ```

* Create a service account.

  ```bash
  ./create_robot_account.sh
  ```

* Setup service account.
  * Run

    ```bash
    ./setup_robot_account.sh
    ```

  * If you see authentication errors, need to contact DataCommons team to complete some of the role binding operations as the service account need access to the Cloud Bigtable and Big Query.

* Deploy Extensive Service Proxy.
  * Add API title in `config.yaml`, **api_title** field. If omit, the API title would be the same as the domain.
  * Run:

    ```bash
    ./setup_esp.sh
    ```

* Create Google Managed SSL Certificate.

  ```bash
  ./setup_ssl.sh
  ```

* Create the cluster.
  * Add the cluster region in `config.yaml`, **region** field.
  * Add the number of nodes in `config.yaml`, **nodes** field.
  * Run:

    ```bash
    ./create_cluster.sh
    ```

* Make sure the managed SSL certificate is "ACTIVE" by checking ["Load balancing" in GCP](https://pantheon.corp.google.com/net-services/loadbalancing/advanced/sslCertificates/list?sslCertificateTablesize=50). This can take minutes up to hours.

* [Optional] DNS Setup for custom domain
  * This is only needed if you use custom domain (instead of the default domain provided by Cloud Endpoints).
  * [configure the DNS in the domain registrar](https://cloud.google.com/load-balancing/docs/ssl-certificates/google-managed-certs#update-dns).
