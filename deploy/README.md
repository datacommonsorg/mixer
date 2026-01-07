# Deployment Configuration

Mixer is deployed to Kubernetes cluster in
[Google Kubernetes Engine](https://cloud.google.com/kubernetes-engine).

## Folder structure

- `storage`: BigQuery and Bigtable version config.
- `mapping`: Schema mapping files for Sparql Query.
- `helm_charts`: Helm charts configuration used for Kubernetes deployment.
- `gke`: GKE cluster config.

## Deploy to Mixer dev instance

To deploy changes to the Mixer dev instance, follow these steps:

1.  **Commit Changes:**
    Commit your changes to your local `mixer` repo.

2.  **Push Image:**
    Build and push your local code as a docker image to GCR (Artifact Registry) and upload the gRPC descriptor to GCS.
    ```bash
    ./scripts/push_image.sh
    ```
    *Note: This script uses `build/ci/cloudbuild.manual_push_image.yaml`.*

    The image gets uploaded here: https://pantheon.corp.google.com/artifacts/docker/datcom-ci/us/gcr.io/datacommons-mixer?project=datcom-ci \
    And the gRPC descriptor gets uploaded here: https://pantheon.corp.google.com/storage/browser/datcom-mixer-grpc/mixer-grpc?project=datcom-ci

3.  **(Optional) Update Deployment Config:**
    *   **If you have modified deployment configurations** (e.g., `deploy/helm_charts/values.yaml`, `deploy/helm_charts/envs/*.yaml`), you **MUST** pull these changes into the `website` repository prior to deploying.
    *   Update your local `website` repo's mixer submodule to point to your `mixer` commit.

4.  **Deploy:**
    Once the image upload (Step 2) is complete, run the deployment script **from your fork of the `website` repository**.

    To deploy to `datcom-mixer-dev` (Default):
    ```bash
    # from root of website repo
    ./scripts/deploy_mixer_cloud_deploy.sh <MIXER_IMAGE_TAG> datacommons-mixer-dev
    ```

    *For deploying to other environments (like `datcom-website-dev`), please refer to the [website developer guide](https://github.com/datacommonsorg/website/blob/master/docs/developer_guide.md#deployment).*

## Helm charts

Each folder in `helm_charts/` contains a single
[helm chart](https://helm.sh/docs/topics/charts/).

A single helm chart organizes a collection of templatized k8s yaml files.

## Installing a Helm chart

Installing a Helm chart means filling in the templates with a set of values and
applying the resources to a live cluster.

Note: Helm uses
[k8s config](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl)
for authentication. You can visit the GCP UI and click "CONNECT" on your
cluster's page to get the command to configure the k8s config.

![Alt text](gke_connect.png?raw=true "credentials")

Check if the k8s config points to the right cluster with `kubectl config current-context`.

Sample output for dev-instance:
`gke_datcom-mixer-dev-316822_us-central1_mixer-us-central1`


### Installing Reloader

[Reloader](https://github.com/stakater/Reloader) watches for changes in ConfigMaps and Secrets and then trigger rolling upgrades on associated Deployments. Mixer
is set up to use Reloader to ensure feature flag updates take effect automatically,
so long as it is installed in the same GKE cluster.

To install Reloader, run the following script with the feature flag config
directory and an optional environment. This will install Reloader on all
clusters defined in the specified feature flag configurations.

```sh
./deploy/install_reloader.sh <config_dir> [environment]
```

For example:
```sh
./deploy/install_reloader.sh deploy/featureflags dev
```
