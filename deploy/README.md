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
    ./scripts/push_image.sh datcom-ci DEV
    ```
    *Note: This script uses `build/ci/cloudbuild.manual_push_image.yaml`.*

    * The image gets uploaded here: https://pantheon.corp.google.com/artifacts/docker/datcom-ci/us/gcr.io/datacommons-mixer?project=datcom-ci
    * And the gRPC descriptor gets uploaded here: https://pantheon.corp.google.com/storage/browser/datcom-mixer-grpc/mixer-grpc?project=datcom-ci

3.  **(Optional) Update Deployment Config:**
    *   **If you have modified standalone Mixer or base chart configurations** (e.g., `deploy/helm_charts/mixer/values.yaml`, `deploy/helm_charts/envs/mixer_*.yaml`), you **MUST** pull these changes into the `website` repository prior to deploying.
    *   Update your local `website` repo's mixer submodule to point to your `mixer` commit.
    *   Note: Per-environment overrides for website's bundled Mixer live directly in `datacommonsorg/website` at `deploy/helm_charts/envs/website_mixer_*.yaml` (not in `deploy/helm_charts/envs/mixer_*.yaml`).

4.  **Deploy:**
    Once the image upload (Step 2) is complete, run the deployment script **from your fork of the `website` repository**.

    To deploy to `datcom-mixer-dev` (Default):
    ```bash
    # from root of website repo
    ./scripts/deploy_mixer_cloud_deploy.sh <MIXER_IMAGE_TAG> datacommons-mixer-dev
    ```

    *For deploying to other environments (like `datcom-website-dev`), please refer to the [website developer guide](https://github.com/datacommonsorg/website/blob/master/docs/developer_guide.md#deployment).*

## Helm charts

`deploy/helm_charts/mixer` contains the [Helm chart](https://helm.sh/docs/topics/charts/) used by both standalone Mixer (`*.api.datacommons.org`) and website's bundled Mixer (`*datacommons.org`). Each deployment layers a single per-environment YAML file directly over `deploy/helm_charts/mixer/values.yaml`:

| Deployment | Base Defaults | Per-Environment Overrides |
| :--- | :--- | :--- |
| Standalone Mixer (`mixer-*`) | `deploy/helm_charts/mixer/values.yaml` | `deploy/helm_charts/envs/mixer_<env>.yaml` (this repo) |
| Website's Bundled Mixer (`dc-mixer`) | `deploy/helm_charts/mixer/values.yaml` | `deploy/helm_charts/envs/website_mixer_<env>.yaml` (`datacommonsorg/website`) |

Overrides in `deploy/helm_charts/envs/mixer_*.yaml` apply to standalone Mixer only and are not inherited by website's bundled Mixer. When changing a setting that should apply to both deployments, update both files.

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
