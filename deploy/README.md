# Deployment Configuration

Mixer is deployed to Kubernetes cluster in
[Google Kubernetes Engine](https://cloud.google.com/kubernetes-engine).

## Folder structure

- `storage`: BigQuery and Bigtable version config.
- `mapping`: Schema mapping files for Sparql Query.
- `helm_charts`: Helm charts configuration used for Kubernetes deployment.
- `gke`: GKE cluster config.

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

### Deploy to Mixer dev instance
 
There are two ways to deploy updates to the Mixer dev instance, depending on what you are changing.
 
#### 1. Code-Only Changes
 
If you have **only** made changes to the Mixer code (Go, Python, etc) and do **not** need to update deployment configurations (Env vars, replicas, resource limits), you can use the direct Cloud Deploy script.
 
1.  **Push Image:** Build and push your local code as a docker image to GCR (Artifact Registry)and upload the gRPC descriptor to GCS.
    ```bash
    ./scripts/push_image.sh
    ```
    *Note: This script uses `build/ci/cloudbuild.push_image.yaml`.*

    The image gets uploaded here: https://pantheon.corp.google.com/artifacts/docker/datcom-ci/us/gcr.io/datacommons-mixer?project=datcom-ci
    And the gRPC descriptor gets uploaded here: https://pantheon.corp.google.com/storage/browser/datcom-mixer-grpc/mixer-grpc?project=datcom-ci
 
2.  **Deploy:** Take the image tag from the previous step (e.g., `dev-githash`) and run the deployment script **from your fork of the `website` repository**:

  To deploy to `datcom-mixer-dev`:
    ```bash
    # from root of website repo
    ./scripts/deploy_mixer_cloud_deploy.sh <MIXER_IMAGE_TAG> datacommons-mixer-dev
```

  To deploy mixer-only changes to datcom-website-dev, use the last commit hash of the `website` repo and the image tag from the previous step:
    ```bash
    # from root of website repo
    ./scripts/deploy_website_cloud_deploy.sh <WEBSITE_GITHASH> <IMAGE_TAG> datacommons-website-dev
    ```
  * You can double check the last website image pushed to Artifact Registry here: https://pantheon.corp.google.com/artifacts/docker/datcom-ci/us/gcr.io/datacommons-website?project=datcom-ci 
 
#### 2. Configuration Changes (Full Deployment)
 
If you have modified deployment configurations (e.g., `deploy/helm_charts/values.yaml`, `deploy/helm_charts/envs/*.yaml`), you **MUST** pull these changes into the `website` repository prior to deploying to ensure they are applied correctly.
 
1.  Commit your changes to your fork of the `mixer` repo.
2.  Update your local `website` repo to point to your `mixer` commit.
3.  You still need to follow the previous steps to build and push your image (mixer repo: scripts/push_image.sh)
4. Run the same deploy script as above, but from the website repo after updating the submodule.

For more info about deployment, see the [website developer guide](https://github.com/datacommonsorg/website/blob/master/docs/developer_guide.md#deployment).

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
