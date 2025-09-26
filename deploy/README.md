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

### Install/update Mixer dev instance

First, commit your changes locally so they have an associated commit hash.

After committing, run the deploy script specifying the dev env:

```sh
./scripts/deploy_gke.sh mixer_dev
```

To deploy a specific non-HEAD commit, you can pass the commit hash as a second argument:

```sh
./scripts/deploy_gke.sh mixer_dev <commit_hash>
```

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
