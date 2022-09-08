# Helm charts

Each folder of this directory contains a single [helm chart](https://helm.sh/docs/topics/charts/).

A single helm chart organizes a collection of templatized k8s yaml files.

## Installing a Helm chart

Installing a Helm chart means filling in the templates with a set of values and 
applying the resources to a live cluster.

Note: Helm uses [k8s config](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl) for authentication. You can visit the GCP UI and click "CONNECT" on your cluster's page to get the command to configure the k8s config.

![Alt text](images/gke_connect.png?raw=true "credentials")

Check if the k8s config points to the right cluster with `kubectl config current-context`.

Sample output for dev-instance: `gke_datcom-mixer-dev-316822_us-central1_mixer-us-central1`

### Example 1: Install/update Mixer dev instance using local mixer

Run the following after changes are made locally and are committed. push_binary.sh creates a new Mixer image
based on local change. Helm then deploys a release that refers to the newly created image.

```sh
./scripts/push_binary.sh

helm upgrade --install mixer-dev deploy/helm_charts/mixer \
    --atomic \
    -f deploy/helm_charts/envs/mixer_dev.yaml \
    --set mixer.githash=$(git rev-parse --short=7 HEAD) \
    --set-file mixer.schemaConfigs."base.mcf"=deploy/mapping/base.mcf \
    --set-file mixer.schemaConfigs."encode.mcf"=deploy/mapping/encode.mcf \
    --set-file mixer.schemaConfigs."dailyweather.mcf"=deploy/mapping/dailyweather.mcf \
    --set-file mixer.schemaConfigs."monthlyweather.mcf"=deploy/mapping/monthlyweather.mcf \
    --set-file kgStoreConfig.bigqueryVersion=deploy/storage/bigquery.version \
    --set-file kgStoreConfig.bigtableImportGroupsVersion=deploy/storage/bigtable_import_groups.version
```
