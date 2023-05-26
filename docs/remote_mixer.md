# Develop and Debug remote mixer

## Start "remote" mixer

Start mixer with base BT only on port "23456"

```bash
go run cmd/main.go     \
--port=23456 \
--host_project=datcom-mixer-dev-316822   \
--bq_dataset=$(head -1 deploy/storage/bigquery.version)  \
--base_bigtable_info="$(cat deploy/storage/base_bigtable_info.yaml)"  \
--schema_path=$PWD/deploy/mapping/  \
--use_branch_bigtable=false   \
--use_base_bigtable=true
```

Update `esp/envoy-config.yaml`, specifically:

* Change restlistener.socket_address.port_value from '8081' to '9091'
* Change clusters.endpoint.port_value from '12345' to '23456'

Start envoy for remote mixer

```bash
envoy --config-path esp/envoy-config.yaml
```

Revert the change in `esp/envoy-config.yaml`

## Start "local" mixer

Start mixer with custom BT and remote mixer (on port 9091)

```bash
go run cmd/main.go     \
--host_project=datcom-mixer-dev-316822   \
--bq_dataset=$(head -1 deploy/storage/bigquery.version)  \
--base_bigtable_info="$(cat deploy/storage/base_bigtable_info.yaml)"  \
--custom_bigtable_info="$(cat test/custom_bigtable_info.yaml)"  \
--schema_path=$PWD/deploy/mapping/  \
--use_branch_bigtable=false   \
--use_base_bigtable=false  \
--remote_mixer_domain=https://autopush.api.datacommons.org \
--use_custom_bigtable=true \
--fold_remote_root_svg=true
```

Start envoy for remote mixer

```bash
envoy --config-path esp/envoy-config.yaml
```
