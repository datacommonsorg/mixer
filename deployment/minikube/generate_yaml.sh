#!/bin/bash

# TODO(shifucun): Use config.yaml instead of env.sh
. env.sh

yq merge -a=append ../template/mixer.yaml.tmpl ../template/esp.yaml.tmpl > deployment.yaml

# Mixer resource
yq w -i --style=double deployment.yaml spec.template.spec.containers[0].resources.requests.memory $MIXER_MEM_REQ
yq w -i --style=double deployment.yaml spec.template.spec.containers[0].resources.requests.cpu $MIXER_CPU_REQ
yq w -i --style=double deployment.yaml spec.template.spec.containers[0].resources.limits.memory $MIXER_MEM_LIMIT
yq w -i --style=double deployment.yaml spec.template.spec.containers[0].resources.limits.cpu $MIXER_CPU_LIMIT

# Mixer container
yq w -i deployment.yaml spec.replicas $REPLICAS
yq w -i deployment.yaml spec.template.spec.containers[0].image $MIXER_IMAGE
yq w -i deployment.yaml spec.template.spec.containers[0].imagePullPolicy $MIXER_PULL_POLICY

# Mixer argumennts
yq w -i --style=double deployment.yaml spec.template.spec.containers[0].args[1] $BQ_DATASET
yq w -i --style=double deployment.yaml spec.template.spec.containers[0].args[3] $BT_TABLE
yq w -i --style=double deployment.yaml spec.template.spec.containers[0].args[5] $BT_PROJECT
yq w -i --style=double deployment.yaml spec.template.spec.containers[0].args[7] $BT_INSTANCE
yq w -i --style=double deployment.yaml spec.template.spec.containers[0].args[9] $PROJECT_ID
yq w -i --style=double deployment.yaml spec.template.spec.containers[0].args[11] $BRANCH_FOLDER

# ESP service name
yq w -i --style=double deployment.yaml spec.template.spec.containers[1].args[1] $SERVICE_NAME

# ESP resource
yq w -i --style=double deployment.yaml spec.template.spec.containers[1].resources.requests.memory $ESP_MEM_REQ
yq w -i --style=double deployment.yaml spec.template.spec.containers[1].resources.requests.cpu $ESP_CPU_REQ
yq w -i --style=double deployment.yaml spec.template.spec.containers[1].resources.limits.memory $ESP_MEM_LIMIT
yq w -i --style=double deployment.yaml spec.template.spec.containers[1].resources.limits.cpu $ESP_CPU_LIMIT

# ESP arguments
# Need to set "non_gcp" flag to make ESP working on Minikube
yq w -i --style=double --inplace -- deployment.yaml spec.template.spec.containers[1].args[+] '--non_gcp'

# ESP service configuration
yq w --style=double ../template/endpoints.yaml.tmpl name $SERVICE_NAME > endpoints.yaml
yq w -i endpoints.yaml title $API_TITLE