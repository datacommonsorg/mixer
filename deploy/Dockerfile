# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Build docker image used in Cloud Build for deployment.
# This image contains "gcloud", "kubectl", "yq", "kustomize", "helm"

FROM google/cloud-sdk:slim

RUN \
    apt-get install kubectl && \
    curl -s "https://raw.githubusercontent.com/kubernetes-sigs/kustomize/master/hack/install_kustomize.sh"  | bash && \
    mv kustomize /bin/ && \
    curl -L https://github.com/mikefarah/yq/releases/download/v4.27.3/yq_linux_386 -o /usr/bin/yq && \
    chmod +x /usr/bin/yq && \
    curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 && \
    chmod 700 get_helm.sh && \
    ./get_helm.sh && \
    mv /usr/local/bin/helm /usr/bin/helm && \
    chmod +x /usr/bin/helm