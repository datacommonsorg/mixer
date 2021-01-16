#!/bin/bash
# Copyright 2019 Google LLC
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


set -e

# Sparql Query
curl localhost:9090/query?sparql=SELECT%20%3Fname%20WHERE%20%7B%20%3Fstate%20typeOf%20State%20.%20%3Fstate%20dcid%20geoId%2F06%20.%20%3Fstate%20name%20%3Fname%20%7D
echo
# Property Labels
curl localhost:9090/node/property-labels?dcids=geoId/05&dcids=geoId/06
echo