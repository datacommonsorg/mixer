#  Copyright 2024 Google LLC

#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at

#       https://www.apache.org/licenses/LICENSE-2.0

#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# Test cases for the Mixer API, currently hosted at
# api.datacommons.org and staging.api.datacommons.org.

import json


def read_json_from_file(file_path):
  with open(file_path, 'r') as f:
    return json.load(f)


ENDPOINTS = [
    # GetVersion
    ("/version", ["GET"], {}),
    # V2Resolve
    ("/v2/resolve", ["GET", "POST"], {
        "nodes": ["Mountain View, CA", "New York City"],
        "property": "<-description{typeOf:City}->dcid"
    }),
    # V2Event
    ("/v2/event", ["GET", "POST"], {
        "node":
            "country/USA",
        "property":
            "<-location{typeOf:FireEvent, date:2020-10, area:3.1#6.2#Acre}"
    }),
    # V2Node
    ("/v2/node", ["GET", "POST"], {
        "nodes": "geoId/06",
        "property": "<-"
    }),
    # V2Observation GET
    ("/v2/observation", ["GET"], {
        "date": "LATEST",
        "variable.dcids": ["Count_Person"],
        "entity.dcids": ["country/USA"],
        "select": ["entity", "variable", "value", "date"]
    }),
    # V2Observation POST
    ("/v2/observation", ["POST"], {
        "date": "LATEST",
        "variable": {
            "dcids": ["Count_Person"]
        },
        "entity": {
            "dcids": ["country/USA"]
        },
        "select": ["entity", "variable", "value", "date"]
    }),
    # V2Sparql
    ("/v2/sparql", ["GET", "POST"], {
        "query":
            "SELECT ?name WHERE {?biologicalSpecimen typeOf BiologicalSpecimen . ?biologicalSpecimen name ?name} ORDER BY DESC(?name) LIMIT 10"
    }),
]

ERROR_TESTS = [
    # 400
    ("/v2/observation?entity.dcids=country/USA&variable.dcids=Count_Person", ["GET", "POST"], {}),
    # 404
    ("/nonexistent", ["GET", "POST"], {}),
    # 415
    ("/v2/sparql?name=example.com&type=A", ["GET", "POST"], {}),
    # 500
    # Error due to requesting too many time series (Count_Person for cities in the US).
    ("/v2/observation?entity.expression=country/USA%3C-containedInPlace%2B%7BtypeOf%3ACity%7D&variable.dcids=Count_Person&select=entity&select=variable&select=date&select=value", ["GET", "POST"], {}),
]
