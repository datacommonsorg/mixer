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
"""
Tests that two different Mixer API domains return the exact same responses.

Usage:
$ python tools/migration_testing/compare_responses.py api.datacommons.org api2.datacommons.org $DC_API_KEY

Note that the API key provided must be valid for both domains.
"""

import argparse
import copy
import logging

import requests

USE_NEW_ENDPOINTS_ONLY = False
NEW_ENDPOINTS = [
    # For testing out new endpoints without having to comment out existing ones
]

ENDPOINTS = [
    # GetStats
    ("/bulk/stats", ["GET", "POST"], {
        "place": ["geoId/05", "geoId/06085"],
        "stats_var": "Count_Person_Male"
    }),
    # GetBioPageData
    ("/internal/bio", ["GET", "POST"], {
        "dcid": "bio/FGFR1_HUMAN"
    }),
    # GetPlacesIn
    ("/node/places-in", ["GET", "POST"], {
        "dcids": ["geoId/10"],
        "placeType": "County"
    }),
    # GetPropertyLabels
    ("/node/property-labels", ["GET", "POST"], {
        "dcids": ["geoId/5508"]
    }),
    # GetPropertyValues
    ("/node/property-values", ["GET", "POST"], {
        "dcids": ["country/CIV"],
        "property": "name"
    }),
    # GetTriples
    ("/node/triples", ["GET", "POST"], {
        "dcids": ["SquareMeter1238495"]
    }),
    # GetPlaceStatVars
    ("/place/stat-vars", ["GET", "POST"], {
        "dcids": ["country/PLW"]
    }),
    # GetPlaceStatDateWithinPlace
    ("/place/stat/date/within-place", ["GET", "POST"], {
        "ancestor_place": "country/USA",
        "place_type": "State",
        "stat_vars": ["Count_Farm", "Count_Teacher"]
    }),
    # Query
    ("/query", ["GET", "POST"], {
        "sparql":
            "SELECT ?name \
    WHERE { \
    ?state typeOf State . \
    ?state dcid geoId/06 . \
    ?state name ?name \
    }"
    }),
    # Search
    ("/search", ["GET"], {
        "query": "Native Hawaiian Farmers"
    }),
    # GetStatAll
    ("/stat/all", ["GET", "POST"], {
        "places": ["geoId/05", "geoId/06085"],
        "stat_vars": ["Count_Person_Male", "Count_Person_Female"]
    }),
    # GetStatSeries
    ("/stat/series", ["GET", "POST"], {
        "place": "geoId/51",
        "stat_var": "Annual_Consumption_Coal_ElectricPower"
    }),
    # GetStatValue
    ("/stat/value", ["GET", "POST"], {
        "place":
            "country/GMB",
        "stat_var":
            "Amount_EconomicActivity_ExpenditureActivity_EducationExpenditure_Government_AsFractionOf_Amount_EconomicActivity_GrossDomesticProduction_Nominal"
    }),
    # Translate
    # EXCLUDED because it's very slow and has no usage in the past 90 days
    # GetLocationsRankings
    ("/v1/place/ranking", ["GET", "POST"], {
        "stat_var_dcids": ["Count_Person"],
        "place_type": "Country"
    }),
    # GetRelatedLocations
    ("/v1/place/related", ["GET", "POST"], {
        "dcid":
            "geoId/06085",
        "stat_var_dcids": [
            "Count_Person", "Median_Income_Person", "Median_Age_Person",
            "UnemploymentRate_Person"
        ]
    }),
    # GetEntityStatVarsUnionV1
    ("/v1/place/stat-vars/union", ["GET", "POST"], {
        "dcids": ["geoId/06", "geoId/36"],
        "stat_vars": ["Count_Farm_ReportedIncome",]
    }),
    # ResolveEntities
    ("/v1/recon/entity/resolve", ["POST"], {
        "entities": [{
            "source_id": "newId/SunnyvaleId",
            "entity_ids": {
                "ids": [{
                    "prop": "geoId",
                    "val": "0677000"
                }, {
                    "prop": "wikidataId",
                    "val": "Q110739"
                }]
            }
        }]
    }),
    # ResolveCoordinates
    ("/v1/recon/resolve/coordinate", ["POST"], {
        "coordinates": {
            "latitude": 56,
            "longitude": -109
        }
    }),
    # ResolveIds
    ("/v1/recon/resolve/id", ["POST"], {
        "ids": ["ChIJ2WrMN9MDDUsRpY9Doiq3aJk"],
        "in_prop": "placeId",
        "out_prop": "dcid"
    }),
    # GetStatDateWithinPlace
    ("/v1/stat/date/within-place", ["GET", "POST"], {
        "ancestor_place": "country/USA",
        "child_place_type": "State",
        "stat_vars": ["Count_Farm", "Count_Teacher"]
    }),
    # SearchStatVar
    ("/v1/variable/search", ["GET", "POST"], {
        "query": "peanuts"
    }),
    # V2Resolve
    ("/v2/resolve", ["GET", "POST"], {
        "nodes": ["Mountain View, CA", "New York City"],
        "property": "<-description{typeOf:City}->dcid"
    }),
    # GetVersion
    ("/version", ["GET"], {}),
    # # GetImportTableData
    # Commented out due to b/365951885
    # ("/internal/import-table", ["GET", "POST"], {}),
    # GetPlaceStatsVar
    ("/place/stats-var", ["GET", "POST"], {
        "dcids": ["country/PLW"]
    }),
    # UpdateCache
    ("/update-cache", ["POST"], {}),
    # BulkFindEntities
    ("/v1/bulk/find/entities", ["POST"], {
        "entities": [{
            "description": "Georgia"
        }, {
            "description": "Georgia",
            "type": "Country"
        }]
    }),
    # BulkPlaceInfo
    ("/v1/bulk/info/place", ["GET", "POST"], {
        "nodes": ["geoId/06", "geoId/02"]
    }),
    # BulkVariableInfo
    ("/v1/bulk/info/variable", ["GET", "POST"], {
        "nodes": ["Count_Farm", "Count_Teacher"]
    }),
    # BulkVariableGroupInfo
    ("/v1/bulk/info/variable-group", ["GET", "POST"], {
        "nodes": [
            "dc/g/Person_Gender-Female",
            "dc/g/Person_Age_Gender-Female",
        ]
    }),
    # BulkObservationDatesLinked
    ("/v1/bulk/observation-dates/linked", ["GET", "POST"], {
        "linked_property": "containedInPlace",
        "linked_entity": "country/USA",
        "entity_type": "State",
        "variables": "Count_Person"
    }),
    # BulkObservationExistence
    ("/v1/bulk/observation-existence", ["GET", "POST"], {
        "variables": "Count_Person",
        "entities": "country/USA"
    }),
    # BulkObservationsPoint
    ("/v1/bulk/observations/point", ["GET", "POST"], {
        "variables": ["Count_Person"],
        "entities": ["country/USA"]
    }),
    # BulkObservationsPointLinked
    ("/v1/bulk/observations/point/linked", ["GET", "POST"], {
        "linked_property": "containedInPlace",
        "linked_entity": "country/USA",
        "entity_type": "State",
        "variables": "Count_Person"
    }),
    # BulkObservationsSeries
    ("/v1/bulk/observations/series", ["GET", "POST"], {
        "variables": ["Count_Person"],
        "entities": ["country/USA"]
    }),
    # BulkObservationsSeriesLinked
    ("/v1/bulk/observations/series/linked", ["GET", "POST"], {
        "linked_property": "containedInPlace",
        "linked_entity": "country/USA",
        "entity_type": "State",
        "variables": "Count_Person"
    }),
    # BulkProperties
    ("/v1/bulk/properties/out", ["GET", "POST"], {
        "nodes": ["country/USA"]
    }),
    # BulkPropertyValues
    ("/v1/bulk/property/values/out", ["GET", "POST"], {
        "nodes": [
            "wikidataId/Q27119", "wikidataId/Q27116", "wikidataId/Q21181"
        ],
        "property": "name"
    }),
    # BulkLinkedPropertyValues
    ("/v1/bulk/property/values/in/linked", ["GET", "POST"], {
        "nodes": ["country/USA", "country/IND"],
        "property": "containedInPlace",
        "value_node_type": "State"
    }),
    # BulkTriples
    ("/v1/bulk/triples/out", ["GET", "POST"], {
        "nodes": ["CarbonDioxide", "Methane"]
    }),
    # BulkVariables
    ("/v1/bulk/variables", ["GET", "POST"], {
        "entities": ["wikidataId/Q1490", "wikidataId/Q8684"]
    }),
    # EventCollection
    ("/v1/events", ["GET", "POST"], {
        "event_type": "CycloneEvent",
        "affected_place_dcid": "Earth",
        "date": "2020-01"
    }),
    # EventCollectionDate
    ("/v1/events/dates", ["GET", "POST"], {
        "event_type": "CycloneEvent",
        "affected_place_dcid": "geoId/12"
    }),
    # FindEntities
    ("/v1/find/entities", ["GET"], {
        "description": "Georgia"
    }),
    # PlaceInfo
    ("/v1/info/place/geoId/3651000", ["GET"], {}),
    # VariableGroupInfo
    ("/v1/info/variable-group/dc/g/Person_Gender-Female", ["GET"], {}),
    # VariableInfo
    ("/v1/info/variable/Count_Farm", ["GET"], {}),
    # BioPage
    ("/v1/internal/page/bio/bio/FGFR1_HUMAN", ["GET"], {}),
    # PlacePage GET
    ("/v1/internal/page/place/country/USA", ["GET"], {
        "category": "Country"
    }),
    # PlacePage POST
    ("/v1/internal/page/place", ["POST"], {
        "node": "country/USA",
        "category": "Country"
    }),
    # ObservationsPoint
    ("/v1/observations/point/geoId/06/Annual_Generation_Electricity", ["GET"], {
        "date": "2018"
    }),
    # ObservationsSeries
    ("/v1/observations/series/wikidataId/Q987/Mean_Rainfall", ["GET"], {}),
    # DerivedObservationsSeries
    ("/v1/observations/series/derived", ["GET", "POST"], {
        "entity": "geoId/06",
        "formula": "Count_Person - Count_Person_Female - Count_Person_Male"
    }),
    # Properties
    ("/v1/properties/out/wikidataId/Q27119", ["GET"], {}),
    # PropertyValues
    ("/v1/property/values/out/geoId/sch3620580/name", ["GET"], {}),
    # LinkedPropertyValues
    ("/v1/property/values/in/linked/country/IND/containedInPlace", ["GET"], {
        "value_node_type": "State"
    }),
    # QueryV1
    ("/v1/query", ["GET", "POST"], {
        "sparql":
            "SELECT ?name \
    WHERE { \
    ?biologicalSpecimen typeOf BiologicalSpecimen . \
    ?biologicalSpecimen name ?name \
    } \
    ORDER BY DESC(?name) \
    LIMIT 10"
    }),
    # RecognizeEntities
    ("/v1/recognize/entities", ["GET", "POST"], {
        "queries": ["the birds in San Jose are chirpy"]
    }),
    # RecognizePlaces
    ("/v1/recognize/places", ["GET", "POST"], {
        "queries": ["the birds in San Jose are chirpy"]
    }),
    # Triples
    ("/v1/triples/out/Count_Person", ["GET"], {}),
    # VariableAncestors GET
    ("/v1/variable/ancestors/WithdrawalRate_Water_Irrigation", ["GET"], {}),
    # VariableAncestors POST
    ("/v1/variable/ancestors", ["POST"], {
        "node": "WithdrawalRate_Water_Irrigation"
    }),
    # Variables
    ("/v1/variables/wikidataId/Q30988", ["GET"], {}),
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
    ("/stat/series?place=&stat_var=Count_Person", ["GET", "POST"], {}),
    # 404
    ("/nonexistent", ["GET", "POST"], {}),
    # 405
    ("/v1/recon/resolve/id", ["GET"], {}),
    # 415
    ("/query?name=example.com&type=A", ["GET"], {}),
    # 500
    ("/v1/recon/resolve/id", ["POST"], {}),
]


class bcolors:
  HEADER = '\033[95m'
  OKBLUE = '\033[94m'
  OKCYAN = '\033[96m'
  OKGREEN = '\033[92m'
  WARNING = '\033[93m'
  FAIL = '\033[91m'
  ENDC = '\033[0m'
  BOLD = '\033[1m'
  UNDERLINE = '\033[4m'


def compare_responses(endpoint, use_api_key, method="GET", params=None):
  current_url = f"https://{args.current_domain}{endpoint}"
  new_url = f"https://{args.new_domain}{endpoint}"

  try:
    if method == "GET":
      req_params = {} if params is None else copy.deepcopy(params)
      if use_api_key:
        req_params['key'] = args.api_key  # Add API key to query params
      current_response = requests.get(current_url, params=req_params)
      new_response = requests.get(new_url, params=req_params)
    elif method == "POST":
      headers = {}
      if use_api_key:
        headers = {'x-api-key': args.api_key}
      current_response = requests.post(current_url,
                                       json=params,
                                       headers=headers)
      new_response = requests.post(new_url, json=params, headers=headers)
    else:
      raise ValueError("Invalid method. Use 'GET' or 'POST'")

    try:
      current_data = current_response.json()
    except requests.exceptions.JSONDecodeError:
      current_data = "Not valid JSON"
    try:
      new_data = new_response.json()
    except requests.exceptions.JSONDecodeError:
      new_data = "Not valid JSON"

    if current_data != new_data:
      print(f"{bcolors.FAIL}DIFF{bcolors.ENDC} {method} {endpoint}")
      print(
          f"  Current ({args.current_domain}): {current_response.status_code} {current_data}"
      )
      print(f"  New ({args.new_domain}): {new_response.status_code} {new_data}")
    else:
      code = current_response.status_code
      if code >= 400:
        colored_code = f"{bcolors.WARNING}{code}{bcolors.ENDC}"
      elif len(current_response.json()) == 0:
        colored_code = f"{bcolors.WARNING}{code} EMPTY{bcolors.ENDC}"
      else:
        colored_code = code
      print(
          f"{bcolors.OKGREEN}SAME{bcolors.ENDC} {colored_code} {method} {endpoint}"
      )
      logging.info(f"Reason: {current_response.reason}")
      if (code < 400):
        logging.debug(current_data)

  except requests.exceptions.RequestException as e:
    print(f"Error fetching {endpoint} ({method}): {e}")


def send_requests(endpoint_infos, use_api_key):
  for endpoint_info in endpoint_infos:
    if isinstance(endpoint_info, tuple):
      endpoint, methods, params = endpoint_info
    else:
      endpoint, methods, params = endpoint_info, ["GET"], None

    for method in methods:
      compare_responses(endpoint, use_api_key, method, params)


def main():
  to_use = NEW_ENDPOINTS if USE_NEW_ENDPOINTS_ONLY else ENDPOINTS

  print()
  print(f"{bcolors.BOLD}With API key{bcolors.ENDC}")
  send_requests(to_use, use_api_key=True)

  print()
  print(f"{bcolors.BOLD}Without API key{bcolors.ENDC}")
  send_requests(to_use, use_api_key=False)

  if not USE_NEW_ENDPOINTS_ONLY:
    print()
    print(f"{bcolors.BOLD}Error tests{bcolors.ENDC}")
    send_requests(ERROR_TESTS, use_api_key=True)


if __name__ == "__main__":
  parser = argparse.ArgumentParser(
      description="Compare API responses between two domains with an API key.")
  parser.add_argument("current_domain",
                      help="The domain to use as a source of truth")
  parser.add_argument("new_domain",
                      help="The domain representing post-migration state")
  parser.add_argument("api_key", help="The API key to use for requests")
  args = parser.parse_args()

  logging.basicConfig(level=logging.WARNING)

  main()
