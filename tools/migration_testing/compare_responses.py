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
Tests that two different API domains return the exact same responses.

Usage:
$ python tools/migration_testing/compare_responses.py mixer api.datacommons.org api2.datacommons.org $DC_API_KEY

Note that the API key provided must be valid for both domains.
"""

import argparse
import copy
import json
import logging

from mixer_api_requests import ENDPOINTS as MIXER_ENDPOINTS
from mixer_api_requests import ERROR_TESTS as MIXER_ERROR_TESTS
from nl_api_requests import ENDPOINTS as NL_ENDPOINTS
from nl_api_requests import ERROR_TESTS as NL_ERROR_TESTS
import requests

NEW_ENDPOINTS = [
    # For testing out new endpoints without having to comment out existing ones.
    # Use mode "new" to run only these.
]

# How many characters of response data to print to the console when showing
# two responses that have a diff.
MAX_OUTPUT_CHARS = 1000


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


def is_json(response):
  return "application/json" in response.headers.get("Content-Type", "")


def get_response_data(response):
  if is_json(response):
    return response.json()
  else:
    return response.content


def get_formatted_response(response):
  if is_json(response):
    formatted = json.dumps(response.json(), indent=2).replace("\n", "\n  ")
    if len(formatted) > MAX_OUTPUT_CHARS:
      formatted = formatted[:MAX_OUTPUT_CHARS] + f"...<{len(formatted) - MAX_OUTPUT_CHARS} chars omitted>"
    return formatted
  else:
    size = len(response.content)
    return f"Content-Type: {response.headers.get('Content-Type', '')}; Size: {size}"


def compare_responses(endpoint, use_api_key, method="GET", params=None):
  current_url = f"https://{args.current_domain}{endpoint}"
  new_url = f"https://{args.new_domain}{endpoint}"

  try:
    if method == "GET":
      req_params = {} if params is None else copy.deepcopy(params)
      if use_api_key:
        if args.endpoints == "nl":
          # Bard API key param name is different for legacy reasons.
          req_params['apikey'] = args.api_key
        else:
          req_params['key'] = args.api_key
      current_response = requests.get(current_url, params=req_params)
      new_response = requests.get(new_url, params=req_params)
    elif method == "POST":
      headers = {}
      query_params = {}
      if use_api_key:
        if args.endpoints == "nl":
          query_params['apikey'] = args.api_key
        else:
          headers = {'x-api-key': args.api_key}
      current_response = requests.post(current_url,
                                       json=params,
                                       headers=headers,
                                       params=query_params)
      new_response = requests.post(new_url,
                                   json=params,
                                   headers=headers,
                                   params=query_params)
    else:
      raise ValueError("Invalid method. Use 'GET' or 'POST'")

    current_data = get_response_data(current_response)
    new_data = get_response_data(new_response)

    if current_data != new_data:
      print(f"{bcolors.FAIL}DIFF{bcolors.ENDC} {method} {endpoint}")
      print(
          f"  Current ({args.current_domain}): {current_response.status_code} {get_formatted_response(current_response)}"
      )
      print(
          f"  New ({args.new_domain}): {new_response.status_code} {get_formatted_response(new_response)}"
      )
    else:
      code = current_response.status_code
      if code >= 400:
        colored_code = f"{bcolors.WARNING}{code}{bcolors.ENDC}"
      elif len(current_response.content) == 0:
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
  if args.endpoints == "new":
    endpoints = NEW_ENDPOINTS
    error_tests = []
  elif args.endpoints == "mixer":
    endpoints = MIXER_ENDPOINTS
    error_tests = MIXER_ERROR_TESTS
  elif args.endpoints == "nl":
    endpoints = NL_ENDPOINTS
    error_tests = NL_ERROR_TESTS
  else:
    raise ValueError(
        f"Invalid endpoint type. Use 'mixer', 'nl', or 'new'. Got: {args.endpoints}"
    )

  print()
  print(f"{bcolors.BOLD}With API key{bcolors.ENDC}")
  send_requests(endpoints, use_api_key=True)

  print()
  print(f"{bcolors.BOLD}Without API key{bcolors.ENDC}")
  send_requests(endpoints, use_api_key=False)

  if (len(error_tests) > 0):
    print()
    print(f"{bcolors.BOLD}Error tests{bcolors.ENDC}")
    send_requests(error_tests, use_api_key=True)


if __name__ == "__main__":
  parser = argparse.ArgumentParser(
      description="Compare API responses between two domains with an API key.")
  parser.add_argument(
      "endpoints",
      help=
      "Which set of endpoints to test. Values are 'mixer' for mixer_api_requests.py, 'nl' for nl_api_requests.py, or 'new' for the array at the top of the file."
  )
  parser.add_argument("current_domain",
                      help="The domain to use as a source of truth")
  parser.add_argument("new_domain",
                      help="The domain representing post-migration state")
  parser.add_argument("api_key", help="The API key to use for requests")
  args = parser.parse_args()

  logging.basicConfig(level=logging.WARNING)

  main()
