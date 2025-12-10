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
$ python3 tools/migration_testing/compare_responses.py mixer api.datacommons.org staging.api.datacommons.org $PROD_API_KEY $STAGING_API_KEY
$ python3 tools/migration_testing/compare_responses.py nl nl.datacommons.org staging.nl.datacommons.org $PROD_API_KEY $STAGING_API_KEY
$ python3 tools/migration_testing/compare_responses.py new <current_domain> <new_domain> <current_api_key> <new_api_key>
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
  """Check if the response content type is JSON.
  
  Args:
      response: A requests.Response object.
      
  Returns:
      bool: True if the response Content-Type header contains 'application/json', False otherwise.
  """
  return "application/json" in response.headers.get("Content-Type", "")


def get_response_data(response):
  """Extract response data in appropriate format based on content type.
  
  Args:
      response: A requests.Response object.
      
  Returns:
      dict or bytes: Parsed JSON if the response is JSON, otherwise raw response content.
  """
  if is_json(response):
    return response.json()
  else:
    return response.content


def get_formatted_response(response):
  """Format response data for console output with truncation if necessary.
  
  Args:
      response: A requests.Response object.
      
  Returns:
      str: Formatted JSON string with optional truncation, or content type and size for non-JSON.
  """
  if is_json(response):
    formatted = json.dumps(response.json(), indent=2).replace("\n", "\n  ")
    if len(formatted) > MAX_OUTPUT_CHARS:
      formatted = formatted[:MAX_OUTPUT_CHARS] + f"...<{len(formatted) - MAX_OUTPUT_CHARS} chars omitted>"
    return formatted
  else:
    size = len(response.content)
    return f"Content-Type: {response.headers.get('Content-Type', '')}; Size: {size}"


def get_response(endpoint, method, params, domain, api_key):
  """Make an HTTP request to the specified endpoint.
  
  Args:
      endpoint (str): The API endpoint path.
      method (str): HTTP method, either 'GET' or 'POST'.
      params (dict, optional): Request parameters/body. For GET requests, passed as query params.
          For POST requests, passed as JSON body.
      domain (str): The domain to send the request to.
      api_key (str): The API key for authentication.
      
  Returns:
      requests.Response: The HTTP response object.
      
  Raises:
      ValueError: If method is not 'GET' or 'POST'.
  """
  url = f"https://{domain}{endpoint}"
  if method == "GET":
    req_params = {} if params is None else copy.deepcopy(params)
    req_params['key'] = api_key
    return requests.get(url, params=req_params)
  elif method == "POST":
    headers = {'x-api-key': api_key}
    return requests.post(url, json=params, headers=headers)
  else:
    raise ValueError("Invalid method. Use 'GET' or 'POST'")


def compare_responses(endpoint, method="GET", params=None):
  """Compare API responses from current and new domains for the same endpoint.
  
  Makes requests to both the current (prod) and new (staging) domains with the same
  endpoint and parameters, then compares the responses. Prints results to console
  indicating whether responses match or differ.
  
  Args:
      endpoint (str): The API endpoint path to test.
      method (str, optional): HTTP method to use. Defaults to 'GET'.
      params (dict, optional): Request parameters. Defaults to None.
  """
  try:
    current_response = get_response(endpoint, method, params, args.current_domain, args.current_api_key)
    new_response = get_response(endpoint, method, params, args.new_domain, args.new_api_key)

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


def send_requests(endpoint_infos):
  """Send comparison requests for a collection of endpoints.
  
  Args:
      endpoint_infos (list): List of endpoint specifications. Each can be either:
          - A string (endpoint path, defaults to GET method, no params)
          - A tuple of (endpoint, methods, params) where methods is a list of HTTP methods
  """
  for endpoint_info in endpoint_infos:
    if isinstance(endpoint_info, tuple):
      endpoint, methods, params = endpoint_info
    else:
      endpoint, methods, params = endpoint_info, ["GET"], None

    for method in methods:
      compare_responses(endpoint, method, params)


def main():
  """Main entry point for the response comparison script.
  
  Loads the appropriate endpoint set based on command-line arguments and runs
  comparison tests for both normal endpoints and error test cases.
  """
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
  send_requests(endpoints)

  if (len(error_tests) > 0):
    print()
    print(f"{bcolors.BOLD}Error tests{bcolors.ENDC}")
    send_requests(error_tests)


if __name__ == "__main__":
  parser = argparse.ArgumentParser(
      description="Compare API responses between two domains with an API key.")
  parser.add_argument(
      "endpoints",
      help=
      "Which set of endpoints to test. Values are 'mixer' for mixer_api_requests.py, 'nl' for nl_api_requests.py, or 'new' for the array at the top of the file."
  )
  parser.add_argument("current_domain",
                      help="The domain to use as a source of truth (prod)")
  parser.add_argument("new_domain",
                      help="The domain representing post-migration state (staging)")
  parser.add_argument("current_api_key", help="The API key to use for requests to the current (prod) domain")
  parser.add_argument("new_api_key", help="The API key to use for requests to the new (staging) domain")
  args = parser.parse_args()

  logging.basicConfig(level=logging.WARNING)

  main()
