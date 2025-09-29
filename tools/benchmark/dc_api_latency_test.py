"""Simple load testing tool for Data Commons API endpoints using Locust."""

import abc
from abc import ABCMeta
import json
import logging
from typing import Any

from locust import between
from locust import events
from locust import FastHttpUser
from locust import task
from locust.env import Environment

logging.basicConfig(level=logging.INFO)

_DC_API_KEY = ""
_SHARED_TEST_REQUESTS = []


def load_test_requests(requests_json_files: str) -> list:
    logging.info("Loading test requests from %s", requests_json_files)
    # Clearing previous requests. Request files can be edited in webui, so we need
    # to clear the previous requests.
    requests = []
    assert requests_json_files, "requests_json_files is empty"
    file_paths = requests_json_files.split(",")
    for file_path in file_paths:
        logging.info("Loading requests from %s", file_path)
        try:
            with open(file_path, "r") as file:
                loaded_data = json.load(file)
                for request in loaded_data:
                    if "test_name" not in request or "path" not in request:
                        raise ValueError(
                            "Each entry in the request file should have 'test_name' and"
                            f" 'path' fields. Request: {request}")
                requests.extend(loaded_data)
        except FileNotFoundError as e:
            e.add_note(f"Failed to load test requests from {file_path}")
            raise
        except json.JSONDecodeError as e:
            e.add_note(f"Failed to decode JSON from {file_path}")
            raise
    return requests


def send_request(
    client,
    api_version: str,
    request: dict[str, Any],
    skip_cache: bool = True,
    request_name: str | None = None,
) -> None:
    headers = {"X-API-Key": _DC_API_KEY}
    if skip_cache:
        headers["X-Skip-Cache"] = str(skip_cache).lower()
    request_name = request_name or f"{request['test_name']}_{api_version}"
    with client.rest(
            "POST",
            f"/{api_version}{request['path']}",
            json=request["json_payload"],
            headers=headers,
            name=request_name,
    ) as _:
        # For examples, check
        # https://docs.locust.io/en/stable/increase-performance.html#rest
        pass


# For details on locust command line parser, refer
# https://docs.locust.io/en/stable/extending-locust.html#custom-arguments
@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument(
        "--dc_api_key",
        type=str,
        env_var="DC_API_KEY",
        default="",
        help="DC API key for authentication",
        is_required=True,
        is_secret=True,
    )
    parser.add_argument(
        "--request_json_files",
        env_var="REQUEST_JSON_FILES",
        type=str,
        default="requests/node_requests.json",
        choices=[
            "requests/node_requests.json",
            "requests/node_search_requests.json",
            "requests/observation_requests.json",
        ],
        is_required=True,
        help="Comma separated list of json files containing test requests",
    )


# For deatils on locust events and lifecycle, refer
# https://github.com/locustio/locust/blob/master/examples/test_data_management.py
@events.test_start.add_listener
def test_start(environment: Environment, **_kwargs) -> None:
    global _DC_API_KEY, _SHARED_TEST_REQUESTS
    logging.info("Starting test run")
    _DC_API_KEY = environment.parsed_options.dc_api_key
    # Explicitly clearing as on load failure() below, tests continue with
    # previous requests.
    _SHARED_TEST_REQUESTS = []
    _SHARED_TEST_REQUESTS = load_test_requests(
        environment.parsed_options.request_json_files)


# Created to avoid metaclass conflict with FastHttpUser.
class UserABCMeta(type(FastHttpUser), ABCMeta):
    pass


class BaseBenchmarkUser(FastHttpUser, metaclass=UserABCMeta):
    # Wait 1 second between tasks. Also prevent tight loop if tasks are failing or
    # empty
    wait_time = between(1, 1)
    api_version: str
    # Marking abstract to prevent locust from running this class directly
    abstract = True

    def on_start(self):
        self.test_requests = _SHARED_TEST_REQUESTS

    @task
    def run_latency_tests(self):
        if self.test_requests:
            for request in self.test_requests:
                if ("api_versions" not in request or
                        not request["api_versions"] or
                        self.api_version in request["api_versions"]):
                    self.execute_specific_request(request)
        else:
            logging.warning("No test requests available for this user.")

    @abc.abstractmethod
    def execute_specific_request(self, request_data: dict[str, Any]):
        pass


class BenchmarkV2(BaseBenchmarkUser):
    api_version = "v2"

    def execute_specific_request(self, request_data: dict[str, Any]):
        send_request(self, self.api_version, request_data)


class BenchmarkV3SkipCache(BaseBenchmarkUser):
    api_version = "v3"

    def execute_specific_request(self, request_data: dict[str, Any]):
        send_request(
            self,
            self.api_version,
            request_data,
            skip_cache=True,
            request_name=(
                f"{request_data['test_name']}_{self.api_version}_skip_cache"),
        )


class BenchmarkV3WithCache(BaseBenchmarkUser):
    api_version = "v3"

    def execute_specific_request(self, request_data: dict[str, Any]):
        send_request(
            self,
            self.api_version,
            request_data,
            skip_cache=False,
            request_name=(
                f"{request_data['test_name']}_{self.api_version}_with_cache"),
        )
