"""Simple loadtesting using V2Observation rpc."""

from locust import HttpUser, events, task, between
from locust.env import Environment

_DC_API_KEY_HEADER = "X-API-Key"
_DC_API_KEY = ""

@events.test_start.add_listener
def test_start(environment: Environment, **_kwargs) -> None:
    global _DC_API_KEY
    _DC_API_KEY = environment.parsed_options.dc_api_key


@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument(
        "--dc_api_key",
        type=str,
        env_var="DC_API_KEY",
        default="",
        help="DC API key for authentication",
        is_secret=True,
    )


# Simulates calls to the V2Observation rpc.
class V2ObservationCaller(HttpUser):
    # Wait 1 second between tasks.
    wait_time = between(1, 1)

    def on_start(self):
        self.client.headers = {
            _DC_API_KEY_HEADER: _DC_API_KEY,
        }

    @task
    def v2_observation_1(self):
        self.client.post(
            '/v2/observation',
            json={
                "entity": {
                    "expression":
                        "country/USA<-containedInPlace+{typeOf:State}"
                },
                "variable": {
                    "dcids": [
                        "dummy",
                        "Median_Age_Person_AmericanIndianOrAlaskaNativeAlone"
                    ]
                },
                "select": ["entity", "variable", "value", "date"]
            })

    @task
    def v2_observation_2(self):
        self.client.post(
            '/v2/observation',
            json={
                "entity": {
                    "expression":
                        "geoId/06<-containedInPlace+{typeOf:City}"
                },
                "variable": {
                    "dcids": [
                        "dummy",
                        "Median_Age_Person_AmericanIndianOrAlaskaNativeAlone"
                    ]
                },
                "select": ["entity", "variable", "value", "date"]
            })

    @task
    def v2_observation_3(self):
        self.client.post(
            '/v2/observation',
            json={
                "entity": {
                    "expression":
                        "Earth<-containedInPlace+{typeOf:Country}"
                },
                "variable": {
                    "dcids": [
                        "Median_Age_Person"
                    ]
                },
                "select": ["entity", "variable", "value", "date"]
            })

    @task
    def v2_observation_4(self):
        self.client.post(
            '/v2/observation',
            json={
                "entity": {
                    "expression":
                        "geoId/06<-containedInPlace+{typeOf:EpaReportingFacility}"
                },
                "variable": {
                    "dcids": [
                        "Annual_Emissions_GreenhouseGas_NonBiogenic"
                    ]
                },
                "select": ["entity", "variable", "value", "date"]
            })

    @task
    def v2_observation_5(self):
        self.client.post(
            '/v2/observation',
            json={
                "entity": {
                    "expression":
                        "country/FRA<-containedInPlace+{typeOf:AdministrativeArea2}"
                },
                "variable": {
                    "dcids": [
                        "Count_Person"
                    ]
                },
                "select": ["entity", "variable", "value", "date"]
            })
