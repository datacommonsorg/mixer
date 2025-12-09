"""Simple loadtesting using V2Observation rpc."""

from locust import HttpUser, task, between


# Simulates calls to the V2Observation rpc.
class V2ObservationCaller(HttpUser):
    # Wait 1 second between tasks.
    wait_time = between(1, 1)

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
