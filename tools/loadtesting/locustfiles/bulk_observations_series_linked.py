"""Simple loadtesting using BulkObservationsSeriesLinked rpc."""

from locust import HttpUser, task, between


# Simulates calls to the BulkObservationsSeriesLinked rpc.
class BulkObservationsSeriesLinkedCaller(HttpUser):
    # Wait 1 second between tasks.
    wait_time = between(1, 1)

    @task
    def bulk_observations_series_linked_1(self):
        self.client.post(
            '/v1/bulk/observations/series/linked',
            json={
                "entity_type":
                    "County",
                "linked_entity":
                    "country/USA",
                "linked_property":
                    "containedInPlace",
                "variables": [
                    "dummy",
                    "Median_Age_Person_AmericanIndianOrAlaskaNativeAlone"
                ]
            })

    @task
    def bulk_observations_series_linked_2(self):
        self.client.post(
            '/v1/bulk/observations/series/linked',
            json={
                "entity_type":
                    "City",
                "linked_entity":
                    "country/USA",
                "linked_property":
                    "containedInPlace",
                "variables": [
                    "Median_Age_Person_AmericanIndianOrAlaskaNativeAlone"
                ]
            })

    @task
    def bulk_observations_series_linked_3(self):
        self.client.post(
            '/v1/bulk/observations/series/linked',
            json={
                "entity_type":
                    "City",
                "linked_entity":
                    "country/USA",
                "linked_property":
                    "containedInPlace",
                "variables": [
                    "Median_Age_Person_AmericanIndianOrAlaskaNativeAlone"
                ]
            })

    @task
    def bulk_observations_series_linked_4(self):
        self.client.post('/v1/bulk/observations/series/linked',
                         json={
                             "entity_type": "State",
                             "linked_entity": "country/USA",
                             "linked_property": "containedInPlace",
                             "variables": ["Count_Person_FoodInsecure"]
                         })

    @task
    def bulk_observations_series_linked_5(self):
        self.client.post('/v1/bulk/observations/series/linked',
                         json={
                             "entity_type": "Country",
                             "linked_entity": "Earth",
                             "linked_property": "containedInPlace",
                             "variables": ["Median_Age_Person"]
                         })

    @task
    def bulk_observations_series_linked_6(self):
        self.client.post(
            '/v1/bulk/observations/series/linked',
            json={
                "entity_type": "EpaReportingFacility",
                "linked_entity": "geoId/06",
                "linked_property": "containedInPlace",
                "variables": ["Annual_Emissions_GreenhouseGas_NonBiogenic"]
            })

    @task
    def bulk_observations_series_linked_7(self):
        self.client.post('/v1/bulk/observations/series/linked',
                         json={
                             "entity_type": "AdministrativeArea2",
                             "linked_entity": "country/FRA",
                             "linked_property": "containedInPlace",
                             "variables": ["Count_Person"]
                         })
