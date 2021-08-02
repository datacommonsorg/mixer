#!/usr/bin/env python

# Copyright 2020 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from locust import HttpUser, task

class MixerUser(HttpUser):
    @task
    def property_values(self):
        self.client.get(
            "/node/property-values?dcids=country/USA&dcids=geoId/06&property=name")

    @task
    def triples(self):
        self.client.get(
            "/node/triples?dcids=Class&dcids=geoId/05")

    @task
    def places_in(self):
        self.client.get(
            "/node/places-in?dcids=geoId/06&placeType=County")

    @task
    def stat_series(self):
        self.client.get(
          "/stat/series?place=geoId/06&stat_var=Count_Person_Male")

    @task
    def place_page_USA(self):
        self.client.post("/internal/place", json={"place": "country/USA"})

    @task
    def place_page_Earth(self):
        self.client.post("/internal/place", json={"place": "Earth"})