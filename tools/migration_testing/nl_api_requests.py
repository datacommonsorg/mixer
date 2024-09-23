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

ENDPOINTS = [
    ("/nodejs/query", ["GET"], {}),
    ("/nodejs/query", ["GET"], {
        "q": "family earnings in north dakota",
    }),
    ("/nodejs/query", ["GET"], {
        "q": "family earnings in north dakota",
        "allCharts": "1",
    }),
    ("/nodejs/query", ["GET"], {
        "q": "obesity vs. poverty in counties of california",
        "client": "dc",
    }),
]

ERROR_TESTS = [
    # 404
    ("/nonexistent", ["GET"], {}),
]
