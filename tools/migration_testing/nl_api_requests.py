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

# Test cases for the NL API, currently hosted at
# bard.datacommons.org, staging.bard.datacommons.org,
# nl.datacommons.org, and staging.nl.datacommons.org

ENDPOINTS = [
    ("/version", ["GET"], {}),
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
    ("/nodejs/chart", ["GET"], {
        "props":
            "eJxVjsEKwjAQRH%2Bl7LngvTcpHgSRQK0XEQnpKoF1E5KtUEL%2F3Q09iMeZeTNMAWRHIeNkyDq8LBGhA2gBP8hS5RDRQcczUQuxMpq7MLOkZTcOe0WzWLnatIG3AmzfFTIhzmTFB%2F4xave1%2BzCYsgbrvQXxhH3gp39BV1QJ%2Fbcbz83IXnBqBl3BrHOy%2FTwdzwdY1y%2BC%2FEQN"
    }),
]

ERROR_TESTS = [
    # 404
    ("/nonexistent", ["GET"], {}),
]
