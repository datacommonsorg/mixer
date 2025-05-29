#!/bin/bash
# Set the dc api key for the target environment
DC_API_KEY=<>

locust --config=locust.conf \
--dc_api_key=$DC_API_KEY \
--request_json_files=requests/node_requests.json
