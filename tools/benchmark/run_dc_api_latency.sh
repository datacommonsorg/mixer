#!/bin/bash
# DC API Latency Benchmark Tool
#
# Usage:
#   1. Set API key as environment variable:
#      DC_API_KEY=your_key ./run_dc_api_latency.sh
#
#   2. Or run directly (will prompt for key):
#      ./run_dc_api_latency.sh
#
# Prerequisites:
#   - Install dependencies: pip install -r requirements.txt


# Get DC API key from user input
if [ -z "$DC_API_KEY" ]; then
    read -p "Enter DC API key: " -s DC_API_KEY
    echo
fi

locust --config=locust.conf \
--dc_api_key=$DC_API_KEY \
--request_json_files=requests/node_requests.json
