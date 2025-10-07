# DC API Latency Benchmark Tool

Simple load testing tool for Data Commons API endpoints using Locust.

## Prerequisites

Install dependencies:
```bash
pip install -r requirements.txt
```

## Running Instructions

1. Start the benchmark tool:
   ```bash
   locust --config=locust.conf
   ```

2. Go to http://localhost:8089/ in browser. You should see the locust UI.

3. Click "New test", set the mandatory parameters :
   - **Host**: Set the target API endpoint (default: http://autopush.api.datacommons.org)
   - **Custom Parameters**:
     - **Dc Api Key**: Check that this is set to correct DC environment. This picks value from DC_API_KEY environment variable by default.
     - **Request File**: Select the request file to run tests (default: node_requests.json)

4. Click "Start" to begin the test and monitor results in real-time.

5. View results in the web UI or download results from the "DOWNLOAD DATA" tab.

## Configuration

Settings in `locust.conf` can be modified in the web interface:

- **Users**: Number of concurrent users (default: 10)
- **Ramp Up**: Users per second (default: 5)
- **Run Time**: Test duration (default: 5m)
- **Host**: Target API endpoint (default: http://autopush.api.datacommons.org)

## Test Types

Select from 3 benchmark user classes:

1. **BenchmarkV2**: Tests v2 API endpoints
2. **BenchmarkV3SkipCache**: Tests v3 API endpoints without cache
3. **BenchmarkV3WithCache**: Tests v3 API endpoints with cache

## Request Types

Choose from 3 types of test requests:

1. **node_requests.json**: Node-related API requests
2. **node_search_requests.json**: Node search API requests
3. **observation_requests.json**: Observation API requests

- New tests added to these files will be automatically picked up in the next run
- Each API is tested against v2 and v3 API versions by default
- To limit to specific API versions, use the `"api_versions": ["v3"]` configuration in the request file

