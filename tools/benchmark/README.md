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
   ./run_dc_api_latency.sh
   ```

2. Open web interface at: **http://localhost:8089**

3. Provide your DC API key:
   - Set environment variable: `export DC_API_KEY=your_key`
   - Or enter when prompted during script execution

4. Configure test parameters in the web UI and start tests

## Configuration

Settings in `locust.conf` can be modified in the web interface:

- **Users**: Number of concurrent users (default: 10)
- **Spawn Rate**: Users per second (default: 5)
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

## Usage

1. Run the script and provide your DC API key
2. Access the web UI at port 8089
3. Select your desired test type and request file
4. Configure load parameters
5. Start the test and monitor results in real-time
6. View results in the web UI or download data from the "DOWNLOAD DATA" tab
