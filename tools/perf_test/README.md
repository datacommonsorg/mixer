# Performance Test Tool

This CLI tool is designed to compare the performance of the legacy and normalized Spanner schemas for key observation APIs in Data Commons Mixer.

## Objective
To measure and compare the execution time of queries in both schemas in parallel, helping to identify performance bottlenecks.

## How to Run

### Examples

#### 1. Test GetObservations
```bash
go run tools/perf_test/main.go -method=GetObservations -variables=Count_Person,Count_Household,Fake_54321 -entities=geoId/06,geoId/08
```

#### 2. Test CheckVariableExistence
```bash
go run tools/perf_test/main.go -method=CheckVariableExistence -variables=Count_Person,Count_Household,Fake_24680 -entities=geoId/06,geoId/08
```

#### 3. Test GetObservationsContainedInPlace
```bash
go run tools/perf_test/main.go -method=GetObservationsContainedInPlace -variables=Count_Person,Count_Household,Fake_13579 -ancestor=geoId/10 -child_type=County
```

## Flags
- `-method`: The method to test (e.g., `GetObservations`, `CheckVariableExistence`, `GetObservationsContainedInPlace`).
- `-variables`: Comma-separated list of variable DCIDs. Add a fake variable (e.g., `Fake_$RANDOM`) to bypass Spanner query cache.
- `-entities`: Comma-separated list of entity DCIDs.
- `-ancestor`: Ancestor place DCID for contained-in queries.
- `-child_type`: Child place type for contained-in queries.
- `-config`: Path to Spanner graph info YAML (defaults to `deploy/storage/spanner_graph_info.yaml`).

## Notes
- The tool automatically injects headers to bypass Mixer cache and force logging of the query string.
- It runs both schemas in parallel by default and reports execution times for each.
