# Spanner Latency Test Tool

This CLI tool times key Spanner client methods in Data Commons Mixer against a
Spanner database.

## Objective
To measure Spanner query latency for a single client method, print the SQL it
executes, and help identify performance bottlenecks.

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

#### 4. Test GetStatVarGroupNode
```bash
go run tools/perf_test/main.go -method=GetStatVarGroupNode -nodes=dc/g/Agriculture,dc/g/SDG
```

#### 5. Test GetFilteredStatVarGroupNode
```bash
go run tools/perf_test/main.go -method=GetFilteredStatVarGroupNode -nodes=dc/g/Environment,dc/g/Agriculture -constrained_entities=country/USA,country/IND,country/CAN -num_entities_existence=2
```

#### 6. Test GetFilteredTopic
```bash
go run tools/perf_test/main.go -method=GetFilteredTopic -nodes=dc/topic/Demographics,dc/topic/Economy -constrained_entities=dc/s/WorldBank
```

#### 7. Repeat a method and summarize latency without SQL logs
```bash
go run tools/perf_test/main.go -method=GetObservations -variables=Count_Person -entities=geoId/06 -date=LATEST -iterations=10 -log_sql=false
```

## Flags
- `-method`: The method to test (e.g., `GetObservations`, `CheckVariableExistence`, `GetObservationsContainedInPlace`, `GetStatVarGroupNode`, `GetFilteredStatVarGroupNode`, `GetFilteredTopic`).
- `-variables`: Comma-separated list of variable DCIDs. Add a fake variable (e.g., `Fake_$RANDOM`) to bypass Spanner query cache.
- `-entities`: Comma-separated list of entity DCIDs.
- `-nodes`: Comma-separated list of StatVarGroup or Topic nodes.
- `-constrained_entities`: Comma-separated list of constrained entities for filtered variable group info methods. `dc/s/...` and `dc/d/...` constraints are treated as source/import constraints; all others are treated as place constraints.
- `-num_entities_existence`: Minimum number of constrained entities that must have observations.
- `-include_definitions`: Include definitions for StatVarGroup node methods.
- `-ancestor`: Ancestor place DCID for contained-in queries.
- `-child_type`: Child place type for contained-in queries.
- `-date`: Optional date filter (`LATEST` or a specific date).
- `-iterations`: Number of sequential runs (default `1`).
- `-log_sql`: Print each executed SQL statement with its Spanner execution time (default `true`).
- `-config`: Path to Spanner graph info YAML (defaults to `deploy/storage/spanner_graph_info.yaml`).

## Notes
- The tool directly calls Spanner client methods; it does not go through Mixer API cache.
- It prints the wall-clock time of each run and a min/median/mean/max summary. The first run includes session setup, so use several iterations and compare medians.
- With `-log_sql`, the tool injects `X-Log-SQL=true`, and the Spanner query logger prints each executed query and its execution time.
