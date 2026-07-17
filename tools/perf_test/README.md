# Performance Test Tool

This CLI tool is designed to compare the performance of the legacy and multi-entity Spanner schemas for key Spanner client methods in Data Commons Mixer.

## Objective
To measure and compare Spanner query execution time in both schemas in parallel, helping to identify performance bottlenecks.

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
- `-config`: Path to Spanner graph info YAML (defaults to `deploy/storage/spanner_graph_info.yaml`).

## Notes
- The tool directly calls Spanner client methods; it does not go through Mixer API cache.
- The tool injects `X-Log-SQL=true` for both schemas and `X-Use-Multi-Entity-Schema=true` for the multi-entity schema leg.
- It runs both schemas in parallel by default. Query execution times are printed by the Spanner query logger for each executed query, not aggregated by this CLI.
