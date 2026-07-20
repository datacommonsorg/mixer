CREATE TABLE Node (
  subject_id STRING(1024) NOT NULL,
  value STRING(MAX),
  bytes BYTES(MAX),
  name STRING(MAX),
  types ARRAY<STRING(1024)>,
  last_update_timestamp TIMESTAMP OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY(subject_id);

CREATE TABLE Edge (
  subject_id STRING(1024) NOT NULL,
  predicate STRING(1024) NOT NULL,
  object_id STRING(1024) NOT NULL,
  provenance STRING(1024) NOT NULL
) PRIMARY KEY(subject_id, predicate, object_id, provenance),
INTERLEAVE IN Node;

CREATE TABLE TimeSeries (
  variable_measured STRING(1024) NOT NULL,
  entity1 STRING(1024) NOT NULL AS (JSON_VALUE(entities, '$.entity1')) STORED,
  extra_entities_id STRING(1024) NOT NULL,
  facet_id STRING(1024) NOT NULL,
  entities JSON NOT NULL,
  facet JSON NOT NULL,
  entity2 STRING(1024) AS (JSON_VALUE(entities, '$.entity2')) STORED,
  entity3 STRING(1024) AS (JSON_VALUE(entities, '$.entity3')) STORED,
  observation_period STRING(1024) AS (JSON_VALUE(facet, '$.observationPeriod')) STORED,
  unit STRING(1024) AS (JSON_VALUE(facet, '$.unit')) STORED,
  measurement_method STRING(1024) AS (JSON_VALUE(facet, '$.measurementMethod')) STORED,
  provenance STRING(1024) NOT NULL AS (JSON_VALUE(facet, '$.provenance')) STORED,
  last_update_timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY(variable_measured, entity1, extra_entities_id, facet_id);

CREATE TABLE Observation (
  variable_measured STRING(1024) NOT NULL,
  entity1 STRING(1024) NOT NULL,
  extra_entities_id STRING(1024) NOT NULL,
  facet_id STRING(1024) NOT NULL,
  date STRING(32) NOT NULL,
  value STRING(MAX) NOT NULL,
  last_update_timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY(variable_measured, entity1, extra_entities_id, facet_id, date DESC),
INTERLEAVE IN PARENT TimeSeries ON DELETE CASCADE;

CREATE INDEX TimeSeriesByEntity1 ON TimeSeries(entity1);

CREATE NULL_FILTERED INDEX TimeSeriesByEntity2 ON TimeSeries(entity2);

CREATE NULL_FILTERED INDEX TimeSeriesByEntity3 ON TimeSeries(entity3, variable_measured, entity1, entity2);

CREATE INDEX TimeSeriesByProvenance ON TimeSeries(provenance);

CREATE TABLE IngestionHistory (
  WorkflowExecutionID STRING(1024) NOT NULL,
  CreationTimestamp TIMESTAMP OPTIONS (allow_commit_timestamp=true),
  CompletionTimestamp TIMESTAMP OPTIONS (allow_commit_timestamp=true),
  IngestionFailure BOOL,
  Status STRING(1024),
  Stage STRING(1024),
  DataflowJobID STRING(1024),
  IngestedImports ARRAY<STRING(MAX)>,
  ExecutionTime INT64,
  NodeCount INT64,
  EdgeCount INT64,
  ObservationCount INT64
) PRIMARY KEY(WorkflowExecutionID);

CREATE PROPERTY GRAPH DCGraph
  NODE TABLES(
    Node
      KEY(subject_id)
      LABEL Node PROPERTIES(
        bytes,
        name,
        subject_id,
        types,
        value)
  )
  EDGE TABLES(
    Edge
      KEY(subject_id, predicate, object_id, provenance)
      SOURCE KEY(subject_id) REFERENCES Node(subject_id)
      DESTINATION KEY(object_id) REFERENCES Node(subject_id)
      LABEL Edge PROPERTIES(
        object_id,
        predicate,
        provenance,
        subject_id)
  );
