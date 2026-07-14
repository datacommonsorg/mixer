INSERT INTO Node (subject_id, value)
VALUES
  ('Count_Household', 'Count_Household'),
  ('Count_Migration', 'Count_Migration'),
  ('Count_MigrationByObservationAbout', 'Count_MigrationByObservationAbout'),
  ('Count_MigrationByTransportMode', 'Count_MigrationByTransportMode'),
  ('Count_Person', 'Count_Person'),
  ('Count_Refugee', 'Count_Refugee'),
  ('destinationCountry', 'destinationCountry'),
  ('observationAbout', 'observationAbout'),
  ('sourceCountry', 'sourceCountry'),
  ('transportMode', 'transportMode');

INSERT INTO Edge (subject_id, predicate, object_id, provenance)
VALUES
  ('Count_Migration', 'observationProperties', 'sourceCountry', 'dc/base/HumanReadableStatVars'),
  ('Count_Migration', 'observationProperties', 'destinationCountry', 'dc/base/HumanReadableStatVars'),
  ('Count_Refugee', 'observationProperties', 'destinationCountry', 'dc/base/HumanReadableStatVars'),
  ('Count_Refugee', 'observationProperties', 'sourceCountry', 'dc/base/HumanReadableStatVars'),
  ('Count_MigrationByTransportMode', 'observationProperties', 'transportMode', 'dc/base/HumanReadableStatVars'),
  ('Count_MigrationByTransportMode', 'observationProperties', 'destinationCountry', 'dc/base/HumanReadableStatVars'),
  ('Count_MigrationByTransportMode', 'observationProperties', 'sourceCountry', 'dc/base/HumanReadableStatVars'),
  ('Count_MigrationByObservationAbout', 'observationProperties', 'sourceCountry', 'dc/base/HumanReadableStatVars'),
  ('Count_MigrationByObservationAbout', 'observationProperties', 'observationAbout', 'dc/base/HumanReadableStatVars'),
  ('Count_MigrationByObservationAbout', 'observationProperties', 'destinationCountry', 'dc/base/HumanReadableStatVars');

INSERT INTO TimeSeries (variable_measured, extra_entities_id, facet_id, entities, facet, last_update_timestamp)
VALUES
  ('Count_Household', 'household-california', 'facet', JSON '{"entity1":"geoId/06"}', JSON '{"measurementMethod":"Census","observationPeriod":"P1Y","provenance":"dc/base/HumanReadableStatVars","unit":"Count"}', PENDING_COMMIT_TIMESTAMP()),
  ('Count_Migration', 'migration-can-usa', 'facet', JSON '{"entity1":"country/CAN","entity2":"country/USA"}', JSON '{"measurementMethod":"Census","observationPeriod":"P1Y","provenance":"dc/base/HumanReadableStatVars","unit":"Count"}', PENDING_COMMIT_TIMESTAMP()),
  ('Count_Migration', 'migration-gbr-fra', 'excluded-facet', JSON '{"entity1":"country/GBR","entity2":"country/FRA"}', JSON '{"measurementMethod":"Modeled","observationPeriod":"P5Y","provenance":"dc/base/Excluded","unit":"Index"}', PENDING_COMMIT_TIMESTAMP()),
  ('Count_Refugee', 'refugee-mex-ind', 'alternate-facet', JSON '{"entity1":"country/MEX","entity2":"country/IND"}', JSON '{"measurementMethod":"Survey","observationPeriod":"P1M","provenance":"dc/base/Other","unit":"Percent"}', PENDING_COMMIT_TIMESTAMP()),
  ('Count_Person', 'person-usa', 'facet', JSON '{"entity1":"country/USA"}', JSON '{"measurementMethod":"Census","observationPeriod":"P1Y","provenance":"dc/base/HumanReadableStatVars","unit":"Count"}', PENDING_COMMIT_TIMESTAMP()),
  ('Count_MigrationByTransportMode', 'migration-air', 'facet', JSON '{"entity1":"country/CAN","entity2":"country/USA","entity3":"Air"}', JSON '{"measurementMethod":"Census","observationPeriod":"P1Y","provenance":"dc/base/HumanReadableStatVars","unit":"Count"}', PENDING_COMMIT_TIMESTAMP()),
  ('Count_MigrationByTransportMode', 'migration-sea', 'facet', JSON '{"entity1":"country/CAN","entity2":"country/USA","entity3":"Sea"}', JSON '{"measurementMethod":"Census","observationPeriod":"P1Y","provenance":"dc/base/HumanReadableStatVars","unit":"Count"}', PENDING_COMMIT_TIMESTAMP()),
  ('Count_MigrationByObservationAbout', 'migration-about', 'facet', JSON '{"entity1":"country/CAN","entity2":"country/MEX","entity3":"country/USA"}', JSON '{"measurementMethod":"Census","observationPeriod":"P1Y","provenance":"dc/base/HumanReadableStatVars","unit":"Count"}', PENDING_COMMIT_TIMESTAMP());

INSERT INTO Observation (variable_measured, entity1, extra_entities_id, facet_id, date, value, last_update_timestamp)
VALUES
  ('Count_Household', 'geoId/06', 'household-california', 'facet', '2024', '50', PENDING_COMMIT_TIMESTAMP()),
  ('Count_Migration', 'country/CAN', 'migration-can-usa', 'facet', '2024', '10', PENDING_COMMIT_TIMESTAMP()),
  ('Count_Migration', 'country/GBR', 'migration-gbr-fra', 'excluded-facet', '2022', '99', PENDING_COMMIT_TIMESTAMP()),
  ('Count_Refugee', 'country/MEX', 'refugee-mex-ind', 'alternate-facet', '2023', '20', PENDING_COMMIT_TIMESTAMP()),
  ('Count_Person', 'country/USA', 'person-usa', 'facet', '2024', '100', PENDING_COMMIT_TIMESTAMP()),
  ('Count_MigrationByTransportMode', 'country/CAN', 'migration-air', 'facet', '2024', '7', PENDING_COMMIT_TIMESTAMP()),
  ('Count_MigrationByTransportMode', 'country/CAN', 'migration-sea', 'facet', '2024', '3', PENDING_COMMIT_TIMESTAMP()),
  ('Count_MigrationByObservationAbout', 'country/CAN', 'migration-about', 'facet', '2024', '5', PENDING_COMMIT_TIMESTAMP());
