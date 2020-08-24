# CSVImport Template

The CSVImport dataflow template creates dataflow jobs to ingest CSV data from
GCS to Cloud Bigtable.

The template is stored at  gs://datcom-dataflow-templates/templates

To deploy changes to the template run:

```
  mvn compile exec:java -Dexec.mainClass=org.datacommons.dataflow.CsvImport -Dexec.args="--runner=DataflowRunner --project=google.com:datcom-store-dev --stagingLocation=gs://datcom-dataflow-templates/staging --templateLocation=gs://datcom-dataflow-templates/templates/csv_to_bt --region=us-central1"

```
