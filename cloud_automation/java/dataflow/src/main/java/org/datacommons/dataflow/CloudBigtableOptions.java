package org.datacommons.dataflow;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;

public interface CloudBigtableOptions extends DataflowPipelineOptions {
  @Description("The Google Cloud project ID for the Cloud Bigtable instance.")
  ValueProvider<String> getBigtableProjectId();

  @SuppressWarnings("unused")
  void setBigtableProjectId(ValueProvider<String> bigtableProjectId);

  @Description("The Google Cloud Bigtable instance ID .")
  ValueProvider<String> getBigtableInstanceId();

  @SuppressWarnings("unused")
  void setBigtableInstanceId(ValueProvider<String> bigtableInstanceId);

  @Description("The Cloud Bigtable table ID in the instance." )
  ValueProvider<String> getBigtableTableId();

  @SuppressWarnings("unused")
  void setBigtableTableId(ValueProvider<String> bigtableTableId);

}
