package org.datacommons.dataflow;

import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*

public class CsvImport {

  private static final byte[] FAMILY = Bytes.toBytes("csv");
  private static final Logger LOG = LoggerFactory.getLogger(CsvImport.class);

  static class CsvToBigtableFn
      extends DoFn<String, KV<ByteString, Iterable<Mutation>>> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      try {
        // TODO: Pass header as congfiguration.
        String[] headers = new String[] {"value"};
        String[] values = c.element().split(",");
        Preconditions.checkArgument(headers.length == (values.length-1)); // first element of values is key for BT row.

        byte[] rowkey = Bytes.toBytes(values[0]);
        byte[][] headerBytes = new byte[headers.length][];
        ImmutableList.Builder<Mutation> mutations = ImmutableList.builder();
        for (int i = 0; i < headers.length; i++) {
          Mutation.SetCell setCell = null;
          headerBytes[i] = Bytes.toBytes(headers[i]);
          setCell =  Mutation.SetCell.newBuilder()
                          .setFamilyName(FAMILY)
                          .setColumnQualifier(ByteString.copyFrom( headerBytes[i]))
                          .setValue(ByteString.copyFrom(Bytes.toBytes(values[i+1]))) // since values[0] is the key.
                          .build();
           mutations.add(Mutation.newBuilder().setSetCell(setCell).build());
        }
        Iterable<Mutation> mutIter = mutations.build();
        c.output(KV.of(ByteString.copyFrom(rowkey), mutIter));
      } catch (Exception e) {
        LOG.error("Failed to process input {}", c.element(), e);
          throw e;
      }
    }
  };

  public static interface BigtableCsvOptions extends CloudBigtableOptions {

    @Description("The headers for the CSV file.")
    ValueProvider<String> getHeaders();

    @SuppressWarnings("unused")
    void setHeaders(ValueProvider<String> headers);

    @Description("The Cloud Storage path to the CSV file.")
    ValueProvider<String> getInputFile();

    @SuppressWarnings("unused")
    void setInputFile(ValueProvider<String> location);
  }


  /**
   * <p>Creates a dataflow template with following stages: </p>
   * <ol>
   * <li> Read a CSV from GCS.
   * <li> Transform each line to a Bigtable Row mutation.
   * <li> Write to Bigtable.
   * </ol>
   *
   * @param args Arguments to use to configure the Dataflow Pipeline. Check README for deployment
   * instructions.
   * --project=[dataflow project] --stagingLocation=gs://[your google storage bucket]
   * --inputFile=gs://[your google storage object]
   * --bigtableProject=[bigtable project] --bigtableInstanceId=[bigtable instance id]
   * --bigtableTableId=[bigtable tableName]
   *
   *
   *  This pipepline uses BigtableIO instead of CloudBigtableIO libraries. This is because,
   *  CloudBigtableIO libraries do not completely support dataflow templates.
   *  The inspiration for this is pipeline is from cloud google ataflow examples.
   */

  public static void main(String[] args) throws IllegalArgumentException {
    BigtableCsvOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(BigtableCsvOptions.class);

    Pipeline p = Pipeline.create(options);

    BigtableIO.Write write = BigtableIO.write()
            .withProjectId(options.getBigtableProjectId())
            .withInstanceId(options.getBigtableInstanceId())
            .withTableId(options.getBigtableTableId());
    p.apply("ReadMyFile", TextIO.read().from(options.getInputFile()))
        .apply("TransformParsingsToBigtable", ParDo.of(new CsvToBigtableFn()))
        .apply("WriteToBigtable", write);

    PipelineResult result = p.run();
    // Wait for pipeline to finish only if it is not constructing a template.
    if (options.as(DataflowPipelineOptions.class).getTemplateLocation() == null) {
      result.waitUntilFinish();
    }
  }
}
