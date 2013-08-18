package com.ifesdjeen.cascading.cassandra.hadoop;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.TupleEntrySchemeCollector;
import org.apache.hadoop.mapred.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CassandraCollector extends TupleEntrySchemeCollector implements OutputCollector {
  /**
   * Field LOG
   */
  private static final Logger LOG = LoggerFactory.getLogger(CassandraCollector.class);

  private final JobConf conf;
  private RecordWriter writer;
  private final FlowProcess<JobConf> hadoopFlowProcess;
  private final Tap<JobConf, RecordReader, OutputCollector> tap;
  private final Reporter reporter = Reporter.NULL;

  /**
   * Constructor TapCollector creates a new TapCollector instance.
   *
   * @param flowProcess
   * @param tap         of type Tap
   * @throws IOException when fails to initialize
   */
  public CassandraCollector(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap) {
    super(flowProcess, tap.getScheme());
    this.hadoopFlowProcess = flowProcess;

    this.tap = tap;
    this.conf = new JobConf(flowProcess.getConfigCopy());

    this.setOutput(this);
  }

  @Override
  public void prepare() {
    try {
      initialize();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    super.prepare();
  }

  private void initialize() throws IOException {
    tap.sinkConfInit(hadoopFlowProcess, conf);

    OutputFormat outputFormat = conf.getOutputFormat();

    LOG.info("Output format class is: " + outputFormat.getClass().toString());

    writer = outputFormat.getRecordWriter(null, conf, tap.getIdentifier(), Reporter.NULL);

    sinkCall.setOutput(this);
  }

  @Override
  public void close() {
    try {
      LOG.info("closing tap collector for: {}", tap);
      writer.close(reporter);
    } catch (IOException exception) {
      LOG.warn("exception closing: {}", exception);
      throw new TapException("exception closing CassandraCollector", exception);
    } finally {
      super.close();
    }
  }

  public void collect(Object writableComparable, Object writable) throws IOException {
    if (hadoopFlowProcess instanceof HadoopFlowProcess)
      ((HadoopFlowProcess) hadoopFlowProcess).getReporter().progress();

    writer.write(writableComparable, writable);
  }
}
