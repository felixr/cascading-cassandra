package com.ifesdjeen.cascading.cassandra;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public abstract class BaseCassandraScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {

  static final Logger logger = LoggerFactory.getLogger(CassandraScheme.class);
  public static final String DB_PORT = "db.port";
  public static final String DB_HOST = "db.host";
  public static final String DB_KEYSPACE = "db.keyspace";
  public static final String DB_COLUMN_FAMILY = "db.columnFamily";
  public static final String SOURCE_RANGE_BATCH_SIZE = "source.rangeBatchSize";
  public static final String SOURCE_INPUT_SPLIT_SIZE = "source.inputSplitSize";
  public static final String CASSANDRA_INPUT_PARTITIONER = "cassandra.inputPartitioner";
  public static final String CASSANDRA_OUTPUT_PARTITIONER = "cassandra.outputPartitioner";

  protected String pathUUID;
  protected Map<String, Object> settings;

  protected String host;
  protected String port;
  protected String columnFamily;
  protected String keyspace;

  public BaseCassandraScheme(Map<String, Object> settings) {
    this.pathUUID = UUID.randomUUID().toString();

    // default settings
    this.settings = new HashMap<String, Object>();
    this.settings.put(DB_PORT, "9160");
    this.settings.put(DB_HOST, "localhost");
    this.settings.put(SOURCE_INPUT_SPLIT_SIZE, 50);
    this.settings.put(SOURCE_RANGE_BATCH_SIZE, 1000);
    this.settings.put(CASSANDRA_INPUT_PARTITIONER, "org.apache.cassandra.dht.Murmur3Partitioner");
    this.settings.put(CASSANDRA_OUTPUT_PARTITIONER, "org.apache.cassandra.dht.Murmur3Partitioner");
    this.settings.putAll(settings);

    this.port = (String) this.settings.get(DB_PORT);
    this.host = (String) this.settings.get(DB_HOST);
    this.keyspace = (String) this.settings.get(DB_KEYSPACE);
    this.columnFamily = (String) this.settings.get(DB_COLUMN_FAMILY);
  }

  /**
   *
   * Source Methods
   *
   */

  @Override
  public void sourceConfInit(FlowProcess<JobConf> process,
                             Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
    logger.info("Configuring source...");

    ConfigHelper.setInputRpcPort(conf, port);
    ConfigHelper.setInputInitialAddress(conf, this.host);

    ConfigHelper.setRangeBatchSize(conf, (Integer) this.settings.get(SOURCE_RANGE_BATCH_SIZE));
    ConfigHelper.setInputSplitSize(conf, (Integer) this.settings.get(SOURCE_INPUT_SPLIT_SIZE));
    ConfigHelper.setInputPartitioner(conf, (String) this.settings.get(CASSANDRA_INPUT_PARTITIONER));

    FileInputFormat.addInputPaths(conf, getPath().toString());
  }


  /**
   * @param flowProcess
   * @param sourceCall
   */
  @Override
  public void sourceCleanup(FlowProcess<JobConf> flowProcess,
                            SourceCall<Object[], RecordReader> sourceCall) {
    sourceCall.setContext(null);
  }

  /**
   *
   * Sink Methods
   *
   */

  /**
   *
   * @param process
   * @param tap
   * @param conf
   */
  @Override
  public void sinkConfInit(FlowProcess<JobConf> process,
                           Tap<JobConf, RecordReader, OutputCollector> tap,
                           JobConf conf) {
    ConfigHelper.setOutputRpcPort(conf, port);
    ConfigHelper.setOutputInitialAddress(conf, host);

    ConfigHelper.setRangeBatchSize(conf, (Integer) this.settings.get(SOURCE_RANGE_BATCH_SIZE));
    ConfigHelper.setInputSplitSize(conf, (Integer) this.settings.get(SOURCE_INPUT_SPLIT_SIZE));

    ConfigHelper.setOutputPartitioner(conf, (String) this.settings.get(CASSANDRA_OUTPUT_PARTITIONER));
    ConfigHelper.setOutputColumnFamily(conf, keyspace, columnFamily);

    FileOutputFormat.setOutputPath(conf, getPath());
  }

  /**
   *
   * Generic Methods
   *
   */

  public Path getPath() {
    return new Path(pathUUID);
  }

  public String getIdentifier() {
    return host + "_" + port + "_" + keyspace + "_" + columnFamily;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other)
      return true;
    if (!(other instanceof CassandraScheme))
      return false;
    if (!super.equals(other))
      return false;

    CassandraScheme that = (CassandraScheme) other;

    return getPath().toString().equals(that.getPath().toString());

  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + getPath().toString().hashCode();
    result = 31 * result + (host != null ? host.hashCode() : 0);
    result = 31 * result + (port != null ? port.hashCode() : 0);
    result = 31 * result + (keyspace != null ? keyspace.hashCode() : 0);
    result = 31 * result + (columnFamily != null ? columnFamily.hashCode() : 0);
    return result;
  }

}
