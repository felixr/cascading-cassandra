package com.ifesdjeen.cascading.cassandra;

import com.google.common.base.Joiner;
import com.ifesdjeen.cascading.cassandra.sinks.DynamicRowSink;
import com.ifesdjeen.cascading.cassandra.sinks.ISink;
import com.ifesdjeen.cascading.cassandra.sinks.StaticRowSink;
import com.ifesdjeen.cascading.cassandra.sources.DynamicRowSource;
import com.ifesdjeen.cascading.cassandra.sources.ISource;
import com.ifesdjeen.cascading.cassandra.sources.StaticRowSource;
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;

import cascading.tap.Tap;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import org.apache.hadoop.mapred.*;

import org.apache.cassandra.hadoop.ConfigHelper;

import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;

import java.io.IOException;
import java.util.*;
import java.nio.ByteBuffer;

public class CassandraScheme extends BaseCassandraScheme {

  ISource sourceImpl;
  ISink sinkImpl;

  public CassandraScheme(Map<String, Object> settings) {
    super(settings);
  }

  /**
   *
   * Source Methods
   *
   */

  /**
   * @param flowProcess
   * @param sourceCall
   */
  @Override
  public void sourcePrepare(FlowProcess<JobConf> flowProcess,
                            SourceCall<Object[], RecordReader> sourceCall) {
    sourceImpl = getSourceImpl();
    sourceImpl.sourcePrepare(sourceCall);
  }

  /**
   *
   * @param process
   * @param tap
   * @param conf
   */
  @Override
  public void sourceConfInit(FlowProcess<JobConf> process,
                             Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
    super.sourceConfInit(process, tap, conf);
    conf.setInputFormat(ColumnFamilyInputFormat.class);
    setInputSlicePredicate(conf);

    if (this.settings.containsKey("source.useWideRows")) {
      ConfigHelper.setInputColumnFamily(conf, this.keyspace, this.columnFamily,
              (Boolean) this.settings.get("source.useWideRows"));
    } else {
      ConfigHelper.setInputColumnFamily(conf, this.keyspace, this.columnFamily);
    }

  }

  /**
   * Sets the Input SlicePredicate via {@link ConfigHelper} using the settings map.
   * @param conf
   */
  private void setInputSlicePredicate(JobConf conf) {
    SlicePredicate predicate;
    final List<String> sourceColumns = this.getSourceColumns();
    if (this.settings.containsKey("source.predicate")) {
      final Object obj = this.settings.get("source.predicate");
      if (obj instanceof SlicePredicate) {
        predicate = (SlicePredicate) obj;
      } else {
        logger.warn("Object in 'source.predicate' not an instance of SlicePredicate; using empty predicate instead.");
        predicate = createEmptyPredicate();
      }
      if (!sourceColumns.isEmpty()) {
        logger.warn("Ignoring 'source.columns' because 'source.predicate' is set");
      }
    } else {

      if (!sourceColumns.isEmpty()) {
        logger.debug("Using with following columns: {}",
                Joiner.on(", ").join(sourceColumns));
        predicate = createColumnFilterPredicate(sourceColumns);
      } else {
        logger.debug("Using slicerange over all columns");
        predicate = createEmptyPredicate();
      }
    }
    ConfigHelper.setInputSlicePredicate(conf, predicate);
  }

  /**
   * Creates a {@link SlicePredicate} using a list of column names.
   *
   * @param sourceColumns
   * @return
   */
  private SlicePredicate createColumnFilterPredicate(List<String> sourceColumns) {
    SlicePredicate predicate;
    List<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();
    for (String columnFieldName : sourceColumns) {
      columnNames.add(ByteBufferUtil.bytes(columnFieldName));
    }

    predicate = new SlicePredicate();
    predicate.setColumn_names(columnNames);
    return predicate;
  }

  /**
   * Creates an empty {@link SlicePredicate}.
   *
   * @return SlicePredicate with no filter predicates
   */
  SlicePredicate createEmptyPredicate() {
    final SlicePredicate predicate = new SlicePredicate();
    SliceRange sliceRange = new SliceRange();
    sliceRange.setStart(ByteBufferUtil.EMPTY_BYTE_BUFFER);
    sliceRange.setFinish(ByteBufferUtil.EMPTY_BYTE_BUFFER);
    predicate.setSlice_range(sliceRange);
    return predicate;
  }

  /**
   * FIXME: Pitfalls: Currently only String is supported as a rowKey.
   *
   * @param flowProcess
   * @param sourceCall
   * @return
   * @throws IOException
   */
  @Override
  public boolean source(FlowProcess<JobConf> flowProcess,
                        SourceCall<Object[], RecordReader> sourceCall) throws IOException {
    RecordReader input = sourceCall.getInput();

    Object key = sourceCall.getContext()[0];
    Object columns = sourceCall.getContext()[1];

    boolean hasNext = input.next(key, columns);
    if (!hasNext) {
      return false;
    }

    Tuple result = sourceImpl.source(this.settings, key, columns);

    sourceCall.getIncomingEntry().setTuple(result);
    return true;
  }

  protected List<String> getSourceColumns() {
    if (this.settings.containsKey("source.columns")) {
      return (List<String>) this.settings.get("source.columns");
    } else {
      return new ArrayList<String>();
    }
  }

  /**
   *
   * Sink Methods
   *
   */

  @Override
  public void sinkPrepare(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
    super.sinkPrepare(flowProcess, sinkCall);
    sinkImpl = getSinkImpl();
  }

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
    super.sinkConfInit(process, tap, conf);
    conf.setOutputFormat(ColumnFamilyOutputFormat.class);
  }

  /**
   * @param flowProcess
   * @param sinkCall
   * @throws IOException
   */
  @Override
  public void sink(FlowProcess<JobConf> flowProcess,
                   SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
    TupleEntry tupleEntry = sinkCall.getOutgoingEntry();
    OutputCollector outputCollector = sinkCall.getOutput();


    sinkImpl.sink(settings, tupleEntry, outputCollector);
  }

  protected ISink getSinkImpl() {
    String className = (String) this.settings.get("sink.sinkImpl");
    boolean useWideRows = SettingsHelper.isDynamicMapping(this.settings);

    try {
      if (className == null) {
        if (useWideRows) {
          return new DynamicRowSink();
        } else {
          return new StaticRowSink();
        }
      } else {
        final Class<?> aClass = Class.forName(className);
        if (ISink.class.isAssignableFrom(aClass)) {
          return (ISink) aClass.newInstance();
        } else {
          throw new IllegalArgumentException(aClass.getCanonicalName() + " does not implement ISink");
        }
      }
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  protected ISource getSourceImpl() {
    String className = (String) this.settings.get("source.sourceImpl");
    boolean useWideRows = SettingsHelper.isDynamicMapping(this.settings);

    try {
      if (className == null) {
        if (useWideRows) {
          return new DynamicRowSource();
        } else {
          return new StaticRowSource();
        }
      } else {
        final Class<?> aClass = Class.forName(className);
        if (ISink.class.isAssignableFrom(aClass)) {
          return (ISource) aClass.newInstance();
        } else {
          throw new IllegalArgumentException(aClass.getCanonicalName() + " does not implement ISource");
        }
      }
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }


}
