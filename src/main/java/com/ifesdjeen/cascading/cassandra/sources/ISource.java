package com.ifesdjeen.cascading.cassandra.sources;

import java.util.*;
import java.nio.ByteBuffer;
import java.io.IOException;

import cascading.scheme.SourceCall;
import cascading.tuple.Tuple;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.db.IColumn;
import org.apache.hadoop.mapred.RecordReader;


/**
 * ISource is used to allow flexibility when dealing with different input sources
 * from Cassandra, and deal with cases such as Dynamic/Static columns, custom
 * serialization etc.
 */
public interface ISource {

  /**
   * Creates initial (empty) tuple
   *
   * @param settings
   * @param sourceCall
   * @return
   */
  public void sourcePrepare(Map<String, Object> settings, SourceCall<Object[], RecordReader> sourceCall);

  /**
   * Convert `value` map (key/value pairs) to Cascading tuple.
   *
   * @param value - key/value pairs of column names and columns (values)
   * @param key - row (partition) key
   * @return
   * @throws IOException
   */
  public Tuple source(Object key, Object value) throws IOException;
}
