package com.ifesdjeen.cascading.cassandra.sources;

import cascading.scheme.SourceCall;
import cascading.tuple.Tuple;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.SortedMap;

public abstract class BaseThriftSource implements ISource {

  protected Map<String, Object> settings;


  public void sourcePrepare(Map<String, Object> settings, SourceCall<Object[], RecordReader> sourceCall) {
    ByteBuffer key = ByteBufferUtil.clone((ByteBuffer) sourceCall.getInput().createKey());
    SortedMap<ByteBuffer, IColumn> value = (SortedMap<ByteBuffer, IColumn>) sourceCall.getInput().createValue();

    Object[] obj = new Object[]{key, value};
    sourceCall.setContext(obj);
    this.settings = settings;
  }

    public Tuple source(Object boxedKey,
                        Object boxedColumns) throws IOException {
        SortedMap<ByteBuffer, IColumn> columns = (SortedMap<ByteBuffer, IColumn>) boxedColumns;
        ByteBuffer key = (ByteBuffer) boxedKey;
        return source(key, columns);
    }

    abstract protected Tuple source(ByteBuffer key,
                                    SortedMap<ByteBuffer, IColumn> columns) throws IOException;
}