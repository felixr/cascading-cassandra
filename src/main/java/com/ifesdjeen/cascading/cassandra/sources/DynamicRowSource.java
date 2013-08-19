package com.ifesdjeen.cascading.cassandra.sources;

import java.util.*;
import java.nio.ByteBuffer;
import java.io.IOException;

import cascading.scheme.SourceCall;
import com.ifesdjeen.cascading.cassandra.SettingsHelper;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.tuple.Tuple;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.ifesdjeen.cascading.cassandra.hadoop.SerializerHelper;


public class DynamicRowSource extends BaseThriftSource implements ISource {

  protected static final Logger logger = LoggerFactory.getLogger(DynamicRowSource.class);
  protected Map<String, String> dataTypes;

  @Override
  public void sourcePrepare(Map<String, Object> settings, SourceCall<Object[], RecordReader> sourceCall) {
    super.sourcePrepare(settings,sourceCall);
    dataTypes = Collections.unmodifiableMap(SettingsHelper.getDynamicTypes(settings));
  }

  public Tuple source(ByteBuffer key,
                      SortedMap<ByteBuffer, IColumn> columns) throws IOException {
    Tuple result = new Tuple();
    result.add(ByteBufferUtil.string(key));

    if (columns.values().isEmpty()) {
      logger.info("Values are empty.");
    }

    AbstractType columnNameType = SerializerHelper.inferType(dataTypes.get("columnName"));
    AbstractType columnValueType = null;
    if (dataTypes.get("columnValue") != null) {
      columnValueType = SerializerHelper.inferType(dataTypes.get("columnValue"));
    }

    for (IColumn column : columns.values()) {
      try {
        if (columnNameType instanceof CompositeType) {
          List components = (List) SerializerHelper.deserialize(column.name(), columnNameType);
          for (Object component : components) {
            result.add(component);
          }
        } else {
          Object val = SerializerHelper.deserialize(column.name(), columnNameType);
          result.add(val);
        }

        if (columnValueType != null) {
          Object colVal = SerializerHelper.deserialize(column.value(), columnValueType);
          result.add(colVal);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

    }

    return result;
  }


}
