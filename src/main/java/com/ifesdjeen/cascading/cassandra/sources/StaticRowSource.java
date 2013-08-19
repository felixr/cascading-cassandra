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
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.ifesdjeen.cascading.cassandra.hadoop.SerializerHelper;


/**
 * StaticRowSource is intended for usage with static sources, as the name suggests.
 */
public class StaticRowSource extends BaseThriftSource implements ISource {

  Map<String, String> dataTypes;
  protected List<String> sourceMappings;

  private static final Logger logger = LoggerFactory.getLogger(StaticRowSource.class);

  @Override
  public void sourcePrepare(Map<String, Object> settings, SourceCall<Object[], RecordReader> sourceCall) {
    super.sourcePrepare(settings,sourceCall);
    dataTypes = SettingsHelper.getTypes(settings);
    sourceMappings = SettingsHelper.getSourceMappings(settings);
  }

  @Override
  protected Tuple source(ByteBuffer key, SortedMap<ByteBuffer, IColumn> columns) throws IOException {
    Tuple result = new Tuple();
    result.add(ByteBufferUtil.string(key));

    Map<String, IColumn> columnsByStringName = new HashMap<String, IColumn>();
    for (ByteBuffer columnName : columns.keySet()) {
      String stringName = ByteBufferUtil.string(columnName);
      logger.debug("column name: {}", stringName);
      IColumn col = columns.get(columnName);
      logger.debug("column: {}", col);
      columnsByStringName.put(stringName, col);
    }

    for (String columnName : sourceMappings) {
      AbstractType columnValueType = SerializerHelper.inferType(dataTypes.get(columnName));
      if (columnValueType != null) {
        try {
          IColumn column = columnsByStringName.get(columnName);
          ByteBuffer serializedVal = column.value();
          Object val = null;
          if (serializedVal != null) {
            val = SerializerHelper.deserialize(serializedVal, columnValueType);
          }
          logger.debug("Putting deserialized column: {}. {}", columnName, val);
          result.add(val);
        } catch (Exception e) {
          throw new RuntimeException("Couldn't deserialize column: " + columnName, e);
        }
      } else {
        throw new RuntimeException("no type given for column: " + columnName);
      }
    }

    return result;
  }


}
