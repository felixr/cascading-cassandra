package com.ifesdjeen.cascading.cassandra.sources;

import java.util.*;
import java.nio.ByteBuffer;
import java.io.IOException;

import com.ifesdjeen.cascading.cassandra.SettingsHelper;
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

  private static final Logger logger = LoggerFactory.getLogger(StaticRowSource.class);

    public Tuple source(Map<String, Object> settings,
                        ByteBuffer key,
                        SortedMap<ByteBuffer, IColumn> columns) throws IOException {

    Tuple result = new Tuple();
    result.add(ByteBufferUtil.string(key));

    Map<String, String> dataTypes = SettingsHelper.getTypes(settings);
    List<String> sourceMappings = SettingsHelper.getSourceMappings(settings);

    Map<String, IColumn> columnsByStringName = new HashMap<String, IColumn>();
    for (ByteBuffer columnName : columns.keySet()) {
      String stringName = ByteBufferUtil.string(columnName);
      logger.info("column name: {}", stringName);
      IColumn col = columns.get(columnName);
      logger.info("column: {}", col);
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
          logger.info("Putting deserialized column: {}. {}", columnName, val);
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
