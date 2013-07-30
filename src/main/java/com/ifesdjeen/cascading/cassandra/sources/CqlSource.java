package com.ifesdjeen.cascading.cassandra.sources;

import cascading.scheme.SourceCall;
import cascading.tuple.Tuple;
import com.ifesdjeen.cascading.cassandra.SettingsHelper;
import com.ifesdjeen.cascading.cassandra.hadoop.SerializerHelper;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class CqlSource implements ISource {


  @Override
  public void sourcePrepare(SourceCall<Object[], RecordReader> sourceCall) {
    Map<String,ByteBuffer> key = (Map<String, ByteBuffer>) sourceCall.getInput().createKey();
    Map<String,ByteBuffer> value = (Map<String, ByteBuffer>) sourceCall.getInput().createValue();

    Object[] obj = new Object[]{key, value};

    sourceCall.setContext(obj);
  }

  @Override
  public Tuple source(Map<String, Object> settings, Object boxedKey, Object boxedValue) throws IOException {
    Map<String, String> dataTypes = SettingsHelper.getTypes(settings);

    Tuple result = new Tuple();
    Map<String,ByteBuffer> keys = (Map<String,ByteBuffer>) boxedKey;
    Map<String,ByteBuffer> columns = (Map<String,ByteBuffer>) boxedValue;

    for(Map.Entry<String, ByteBuffer> key : keys.entrySet()) {
      try {
        result.add(SerializerHelper.deserialize(key.getValue(),
                SerializerHelper.inferType(dataTypes.get(key.getKey()))));
      } catch (Exception e) {
        throw new RuntimeException("Couldn't deserialize key: " + key.getKey(), e);
      }
    }

    for(Map.Entry<String, ByteBuffer> column : columns.entrySet()) {
      try {
        result.add(SerializerHelper.deserialize(column.getValue(),
                SerializerHelper.inferType(dataTypes.get(column.getKey()))));
      } catch (Exception e) {
        throw new RuntimeException("Couldn't deserialize value for: " + column.getKey(), e);
      }
    }

    return result;
  }
}