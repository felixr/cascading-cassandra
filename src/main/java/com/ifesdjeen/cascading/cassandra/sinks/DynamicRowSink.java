package com.ifesdjeen.cascading.cassandra.sinks;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.FieldsResolverException;


import com.ifesdjeen.cascading.cassandra.SettingsHelper;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.thrift.*;

import com.ifesdjeen.cascading.cassandra.hadoop.SerializerHelper;

import java.io.IOException;
import java.util.*;
import java.nio.ByteBuffer;

import org.apache.hadoop.mapred.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicRowSink implements ISink {

  private static final Logger logger = LoggerFactory.getLogger(DynamicRowSink.class);

  public void sink(Map<String, Object> settings, TupleEntry tupleEntry, OutputCollector outputCollector)
          throws IOException {

    String rowKeyField = SettingsHelper.getMappingRowKeyField(settings);

    Tuple key = tupleEntry.selectTuple(new Fields(rowKeyField));
    ByteBuffer keyBuffer = SerializerHelper.serialize(key.get(0));

    Map<String, String> dataTypes = SettingsHelper.getDynamicTypes(settings);
    Map<String, String> dynamicMappings = SettingsHelper.getDynamicMappings(settings);

    AbstractType columnNameType = SerializerHelper.inferType(dataTypes.get("columnName"));

    Object columnNameFieldSpec = dynamicMappings.get("columnName");
    List<String> columnNameFields = new ArrayList<String>();
    if (columnNameFieldSpec instanceof String) {
      columnNameFields.add((String) columnNameFieldSpec);
    } else {
      columnNameFields.addAll((List<String>) columnNameFieldSpec);
    }

    List tupleEntryColumnNameValues = new ArrayList();
    for (String columnNameField : columnNameFields) {
      try {
        Object columnNameValue = tupleEntry.getObject(columnNameField);
        logger.info("column name component field: {}", columnNameField);
        logger.info("column name component value: {}", columnNameValue);
        tupleEntryColumnNameValues.add(columnNameValue);
      } catch (FieldsResolverException e) {
        throw new RuntimeException("Couldn't resolve column name field: " + columnNameField);
      }
    }

    String columnValueField = dynamicMappings.get("columnValue");
    Object tupleEntryColumnValueValue = null;
    if (columnValueField != null) {
      try {
        tupleEntryColumnValueValue = tupleEntry.getObject(columnValueField);
        logger.info("column value field: {}", columnValueField);
        logger.info("column value value: {}", tupleEntryColumnValueValue);
      } catch (FieldsResolverException e) {
        throw new RuntimeException("Couldn't resolve column value field: " + columnValueField);
      }
    }

    List<Mutation> mutations = new ArrayList<Mutation>();
    if (tupleEntryColumnNameValues.size() > 0) {

      ByteBuffer columnName;
      if (columnNameType instanceof CompositeType) {
        logger.info("CompositeType");
        columnName = SerializerHelper.serializeComposite(tupleEntryColumnNameValues, (CompositeType) columnNameType);
      } else {
        logger.info("SimpleType");
        columnName = SerializerHelper.serialize(tupleEntryColumnNameValues.get(0));
      }

      ByteBuffer serializedColValue;
      if (tupleEntryColumnValueValue != null) {
        serializedColValue = SerializerHelper.serialize(tupleEntryColumnValueValue);
      } else {
        byte[] emptyBytes = {};
        serializedColValue = ByteBuffer.wrap(emptyBytes);
      }

      Mutation mutation = Util.createColumnPutMutation(columnName, serializedColValue);
      mutations.add(mutation);
    }

    outputCollector.collect(keyBuffer, mutations);
  }
}
