package io.github.anthorx.parquet.sql.write;

import io.github.anthorx.parquet.sql.read.SQLGroupConverter;
import io.github.anthorx.parquet.sql.record.ReadRecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.*;
import java.util.function.Consumer;

/**
 * create the Consumer that calls ReadRecordConsumer.setNull with the proper type
 */
public class NullTypeConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(SQLGroupConverter.class);

  public static Consumer<ReadRecordConsumer> get(Type currentField) {

    Optional<Consumer<ReadRecordConsumer>> result;

    if (currentField.getLogicalTypeAnnotation() == null) {
      result = getNullTypeConsumerFromPrimitiveType(currentField);
    } else {
      result = getNullTypeConsumerFromLogicalType(currentField);
    }

    return result.orElse((read) -> {
      LOG.debug("No proper type found, falling back to generic setObject(null) which may cause performance issue");
      read.setObject(null);
    });
  }

  private static Optional<Consumer<ReadRecordConsumer>> setNullConsumer(int type) {
    return Optional.of((read) -> read.setNull(type));
  }

  private static Optional<Consumer<ReadRecordConsumer>> getNullTypeConsumerFromLogicalType(Type currentField) {
    return currentField.getLogicalTypeAnnotation().accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Consumer<ReadRecordConsumer>>() {
      @Override
      public Optional<Consumer<ReadRecordConsumer>> visit(LogicalTypeAnnotation.StringLogicalTypeAnnotation logicalType) {
        return setNullConsumer(Types.VARCHAR);
      }

      @Override
      public Optional<Consumer<ReadRecordConsumer>> visit(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation logicalType) {
        return setNullConsumer(Types.TIMESTAMP);
      }

      @Override
      public Optional<Consumer<ReadRecordConsumer>> visit(LogicalTypeAnnotation.DateLogicalTypeAnnotation logicalType) {
        return setNullConsumer(Types.DATE);
      }

      @Override
      public Optional<Consumer<ReadRecordConsumer>> visit(LogicalTypeAnnotation.IntLogicalTypeAnnotation logicalType) {
        return setNullConsumer(Types.NUMERIC);
      }

      @Override
      public Optional<Consumer<ReadRecordConsumer>> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation logicalType) {
        return setNullConsumer(Types.DECIMAL);
      }
    });
  }

  private static Optional<Consumer<ReadRecordConsumer>> getNullTypeConsumerFromPrimitiveType(Type parquetField) {
    PrimitiveType primitiveType = parquetField.asPrimitiveType();
    Optional<Consumer<ReadRecordConsumer>> result = Optional.empty();

    switch (primitiveType.getPrimitiveTypeName()) {
      case DOUBLE:
        result = setNullConsumer(Types.DECIMAL);
        break;
      case FLOAT:
        result = setNullConsumer(Types.FLOAT);
        break;
      case BOOLEAN:
        result = setNullConsumer(Types.BOOLEAN);
        break;
      case INT32:
        result = setNullConsumer(Types.INTEGER);
        break;
      case INT64:
        result = setNullConsumer(Types.NUMERIC);
        break;
      // According to Spark's code (SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE and ParquetWriteSupport), default Parquet format used for timestamp is INT96
      // INT96 is not standard and is only used for timestamp
      // Still the case on Spark 3.0.0
      case INT96:
        result = setNullConsumer(Types.TIMESTAMP);
        break;
    }
    return result;
  }

}
