package io.github.anthorx.parquet.sql.jdbc;

import io.github.anthorx.parquet.sql.parquet.model.ParquetRecordField;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.sql.Types;
import java.util.Optional;
import java.util.function.Consumer;

public class NullRecordFieldConstructor {

  /**
   * Create RecordField with null value for a given type
   */
  public static ParquetRecordField<?> newNullRecordField(Type currentField) {
    ParquetRecordField<?> result = new ParquetRecordField<>(currentField.getName(), null);

    Consumer<ReadRecordConsumer> consumer = NullRecordFieldConstructor.getConverter(currentField);
    result.addReadConsumer(((readRecordConsumer, o) -> consumer.accept(readRecordConsumer)));

    return result;
  }

  private static Consumer<ReadRecordConsumer> getConverter(Type currentField) {
    Optional<Integer> result;

    if (currentField.getLogicalTypeAnnotation() == null) {
      result = getNullTypeConsumerFromPrimitiveType(currentField);
    } else {
      result = getNullTypeConsumerFromLogicalType(currentField);
    }

    return result
        .map((type) -> (Consumer<ReadRecordConsumer>) read -> read.setNull(type))
        .orElse((read) -> read.setObject(null));
  }

  protected static Optional<Integer> getNullTypeConsumerFromLogicalType(Type currentField) {
    return currentField.getLogicalTypeAnnotation().accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Integer>() {
      @Override
      public Optional<Integer> visit(LogicalTypeAnnotation.StringLogicalTypeAnnotation logicalType) {
        return Optional.of(Types.VARCHAR);
      }

      @Override
      public Optional<Integer> visit(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation logicalType) {
        return Optional.of(Types.TIMESTAMP);
      }

      @Override
      public Optional<Integer> visit(LogicalTypeAnnotation.DateLogicalTypeAnnotation logicalType) {
        return Optional.of(Types.DATE);
      }

      @Override
      public Optional<Integer> visit(LogicalTypeAnnotation.IntLogicalTypeAnnotation logicalType) {
        return Optional.of(Types.NUMERIC);
      }

      @Override
      public Optional<Integer> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation logicalType) {
        return Optional.of(Types.DECIMAL);
      }
    });
  }

  protected static Optional<Integer> getNullTypeConsumerFromPrimitiveType(Type parquetField) {
    PrimitiveType primitiveType = parquetField.asPrimitiveType();
    Optional<Integer> result = Optional.empty();

    switch (primitiveType.getPrimitiveTypeName()) {
      case DOUBLE:
        result = Optional.of(Types.DECIMAL);
        break;
      case FLOAT:
        result = Optional.of(Types.FLOAT);
        break;
      case BOOLEAN:
        result = Optional.of(Types.BOOLEAN);
        break;
      case INT32:
        result = Optional.of(Types.INTEGER);
        break;
      case INT64:
        result = Optional.of(Types.NUMERIC);
        break;
      // According to Spark's code (SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE and ParquetWriteSupport), default Parquet format used for timestamp is INT96
      // INT96 is not standard and is only used for timestamp
      // Still the case on Spark 3.0.0
      case INT96:
        result = Optional.of(Types.TIMESTAMP);
        break;
    }
    return result;
  }

}
