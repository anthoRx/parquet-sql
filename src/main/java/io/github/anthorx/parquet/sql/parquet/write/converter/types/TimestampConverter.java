package io.github.anthorx.parquet.sql.parquet.write.converter.types;

import io.github.anthorx.parquet.sql.jdbc.model.SQLColumnDefinition;
import io.github.anthorx.parquet.sql.jdbc.model.SQLField;
import io.github.anthorx.parquet.sql.parquet.model.RecordField;
import io.github.anthorx.parquet.sql.parquet.write.converter.PrimitiveTypeCreator;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import java.sql.Timestamp;
import java.util.TimeZone;

public class TimestampConverter implements ParquetSQLConverter {

  @Override
  public boolean accept(Class<?> c) {
    return c.isAssignableFrom(Timestamp.class);
  }

  /**
   * Convert a sqlField to A RecordField.
   * Add offset from UTC because JDBC timestamp is dependant to the local timezone
   */
  @Override
  public RecordField<?> convert(SQLField sqlField) {
    Timestamp timestamp = (Timestamp) sqlField.getValue();
    long timestampMs = timestamp.getTime();
    long offset = TimeZone.getDefault().getOffset(timestampMs);
    long timestampUtcAdjusted = timestampMs + offset;

    return new RecordField<>(sqlField.getName(), timestampUtcAdjusted)
        .addWriteConsumer(RecordConsumer::addLong);
  }

  @Override
  public PrimitiveType convert(SQLColumnDefinition sqlColumnDefinition) {
    return PrimitiveTypeCreator.create(sqlColumnDefinition.getName(),
        PrimitiveType.PrimitiveTypeName.INT64,
        LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS),
        sqlColumnDefinition.isNullable());
  }
}
