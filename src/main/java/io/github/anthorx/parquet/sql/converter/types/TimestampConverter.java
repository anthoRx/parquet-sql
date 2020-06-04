package io.github.anthorx.parquet.sql.converter.types;

import io.github.anthorx.parquet.sql.converter.ConvertException;
import io.github.anthorx.parquet.sql.converter.PrimitiveTypeCreator;
import io.github.anthorx.parquet.sql.model.SQLColumnDefinition;
import io.github.anthorx.parquet.sql.model.SQLField;
import io.github.anthorx.parquet.sql.record.RecordField;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import java.sql.Timestamp;

public class TimestampConverter implements ParquetSQLConverter {

    @Override
    public boolean accept(Class<?> c) {
        return c.isAssignableFrom(Timestamp.class);
    }

    @Override
    public RecordField<?> convert(SQLField sqlField) throws ConvertException {
        Timestamp timestamp = (Timestamp) sqlField.getValue();
        return new RecordField<>(sqlField.getName(), timestamp.getTime(), RecordConsumer::addLong);
    }

    @Override
    public PrimitiveType convert(SQLColumnDefinition sqlColumnDefinition) throws ConvertException {
        return PrimitiveTypeCreator.create(sqlColumnDefinition.getName(),
                PrimitiveType.PrimitiveTypeName.INT64,
                LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS),
                sqlColumnDefinition.isNullable());
    }
}
