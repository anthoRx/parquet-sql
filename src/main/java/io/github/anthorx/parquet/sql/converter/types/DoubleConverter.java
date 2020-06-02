package io.github.anthorx.parquet.sql.converter.types;

import io.github.anthorx.parquet.sql.converter.ConvertException;
import io.github.anthorx.parquet.sql.converter.PrimitiveTypeCreator;
import io.github.anthorx.parquet.sql.model.SQLColumnDefinition;
import io.github.anthorx.parquet.sql.model.SQLField;
import io.github.anthorx.parquet.sql.record.RecordField;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.PrimitiveType;

import java.math.BigDecimal;

public class DoubleConverter implements ParquetSQLConverter {

    @Override
    public boolean accept(Class<?> c) {
        return c.isAssignableFrom(Double.class);
    }

    @Override
    public RecordField<?> convert(SQLField sqlField) throws ConvertException {
        BigDecimal bigDecimal = (BigDecimal) sqlField.getValue();
        return new RecordField<>(sqlField.getName(), bigDecimal.doubleValue(), RecordConsumer::addDouble);
    }

    @Override
    public PrimitiveType convert(SQLColumnDefinition sqlColumnDefinition) throws ConvertException {
        return PrimitiveTypeCreator.create(sqlColumnDefinition.getName(),
                PrimitiveType.PrimitiveTypeName.DOUBLE,
                null,
                sqlColumnDefinition.isNullable());
    }
}
