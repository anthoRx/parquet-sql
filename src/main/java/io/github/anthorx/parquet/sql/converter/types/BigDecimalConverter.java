package io.github.anthorx.parquet.sql.converter.types;

import io.github.anthorx.parquet.sql.converter.ConvertException;
import io.github.anthorx.parquet.sql.converter.PrimitiveTypeCreator;
import io.github.anthorx.parquet.sql.model.SQLColumnDefinition;
import io.github.anthorx.parquet.sql.model.SQLField;
import io.github.anthorx.parquet.sql.record.RecordField;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import java.math.BigDecimal;

public class BigDecimalConverter implements ParquetSQLConverter {

    public final int DEFAULT_PRECISION = 18;

    @Override
    public boolean accept(Class<?> c) {
        return c.isAssignableFrom(BigDecimal.class);
    }

    @Override
    public RecordField<?> convert(SQLField sqlField) throws ConvertException {
        int checkedPrecision = sqlField
                .getPrecision()
                .filter(p -> p > 0)
                .orElse(DEFAULT_PRECISION);
        BigDecimal bd = (BigDecimal) sqlField.getValue();

        if (checkedPrecision <= 9) {
            return new RecordField<>(sqlField.getName(), bd.intValue(), RecordConsumer::addInteger);
        } else if (checkedPrecision <= 18) {
            return new RecordField<>(sqlField.getName(), bd.longValue(), RecordConsumer::addLong);
        } else {
            byte[] bdBytes = bd.unscaledValue().toByteArray();
            Binary binaryArray = Binary.fromReusedByteArray(bdBytes);
            return new RecordField<>(sqlField.getName(), binaryArray, RecordConsumer::addBinary);
        }
    }

    @Override
    public PrimitiveType convert(SQLColumnDefinition sqlColumnDefinition) throws ConvertException {
        int checkedPrecision = sqlColumnDefinition.getPrecision() <= 0 ? DEFAULT_PRECISION : sqlColumnDefinition.getPrecision();
        int checkedScale = Math.max(sqlColumnDefinition.getScale(), 0);

        PrimitiveType.PrimitiveTypeName primitiveTypeName;
        if (checkedPrecision <= 9) {
            primitiveTypeName = PrimitiveType.PrimitiveTypeName.INT32;
        } else if (checkedPrecision <= 18) {
            primitiveTypeName = PrimitiveType.PrimitiveTypeName.INT64;
        } else {
            primitiveTypeName = PrimitiveType.PrimitiveTypeName.BINARY;
        }
        return PrimitiveTypeCreator.create(sqlColumnDefinition.getName(),
                primitiveTypeName,
                LogicalTypeAnnotation.decimalType(checkedScale, checkedPrecision),
                sqlColumnDefinition.isNullable());
    }
}
