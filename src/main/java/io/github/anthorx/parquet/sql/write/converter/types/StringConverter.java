package io.github.anthorx.parquet.sql.write.converter.types;

import io.github.anthorx.parquet.sql.model.SQLColumnDefinition;
import io.github.anthorx.parquet.sql.model.SQLField;
import io.github.anthorx.parquet.sql.record.RecordField;
import io.github.anthorx.parquet.sql.write.converter.ConvertException;
import io.github.anthorx.parquet.sql.write.converter.PrimitiveTypeCreator;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

public class StringConverter implements ParquetSQLConverter {

    @Override
    public boolean accept(Class<?> c) {
        return c.isAssignableFrom(String.class);
    }

    @Override
    public RecordField<?> convert(SQLField sqlField) throws ConvertException {
        String value;
        if (sqlField.getValue() instanceof char[]) {
            value = new String((char[]) sqlField.getValue());
        } else {
            value = (String) sqlField.getValue();
        }
        Binary binaryString = Binary.fromString(value);
        return new RecordField<>(sqlField.getName(), binaryString)
            .addWriteConsumer(RecordConsumer::addBinary);
    }

    @Override
    public PrimitiveType convert(SQLColumnDefinition sqlColumnDefinition) throws ConvertException {
        return PrimitiveTypeCreator.create(sqlColumnDefinition.getName(),
                PrimitiveType.PrimitiveTypeName.BINARY,
                LogicalTypeAnnotation.stringType(),
                sqlColumnDefinition.isNullable());
    }
}
