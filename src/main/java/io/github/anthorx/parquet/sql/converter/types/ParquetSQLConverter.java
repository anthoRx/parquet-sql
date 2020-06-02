package io.github.anthorx.parquet.sql.converter.types;

import io.github.anthorx.parquet.sql.converter.ConvertException;
import io.github.anthorx.parquet.sql.record.RecordField;
import io.github.anthorx.parquet.sql.model.SQLColumnDefinition;
import io.github.anthorx.parquet.sql.model.SQLField;
import org.apache.parquet.schema.PrimitiveType;

public interface ParquetSQLConverter {

    boolean accept(Class<?> c);

    RecordField<?> convert(SQLField sqlField) throws ConvertException;

    PrimitiveType convert(SQLColumnDefinition sqlColumnDefinition) throws ConvertException;
}
