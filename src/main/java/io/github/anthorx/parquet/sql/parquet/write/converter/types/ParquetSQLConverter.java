package io.github.anthorx.parquet.sql.parquet.write.converter.types;

import io.github.anthorx.parquet.sql.jdbc.model.SQLColumnDefinition;
import io.github.anthorx.parquet.sql.jdbc.model.SQLField;
import io.github.anthorx.parquet.sql.parquet.model.ParquetRecordField;
import io.github.anthorx.parquet.sql.parquet.write.converter.ConvertException;
import org.apache.parquet.schema.PrimitiveType;

public interface ParquetSQLConverter {

  boolean accept(Class<?> c);

  ParquetRecordField<?> convert(SQLField sqlField) throws ConvertException;

  PrimitiveType convert(SQLColumnDefinition sqlColumnDefinition) throws ConvertException;
}
