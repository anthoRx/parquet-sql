package io.github.anthorx.parquet.sql.parquet.write.converter;

import io.github.anthorx.parquet.sql.jdbc.model.SQLColumnDefinition;
import io.github.anthorx.parquet.sql.jdbc.model.SQLField;
import io.github.anthorx.parquet.sql.parquet.model.ParquetRecordField;
import io.github.anthorx.parquet.sql.parquet.write.converter.types.ParquetSQLConverter;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConverterContainerTest {

  ConverterContainer converterContainer = new ConverterContainer();

  public static class CustomBigDecimalConverter implements ParquetSQLConverter {

    @Override
    public boolean accept(Class<?> c) {
      return c.isAssignableFrom(BigDecimal.class);
    }

    @Override
    public ParquetRecordField<?> convert(SQLField sqlField) throws ConvertException {
      return null;
    }

    @Override
    public PrimitiveType convert(SQLColumnDefinition sqlColumnDefinition) throws ConvertException {
      return null;
    }
  }

  @Test
  public void testGetConverterReturnsTheUserDefinedInsteadOfDefault() {
    CustomBigDecimalConverter expectedConverter = new CustomBigDecimalConverter();
    converterContainer.registerConverter(expectedConverter);

    Optional<ParquetSQLConverter> actualConverter = converterContainer.getConverter(BigDecimal.class);

    assertTrue(actualConverter.isPresent());
    assertSame(actualConverter.get(), expectedConverter);
  }
}