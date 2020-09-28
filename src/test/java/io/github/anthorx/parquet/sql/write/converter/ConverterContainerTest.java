package io.github.anthorx.parquet.sql.write.converter;

import io.github.anthorx.parquet.sql.model.SQLColumnDefinition;
import io.github.anthorx.parquet.sql.model.SQLField;
import io.github.anthorx.parquet.sql.record.RecordField;
import io.github.anthorx.parquet.sql.write.converter.types.ParquetSQLConverter;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Optional;

import static junit.framework.Assert.assertSame;
import static junit.framework.Assert.assertTrue;

public class ConverterContainerTest {

  ConverterContainer converterContainer = new ConverterContainer();

  public static class CustomBigDecimalConverter implements ParquetSQLConverter {

    @Override
    public boolean accept(Class<?> c) {
      return c.isAssignableFrom(BigDecimal.class);
    }

    @Override
    public RecordField<?> convert(SQLField sqlField) throws ConvertException {
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