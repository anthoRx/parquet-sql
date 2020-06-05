package io.github.anthorx.parquet.sql.converter.types;

import io.github.anthorx.parquet.sql.converter.ConvertException;
import io.github.anthorx.parquet.sql.model.SQLColumnDefinition;
import io.github.anthorx.parquet.sql.model.SQLField;
import io.github.anthorx.parquet.sql.record.RecordField;
import junit.framework.Assert;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.stream.IntStream;

import static junit.framework.Assert.fail;

public class BigDecimalConverterTest {

    @Test
    public void convertDecimalPrecisionUnder0() {
        BigDecimalConverter converter = new BigDecimalConverter();

        IntStream.range(-1, 1).forEach(i -> {
            try {
                String name = "aname";
                SQLField field = new SQLField(name, new BigDecimal("1.0"), 1, i, 0, "java.math.BigDecimal");
                RecordField<?> record = converter.convert(field);

                Assert.assertTrue(record.getValue() instanceof Long);
                Assert.assertEquals(name, record.getName());
            } catch (ConvertException e) {
                fail("Exception thrown. " + e.getMessage());
            }
        });
    }

    @Test
    public void convertDecimal9Precision() {
        BigDecimalConverter converter = new BigDecimalConverter();

        IntStream.range(1, 10).forEach(i -> {
            try {
                String name = "aname";
                SQLField field = new SQLField(name, new BigDecimal("1.0"), 1, i, 0, "java.math.BigDecimal");
                RecordField<?> record = converter.convert(field);

                Assert.assertTrue(record.getValue() instanceof Integer);
                Assert.assertEquals(name, record.getName());
            } catch (ConvertException e) {
                fail("Exception thrown. " + e.getMessage());
            }
        });
    }

    @Test
    public void convertDecimal18Precision() {
        BigDecimalConverter converter = new BigDecimalConverter();

        IntStream.range(10, 19).forEach(i -> {
            try {
                String name = "aname";
                SQLField field = new SQLField(name, new BigDecimal("1.0"), 1, i, 0, "java.math.BigDecimal");
                RecordField<?> record = converter.convert(field);

                Assert.assertTrue(record.getValue() instanceof Long);
                Assert.assertEquals(name, record.getName());
            } catch (ConvertException e) {
                fail("Exception thrown. " + e.getMessage());
            }
        });
    }

    @Test
    public void convertDecimal19Precision() {
        BigDecimalConverter converter = new BigDecimalConverter();

        IntStream.range(19, 39).forEach(i -> {
            try {
                String name = "aname";
                SQLField field = new SQLField(name, new BigDecimal("1.0"), 1, i, 0, "java.math.BigDecimal");
                RecordField<?> record = converter.convert(field);

                Assert.assertTrue(record.getValue() instanceof Binary);
                Assert.assertEquals(name, record.getName());
            } catch (ConvertException e) {
                fail("Exception thrown. " + e.getMessage());
            }
        });
    }

    @Test
    public void convertColumnDecimalPrecisionUnder0() {
        BigDecimalConverter converter = new BigDecimalConverter();

        IntStream.range(-1, 1).forEach(precision -> {
            try {
                String name = "aname";
                int scale = 0;
                SQLColumnDefinition column =
                        new SQLColumnDefinition(name, 1, true, precision, scale, "java.math.BigDecimal");
                PrimitiveType primitiveType = converter.convert(column);

                Assert.assertEquals(PrimitiveType.PrimitiveTypeName.INT64, primitiveType.getPrimitiveTypeName());
                Assert.assertEquals(LogicalTypeAnnotation.decimalType(scale, BigDecimalConverter.DEFAULT_PRECISION), primitiveType.getLogicalTypeAnnotation());
                Assert.assertEquals(name, primitiveType.getName());
            } catch (ConvertException e) {
                fail("Exception thrown. " + e.getMessage());
            }
        });
    }

    @Test
    public void convertColumnDecimalPrecisionUnder10() {
        BigDecimalConverter converter = new BigDecimalConverter();

        IntStream.range(1, 10).forEach(precision -> {
            try {
                String name = "aname";
                int scale = 0;
                SQLColumnDefinition column =
                        new SQLColumnDefinition(name, 1, true, precision, scale, "java.math.BigDecimal");
                PrimitiveType primitiveType = converter.convert(column);

                Assert.assertEquals(PrimitiveType.PrimitiveTypeName.INT32, primitiveType.getPrimitiveTypeName());
                Assert.assertEquals(LogicalTypeAnnotation.decimalType(scale, precision), primitiveType.getLogicalTypeAnnotation());
                Assert.assertEquals(name, primitiveType.getName());
            } catch (ConvertException e) {
                fail("Exception thrown. " + e.getMessage());
            }
        });
    }

    @Test
    public void convertColumnDecimalPrecisionUnder19() {
        BigDecimalConverter converter = new BigDecimalConverter();

        IntStream.range(10, 19).forEach(precision -> {
            try {
                String name = "aname";
                int scale = 2;
                SQLColumnDefinition column =
                        new SQLColumnDefinition(name, 1, true, precision, scale, "java.math.BigDecimal");
                PrimitiveType primitiveType = converter.convert(column);

                Assert.assertEquals(PrimitiveType.PrimitiveTypeName.INT64, primitiveType.getPrimitiveTypeName());
                Assert.assertEquals(LogicalTypeAnnotation.decimalType(scale, precision), primitiveType.getLogicalTypeAnnotation());
                Assert.assertEquals(name, primitiveType.getName());
            } catch (ConvertException e) {
                fail("Exception thrown. " + e.getMessage());
            }
        });
    }


    @Test
    public void convertColumnDecimalPrecisionUpTo19() {
        BigDecimalConverter converter = new BigDecimalConverter();

        IntStream.range(19, 39).forEach(precision -> {
            try {
                String name = "aname";
                int scale = 2;
                SQLColumnDefinition column =
                        new SQLColumnDefinition(name, 1, true, precision, scale, "java.math.BigDecimal");
                PrimitiveType primitiveType = converter.convert(column);

                Assert.assertEquals(PrimitiveType.PrimitiveTypeName.BINARY, primitiveType.getPrimitiveTypeName());
                Assert.assertEquals(LogicalTypeAnnotation.decimalType(scale, precision), primitiveType.getLogicalTypeAnnotation());
                Assert.assertEquals(name, primitiveType.getName());
            } catch (ConvertException e) {
                fail("Exception thrown. " + e.getMessage());
            }
        });
    }
}
