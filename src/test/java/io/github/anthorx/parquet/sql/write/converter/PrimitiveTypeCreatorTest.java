package io.github.anthorx.parquet.sql.write.converter;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;

import static junit.framework.Assert.assertEquals;

public class PrimitiveTypeCreatorTest {

  public static final String ExpectedName = "BINARY";
  public static final PrimitiveType.PrimitiveTypeName ExpectedPrimitiveName = PrimitiveType.PrimitiveTypeName.BINARY;
  public static final int DefaultScale = 18;
  public static final int DefaultPrecision = 38;
  public static final LogicalTypeAnnotation.DecimalLogicalTypeAnnotation ExpectedLogicalTypeAnnotation = LogicalTypeAnnotation.decimalType(DefaultScale, DefaultPrecision);

  @Test
  public void testNameAndTypeName() {
    PrimitiveType primitiveType = PrimitiveTypeCreator.create(ExpectedName, ExpectedPrimitiveName, true);
    assertEquals(primitiveType.getName(), ExpectedName);
    assertEquals(primitiveType.getPrimitiveTypeName(), ExpectedPrimitiveName);
  }

  @Test
  public void testNullable() {
    PrimitiveType primitiveType = PrimitiveTypeCreator.create(ExpectedName, ExpectedPrimitiveName, true);
    assertEquals(primitiveType.getRepetition(), Type.Repetition.OPTIONAL);
  }

  @Test
  public void testNotNullable() {
    PrimitiveType primitiveType = PrimitiveTypeCreator.create(ExpectedName, ExpectedPrimitiveName, false);
    assertEquals(primitiveType.getRepetition(), Type.Repetition.REQUIRED);
  }

  @Test
  public void testLogicalTypeAnnotation() {
    PrimitiveType primitiveType = PrimitiveTypeCreator.create(ExpectedName, ExpectedPrimitiveName,
        ExpectedLogicalTypeAnnotation, false);
    assertEquals(primitiveType.getLogicalTypeAnnotation(), ExpectedLogicalTypeAnnotation);
  }

  @Test
  public void testTypeLength() {
    int ExpectedTypeLength = 16;
    PrimitiveType primitiveType = PrimitiveTypeCreator.create(ExpectedName, ExpectedPrimitiveName,
        ExpectedLogicalTypeAnnotation, ExpectedTypeLength, false);
    assertEquals(primitiveType.getTypeLength(), ExpectedTypeLength);
  }
}