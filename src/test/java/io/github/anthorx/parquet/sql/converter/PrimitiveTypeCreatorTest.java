package io.github.anthorx.parquet.sql.converter;

import junit.framework.TestCase;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class PrimitiveTypeCreatorTest extends TestCase {

  public static final String ExpectedName = "BINARY";
  public static final PrimitiveType.PrimitiveTypeName ExpectedPrimitiveName = PrimitiveType.PrimitiveTypeName.BINARY;
  public static final int DefaultScale = 18;
  public static final int DefaultPrecision = 38;
  public static final LogicalTypeAnnotation.DecimalLogicalTypeAnnotation ExpectedLogicalTypeAnnotation = LogicalTypeAnnotation.decimalType(DefaultScale, DefaultPrecision);

  public void testNameAndTypeName() {
    PrimitiveType primitiveType = PrimitiveTypeCreator.create(ExpectedName, ExpectedPrimitiveName, true);
    assertEquals(primitiveType.getName(), ExpectedName);
    assertEquals(primitiveType.getPrimitiveTypeName(), ExpectedPrimitiveName);
  }

  public void testNullable() {
    PrimitiveType primitiveType = PrimitiveTypeCreator.create(ExpectedName, ExpectedPrimitiveName, true);
    assertEquals(primitiveType.getRepetition(), Type.Repetition.OPTIONAL);
  }

  public void testNotNullable() {
    PrimitiveType primitiveType = PrimitiveTypeCreator.create(ExpectedName, ExpectedPrimitiveName, false);
    assertEquals(primitiveType.getRepetition(), Type.Repetition.REQUIRED);
  }

  public void testLogicalTypeAnnotation() {
    PrimitiveType primitiveType = PrimitiveTypeCreator.create(ExpectedName, ExpectedPrimitiveName,
        ExpectedLogicalTypeAnnotation,false);
    assertEquals(primitiveType.getLogicalTypeAnnotation(), ExpectedLogicalTypeAnnotation);
  }

  public void testTypeLength() {
    int ExpectedTypeLength = 16;
    PrimitiveType primitiveType = PrimitiveTypeCreator.create(ExpectedName, ExpectedPrimitiveName,
        ExpectedLogicalTypeAnnotation, ExpectedTypeLength, false);
    assertEquals(primitiveType.getTypeLength(), ExpectedTypeLength);
  }
}