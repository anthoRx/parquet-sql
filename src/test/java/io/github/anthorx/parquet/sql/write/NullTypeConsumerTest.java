package io.github.anthorx.parquet.sql.write;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.parquet.schema.LogicalTypeAnnotation.*;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NullTypeConsumerTest {

  private static Type primitiveType(PrimitiveTypeName primitiveTypeName) {
    return new PrimitiveType(Type.Repetition.OPTIONAL, primitiveTypeName, "defaultName");
  }

  private static Type logicalType(PrimitiveTypeName primitiveTypeName, LogicalTypeAnnotation logicalTypeAnnotation) {
    return Types.optional(primitiveTypeName).as(logicalTypeAnnotation).named("defaultName");
  }

  public static Map<Type, Integer> primitiveInputExpected;
  static {
    primitiveInputExpected = new HashMap<>();
    primitiveInputExpected.put(primitiveType(DOUBLE), java.sql.Types.DECIMAL);
    primitiveInputExpected.put(primitiveType(FLOAT), java.sql.Types.FLOAT);
    primitiveInputExpected.put(primitiveType(BOOLEAN), java.sql.Types.BOOLEAN);
    primitiveInputExpected.put(primitiveType(INT32), java.sql.Types.INTEGER);
    primitiveInputExpected.put(primitiveType(INT64), java.sql.Types.NUMERIC);
    primitiveInputExpected.put(primitiveType(INT96), java.sql.Types.TIMESTAMP);
  }

  public static Map<Type, Integer> logicalInputExpected;
  static {
    logicalInputExpected = new HashMap<>();
    logicalInputExpected.put(logicalType(BINARY, stringType()), java.sql.Types.VARCHAR);
    logicalInputExpected.put(logicalType(INT64, timestampType(false, TimeUnit.NANOS)), java.sql.Types.TIMESTAMP);
    logicalInputExpected.put(logicalType(INT32, intType(32, false)), java.sql.Types.NUMERIC);
    logicalInputExpected.put(logicalType(BINARY, decimalType(18, 32)), java.sql.Types.DECIMAL);
  }

  @Test
  public void testPrimitiveGet() {
    primitiveInputExpected.forEach((input, expected) -> {
      Optional<Integer> maybeActual = NullTypeConsumer.getNullTypeConsumerFromPrimitiveType(input);
      assertTrue(maybeActual.isPresent());
      Integer actual = maybeActual.get();
      assertEquals(expected, actual);
    });
  }

  @Test
  public void testLogicalGet() {
    logicalInputExpected.forEach((input, expected) -> {
      Optional<Integer> maybeActual = NullTypeConsumer.getNullTypeConsumerFromLogicalType(input);
      assertTrue(maybeActual.isPresent());
      Integer actual = maybeActual.get();
      assertEquals(expected, actual);
    });
  }
}