package io.github.anthorx.parquet.sql.converter;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.util.Optional;

public class PrimitiveTypeCreator {

  public static PrimitiveType create(String name,
                                     PrimitiveType.PrimitiveTypeName primitiveTypeName,
                                     Boolean nullable) {
    return PrimitiveTypeCreator.create(name,
        primitiveTypeName,
        Optional.empty(),
        Optional.empty(), nullable
    );
  }

  public static PrimitiveType create(String name,
                                     PrimitiveType.PrimitiveTypeName primitiveTypeName,
                                     LogicalTypeAnnotation logicalTypeAnnotation,
                                     Boolean nullable) {
    return PrimitiveTypeCreator.create(name,
        primitiveTypeName,
        Optional.ofNullable(logicalTypeAnnotation),
        Optional.empty(), nullable
    );
  }

  public static PrimitiveType create(String name,
                                     PrimitiveType.PrimitiveTypeName primitiveTypeName,
                                     LogicalTypeAnnotation logicalTypeAnnotation,
                                     int typeLength, Boolean nullable) {
    return PrimitiveTypeCreator.create(name,
        primitiveTypeName,
        Optional.ofNullable(logicalTypeAnnotation),
        Optional.of(typeLength), nullable
    );
  }

  private static PrimitiveType create(String name,
                                      PrimitiveType.PrimitiveTypeName primitiveTypeName,
                                      Optional<LogicalTypeAnnotation> logicalTypeAnnotation,
                                      Optional<Integer> typeLength, Boolean nullable) {
    Types.PrimitiveBuilder<PrimitiveType> primitiveBuilder;

    if (nullable) {
      primitiveBuilder = org.apache.parquet.schema.Types.optional(primitiveTypeName);
    } else {
      primitiveBuilder = org.apache.parquet.schema.Types.required(primitiveTypeName);
    }

    logicalTypeAnnotation.map(primitiveBuilder::as);
    typeLength.map(primitiveBuilder::length);
    return primitiveBuilder.named(name);
  }
}
